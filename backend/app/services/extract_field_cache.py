"""版本化抽取字段缓存：按「脚本 + 映射版本」缓存完整字段集。

旧实现的痛点：缓存只识别采集键（脚本），每次抽取都从生效模板重读，
模板 V1→V2 切换期间并行读取会重复查询、重复重建。本模块的约定：

- 版本化缓存条目：键 = (脚本, 映射版本)。条目记录映射版本、来源模板、
  填充序号与耗时，可追溯；旧版本条目保留，供运行样本按版本回查。
- 单飞填充：同一键的并行读取合并为一次真实模板读取，其余读取者等待
  共享结果（coalesced），不再各自重查模板。
- 取消安全：等待者中途取消只影响自身，共享填充继续；填充者被取消或
  失败时丢弃整个进行中填充，绝不发布半个字段集，后续采集可重新填充。
- 作用域失效：invalidate 只影响指定键（如某脚本的当前指针），模板
  切换不影响无关脚本的缓存条目，无关脚本仍可命中。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable


class FillCancelled(Exception):
    """填充被取消：读取者中途退出，或填充本身被暂停。"""


class FillFailed(Exception):
    """填充失败：本次构建未产出完整字段集，缓存不保留半成品。"""


class CancelToken:
    """协作式取消令牌：某个读取者中途取消，不影响其他并行读取者。"""

    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()


@dataclass(frozen=True)
class FieldSetEntry:
    """版本化抽取字段缓存条目（不可变，整体构建完成后一次性发布）。"""

    cache_key: str
    script_id: str
    mapping_version: str
    fields: tuple[dict[str, Any], ...]
    source_templates: tuple[dict[str, Any], ...]
    read_seq: int
    filled_at: str
    fill_cost_ms: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "cache_key": self.cache_key,
            "script_id": self.script_id,
            "mapping_version": self.mapping_version,
            "fields": [dict(item) for item in self.fields],
            "source_templates": [dict(item) for item in self.source_templates],
            "read_seq": self.read_seq,
            "filled_at": self.filled_at,
            "fill_cost_ms": self.fill_cost_ms,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FieldSetEntry":
        return cls(
            cache_key=str(data["cache_key"]),
            script_id=str(data["script_id"]),
            mapping_version=str(data["mapping_version"]),
            fields=tuple(dict(item) for item in data["fields"]),
            source_templates=tuple(dict(item) for item in data["source_templates"]),
            read_seq=int(data["read_seq"]),
            filled_at=str(data["filled_at"]),
            fill_cost_ms=float(data["fill_cost_ms"]),
        )


@dataclass
class FillContext:
    """传给填充函数的上下文：本次真实模板读取的序号与取消令牌。"""

    read_seq: int
    token: CancelToken | None = None


@dataclass
class _Inflight:
    """进行中的填充：并行读取者等待同一个填充结果。"""

    event: threading.Event = field(default_factory=threading.Event)
    entry: FieldSetEntry | None = None
    error: BaseException | None = None


class VersionedFieldCache:
    """线程安全的版本化字段集缓存：单飞填充 + 取消安全 + 作用域失效。"""

    WAIT_SLICE_SEC = 0.05

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, FieldSetEntry] = {}
        self._inflight: dict[str, _Inflight] = {}
        self._invalidated: dict[str, int] = {}
        self._read_seq = 0
        self._hit = 0
        self._filled = 0
        self._coalesced = 0

    # ---------------- 查询 ----------------

    def get(self, key: str) -> FieldSetEntry | None:
        """只读命中检查：不触发填充，返回完整条目或 None。"""
        with self._lock:
            return self._entries.get(key)

    def keys(self) -> list[str]:
        with self._lock:
            return sorted(self._entries)

    # ---------------- 单飞填充 ----------------

    def get_or_fill(
        self,
        key: str,
        build_fn: Callable[[FillContext], FieldSetEntry],
        *,
        token: CancelToken | None = None,
        alias_fn: Callable[[FieldSetEntry], Iterable[str]] | None = None,
    ) -> FieldSetEntry:
        """命中返回完整条目；未命中合并并行读取，由 leader 执行一次真实填充。

        build_fn：leader 在锁外执行，内部完成真实模板读取并构建完整条目。
        alias_fn：填充成功后为条目追加发布的键（如版本化键），条目不可变，
        别名与主键指向同一完整字段集。
        """
        if token is not None and token.cancelled:
            raise FillCancelled(f"读取 {key} 开始前已取消")

        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                self._hit += 1
                return entry
            inflight = self._inflight.get(key)
            if inflight is None:
                inflight = _Inflight()
                self._inflight[key] = inflight
                self._read_seq += 1
                read_seq = self._read_seq
                invalid_gen = self._invalidated.get(key, 0)
                leader = True
            else:
                self._coalesced += 1
                leader = False
                read_seq = 0
                invalid_gen = 0

        if leader:
            return self._fill_as_leader(
                key, inflight, build_fn, token, alias_fn, read_seq, invalid_gen)
        return self._wait_as_follower(key, inflight, token)

    def _fill_as_leader(
        self,
        key: str,
        inflight: _Inflight,
        build_fn: Callable[[FillContext], FieldSetEntry],
        token: CancelToken | None,
        alias_fn: Callable[[FieldSetEntry], Iterable[str]] | None,
        read_seq: int,
        invalid_gen: int,
    ) -> FieldSetEntry:
        started = time.perf_counter()
        try:
            entry = build_fn(FillContext(read_seq=read_seq, token=token))
        except FillCancelled as exc:
            # 填充被暂停/取消：丢弃整个进行中填充，不发布任何半成品。
            self._finish_inflight(key, inflight, error=exc)
            raise
        except Exception as exc:  # noqa: BLE001 - 统一包装为领域错误
            wrapped = FillFailed(f"填充 {key} 失败：{exc}")
            self._finish_inflight(key, inflight, error=wrapped)
            raise wrapped from exc

        cost_ms = round((time.perf_counter() - started) * 1000, 3)
        object.__setattr__(entry, "fill_cost_ms", cost_ms)
        with self._lock:
            # 原子发布：完整条目一次性对外可见；若填充期间该键被失效
            # （如模板切换），主键不再发布，避免覆盖新指针，但版本化别名
            # 仍发布——历史版本条目是真实的、可追溯的存档。
            if self._invalidated.get(key, 0) == invalid_gen:
                self._entries[key] = entry
            for alias in (alias_fn(entry) if alias_fn is not None else ()):
                self._entries[alias] = entry
            self._filled += 1
            self._inflight.pop(key, None)
            inflight.entry = entry
            inflight.event.set()
        return entry

    def _wait_as_follower(
        self, key: str, inflight: _Inflight, token: CancelToken | None
    ) -> FieldSetEntry:
        while True:
            if inflight.event.wait(self.WAIT_SLICE_SEC):
                break
            if token is not None and token.cancelled:
                # 等待者中途取消：只退出自身，共享填充继续服务其他读取者。
                raise FillCancelled(f"读取者已取消等待 {key}，共享填充继续")
        if inflight.error is not None:
            raise FillFailed(f"共享填充 {key} 未完成：{inflight.error}")
        assert inflight.entry is not None
        return inflight.entry

    def _finish_inflight(self, key: str, inflight: _Inflight, error: BaseException) -> None:
        with self._lock:
            self._inflight.pop(key, None)
            inflight.error = error
            inflight.event.set()

    # ---------------- 作用域失效 ----------------

    def invalidate(self, key: str) -> None:
        """只失效指定键：无关键（其他脚本、历史版本条目）不受影响。

        同时递增失效代际：在此之前开始的进行中填充完成后不得再发布该键，
        防止模板切换期间旧填充覆盖新指针。
        """
        with self._lock:
            self._entries.pop(key, None)
            self._invalidated[key] = self._invalidated.get(key, 0) + 1

    # ---------------- 统计 ----------------

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "hit": self._hit,
                "filled": self._filled,
                "coalesced": self._coalesced,
                "inflight": len(self._inflight),
                "entries": len(self._entries),
                "read_seq": self._read_seq,
            }
