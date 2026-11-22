"""数据抽取领域服务：消费behavior_script脚本和element_selector锚点，执行字段抽取。

抽取字段映射统一走「版本化抽取字段缓存」（extract_field_cache）：

- 缓存键 = (脚本, 映射版本)：一次抽取只取其采用模板的完整字段集及版本；
  模板切换只失效该脚本的当前指针，历史版本条目与无关脚本缓存仍可命中。
- 并行读取合并为单次真实模板读取；填充失败/取消不暴露半个字段集，
  新的采集可重新填充取得完整映射。
- 每次填充把版本化缓存条目持久化到 data/snapshots/extract_mapping_*.json
  （可追溯：映射版本 + 来源模板 + 填充序号）；每次抽取把结构化行与所用
  映射的版本、来源写入 data/snapshots/extract_sample_*.json，运行结束后
  可按样本回查，重读映射与存档一致。
"""
from __future__ import annotations

import itertools
import threading
import uuid
from typing import Any, Callable

from app.store import store
from app.services.base import BaseModuleService
from app.services.extract_field_cache import (
    CancelToken,
    FieldSetEntry,
    FillCancelled,
    FillContext,
    FillFailed,
    VersionedFieldCache,
)
from app import persistence as persist

CURRENT_KEY = "current::{script}"
VERSIONED_KEY = "versioned::{script}::{version}"

# 模板状态变化动作：只失效关联脚本的当前指针
_INVALIDATING_ACTIONS = {"标记生效", "停用模板"}


class DataExtractService(BaseModuleService):
    MODULE = "data_extract"
    ENTITY = "抽取模板"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["模板编号", "关联脚本", "抽取字段", "提取方式", "数据处理", "输出类型", "默认值", "模板版本"]
    REQUIRED_FIELDS = ["模板编号", "关联脚本", "抽取字段"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用模板": "已停用"}
    NEGATIVE_ACTIONS = ["停用模板"]

    def __init__(self) -> None:
        self._field_cache = VersionedFieldCache()
        # 真实模板读取计量：(脚本, 映射版本) -> 次数，只在新版本填充时增长
        self._template_reads: dict[tuple[str, str], int] = {}
        self._reads_lock = threading.Lock()
        self._sample_seq = itertools.count(1)
        # 填充钩子：在真实模板读取之后、字段集发布之前调用。
        # 运维/测试可借此模拟缓慢模板源、暂停填充或注入失败；生产默认 None。
        self.fill_hook: Callable[[], None] | None = None

    # ---------------- 模板读取（唯一真实读取点，全部计数） ----------------

    def _adopted_template(self, script_id: str) -> dict[str, Any] | None:
        """一次抽取只采用一份模板：同一脚本取最新生效的一份。"""
        linked = [
            t for t in store.rows(self.MODULE)
            if str(t.get("关联脚本")) == script_id and t.get("status") == "已生效"
        ]
        if not linked:
            return None
        return max(linked, key=lambda t: int(t.get("id", 0)))

    def _fields_from_template(self, tpl: dict[str, Any]) -> tuple[dict[str, Any], ...]:
        names = [n.strip() for n in str(tpl.get("抽取字段") or "").split(",") if n.strip()]
        return tuple(
            {
                "field": name,
                "method": tpl.get("提取方式"),
                "output_type": tpl.get("输出类型"),
                "default": tpl.get("默认值"),
            }
            for name in names
        )

    def _note_template_read(self, script_id: str, version: str) -> None:
        with self._reads_lock:
            key = (script_id, version)
            self._template_reads[key] = self._template_reads.get(key, 0) + 1

    def template_reads(
        self, script_id: str | None = None, version: str | None = None
    ) -> int | dict[str, int]:
        """实际模板读取次数：可按脚本+版本精确查询，或取全量分明细。"""
        with self._reads_lock:
            snapshot = dict(self._template_reads)
        if script_id is not None and version is not None:
            return snapshot.get((script_id, version), 0)
        if script_id is not None:
            return sum(v for (s, _), v in snapshot.items() if s == script_id)
        return {f"{script}::{ver}": count for (script, ver), count in sorted(snapshot.items())}

    # ---------------- 版本化缓存条目 ----------------

    def _build_current_entry(self, script_id: str) -> Callable[[FillContext], FieldSetEntry]:
        """构建当前采用模板的完整字段集：真实模板读取只发生在这里。"""

        def build(ctx: FillContext) -> FieldSetEntry:
            tpl = self._adopted_template(script_id)  # ← 真实模板读取
            if tpl is None:
                raise FillFailed(f"脚本 {script_id} 没有已生效的抽取模板")
            version = str(tpl.get("version") or "")
            self._note_template_read(script_id, version)
            if self.fill_hook is not None:
                self.fill_hook()  # 填充可在此被暂停或注入失败
            if ctx.token is not None and ctx.token.cancelled:
                raise FillCancelled(f"脚本 {script_id} 模板版本 {version} 填充已暂停")
            entry = FieldSetEntry(
                cache_key=VERSIONED_KEY.format(script=script_id, version=version),
                script_id=script_id,
                mapping_version=version,
                fields=self._fields_from_template(tpl),
                source_templates=(
                    {
                        "id": tpl.get("id"),
                        "模板编号": tpl.get("模板编号"),
                        "模板版本": tpl.get("模板版本"),
                        "version": version,
                    },
                ),
                read_seq=ctx.read_seq,
                filled_at=persist.now_iso(),
                fill_cost_ms=0.0,
            )
            self._persist_entry(entry)
            return entry

        return build

    def _persist_entry(self, entry: FieldSetEntry) -> None:
        """版本化缓存条目落盘：可追溯，供运行样本按版本回查。"""
        persist.write_json(
            "snapshots",
            f"extract_mapping_{entry.script_id}_{entry.mapping_version}.json",
            entry.as_dict(),
        )
        persist.append_jsonl("runs", "extract_mapping_cache.jsonl", {
            "action": "field_cache_filled",
            "script_id": entry.script_id,
            "mapping_version": entry.mapping_version,
            "source_templates": [dict(s) for s in entry.source_templates],
            "field_count": len(entry.fields),
            "read_seq": entry.read_seq,
            "timestamp": entry.filled_at,
        })

    def current_mapping(
        self, script_id: str, *, token: CancelToken | None = None
    ) -> FieldSetEntry:
        """脚本当前采用模板的完整字段集：并行读取合并为单次模板读取。"""
        return self._field_cache.get_or_fill(
            CURRENT_KEY.format(script=script_id),
            self._build_current_entry(script_id),
            token=token,
            alias_fn=lambda entry: (
                VERSIONED_KEY.format(script=entry.script_id, version=entry.mapping_version),
            ),
        )

    def mapping_for_version(self, script_id: str, version: str) -> FieldSetEntry:
        """按版本回查映射：优先内存缓存，缺失时读持久化存档，保证与存档一致。"""
        entry = self._field_cache.get(VERSIONED_KEY.format(script=script_id, version=version))
        if entry is not None:
            return entry
        archived = persist.read_json("snapshots", f"extract_mapping_{script_id}_{version}.json")
        if archived is None:
            raise FillFailed(f"脚本 {script_id} 没有版本 {version} 的存档映射")
        return FieldSetEntry.from_dict(archived)

    def extract_fields_for_script(self, script_id: str) -> list[dict[str, Any]]:
        """根据脚本编号返回采用模板的字段映射（走版本化缓存，不再每次重读模板）。"""
        entry = self.current_mapping(script_id)
        return [dict(field) for field in entry.fields]

    # ---------------- 抽取运行与样本回查 ----------------

    def run_extraction(
        self,
        script_id: str,
        records: list[dict[str, Any]],
        *,
        mapping_version: str | None = None,
        token: CancelToken | None = None,
    ) -> dict[str, Any]:
        """执行一次抽取：取采用模板的完整字段集及版本，生成结构化行并持久化样本。

        样本记录所用映射的版本与来源，运行结束后可按 sample_id 回查。
        """
        if mapping_version is None:
            entry = self.current_mapping(script_id, token=token)
        else:
            entry = self.mapping_for_version(script_id, mapping_version)
        rows = [self._apply_fields(entry.fields, record) for record in records]
        sample_id = f"EXS-{next(self._sample_seq):04d}-{uuid.uuid4().hex[:8]}"
        sample = {
            "sample_id": sample_id,
            "script_id": script_id,
            "mapping_version": entry.mapping_version,
            "source_templates": [dict(s) for s in entry.source_templates],
            "fields": [dict(f) for f in entry.fields],
            "rows": rows,
            "row_count": len(rows),
            "cache_read_seq": entry.read_seq,
            "created_at": persist.now_iso(),
        }
        persist.write_json("snapshots", f"extract_sample_{sample_id}.json", sample)
        persist.append_jsonl("runs", f"{self.MODULE}.jsonl", {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "action": "run_extraction",
            "sample_id": sample_id,
            "script_id": script_id,
            "mapping_version": entry.mapping_version,
            "row_count": len(rows),
            "timestamp": sample["created_at"],
        })
        return sample

    def get_sample(self, sample_id: str) -> dict[str, Any] | None:
        """按样本编号回查运行样本（含所用映射版本、来源与结构化行）。"""
        return persist.read_json("snapshots", f"extract_sample_{sample_id}.json")

    def _apply_fields(
        self, fields: tuple[dict[str, Any], ...], record: dict[str, Any]
    ) -> dict[str, Any]:
        return {f["field"]: self._cast(record.get(f["field"], f.get("default")), f) for f in fields}

    @staticmethod
    def _cast(value: Any, field: dict[str, Any]) -> Any:
        output = str(field.get("output_type") or "文本")
        if value is None:
            value = field.get("default")
        try:
            if output == "数值":
                return int(value)
            if output == "浮点":
                return float(value)
            if output == "布尔":
                return str(value).strip().lower() in ("1", "true", "是", "yes")
        except (TypeError, ValueError):
            return field.get("default")
        return "" if value is None else str(value)

    # ---------------- 模板切换：只失效关联脚本的缓存范围 ----------------

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is not None and action in _INVALIDATING_ACTIONS:
            script_id = str(entry.get("关联脚本") or "").strip()
            if script_id:
                # 只失效该脚本的当前指针：历史版本条目保留供回查，
                # 无关脚本的缓存条目不受影响，仍可命中。
                self._field_cache.invalidate(CURRENT_KEY.format(script=script_id))
        return entry, message

    # ---------------- 统计 ----------------

    def cache_stats(self) -> dict[str, Any]:
        stats = self._field_cache.stats()
        stats["template_reads"] = self.template_reads()
        return stats

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        templates = store.rows(self.MODULE)
        # 关联的元素锚点统计
        selectors = store.rows("element_selector")
        base["templates_with_selectors"] = sum(
            1 for t in templates if any(
                s.get("关联脚本") == t.get("关联脚本") for s in selectors))
        # 关联的清洗管道
        pipelines = store.rows("clean_pipeline")
        base["templates_in_pipeline"] = sum(
            1 for t in templates if any(
                p.get("输入源") and t.get("关联脚本") in str(p.get("输入源", "")) for p in pipelines))
        # 版本化字段缓存运行指标
        cache = self._field_cache.stats()
        base["field_cache_hit"] = cache["hit"]
        base["field_cache_filled"] = cache["filled"]
        base["field_cache_coalesced"] = cache["coalesced"]
        with self._reads_lock:
            base["template_reads"] = sum(self._template_reads.values())
        return base
