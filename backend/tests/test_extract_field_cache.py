"""版本化抽取字段缓存验收测试。

核对流程（对应任务验收路径）：
1. 旧批填充模板 V1 期间暂停填充 → 不暴露半个字段集；
2. V2 生效后同时提交两次抽取并取消其中等待者 → 共享填充只读一次模板，
   另一份拿到完整字段集；V2 再试一次 → 稳定命中；
3. 无关脚本缓存不受模板切换影响，仍可命中复用；
4. 填充失败 → 新采集重新取得完整映射；
5. 运行样本按编号回查，重读映射与存档一致；
6. 并发完成后再发一个 V2 请求 → 模板读取数不再增加。
"""
from __future__ import annotations

import os
import sys
import time
import threading

# 确保后端目录在导入路径中
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist

# 清理本测试的持久化产物（只删 extract_ 前缀，避免影响其他模块数据）
for _sub in ("snapshots", "runs"):
    _dir = persist.data_path(_sub, "")
    if os.path.isdir(_dir):
        for _f in os.listdir(_dir):
            if _f.startswith("extract_"):
                os.remove(os.path.join(_dir, _f))


def _make_template(svc, code, script, fields, version_tag, output_type="文本", default="-"):
    """登记模板并标记生效，返回 (id, 生效后的版本指纹)。"""
    entry, missing = svc.create_entry({
        "模板编号": code,
        "关联脚本": script,
        "抽取字段": fields,
        "提取方式": "css",
        "数据处理": "trim",
        "输出类型": output_type,
        "默认值": default,
        "模板版本": version_tag,
    })
    assert entry is not None, f"模板登记失败: {missing}"
    result, msg = svc.run_action(entry["id"], "标记生效")
    assert result is not None, f"标记生效失败: {msg}"
    return result["id"], result["version"]


def test_template_switch_parallel_reads_and_cancel():
    """模板切换期间：暂停 V1 填充、V2 并行读取合并、取消安全、无关脚本不受影响。"""
    from app.services.data_extract import DataExtractService
    from app.services.extract_field_cache import CancelToken, FillCancelled

    svc = DataExtractService()
    script_s = "BEHA-CACHE-S1"
    script_u = "BEHA-CACHE-U1"

    _, v1_version = _make_template(svc, "DATA-CACHE-V1", script_s, "title,price", "v1")
    _, u_version = _make_template(svc, "DATA-CACHE-U1", script_u, "sku", "u1")

    # 0) 无关脚本先建立缓存：两次抽取只读一次模板
    svc.run_extraction(script_u, [{"sku": "A-1"}])
    svc.run_extraction(script_u, [{"sku": "A-2"}])
    assert svc.template_reads(script_u, u_version) == 1, "无关脚本重复读取模板"

    # 1) 旧批正在填充 V1，暂停（取消）这次填充：不得暴露半个字段集
    gate = threading.Event()
    fill_started = threading.Event()

    def pausable_hook():
        fill_started.set()
        gate.wait(timeout=10)

    svc.fill_hook = pausable_hook
    token_v1 = CancelToken()
    outcome: dict[str, object] = {}

    def old_batch():
        try:
            svc.run_extraction(script_s, [{"title": "旧批", "price": "1"}], token=token_v1)
            outcome["v1"] = "completed"
        except FillCancelled:
            outcome["v1"] = "cancelled"

    t1 = threading.Thread(target=old_batch)
    t1.start()
    assert fill_started.wait(timeout=5), "V1 填充未开始"
    token_v1.cancel()  # 暂停 V1 填充
    gate.set()
    t1.join(timeout=5)
    assert outcome.get("v1") == "cancelled", f"V1 填充应被取消: {outcome}"
    assert svc._field_cache.get(f"current::{script_s}") is None, "取消后当前指针不应有半成品"
    assert svc._field_cache.get(f"versioned::{script_s}::{v1_version}") is None, "取消后不应有半个字段集"
    assert persist.read_json("snapshots", f"extract_mapping_{script_s}_{v1_version}.json") is None, \
        "取消的填充不应落盘"
    assert svc.template_reads(script_s, v1_version) == 1, "V1 读取发生一次但填充被废弃"
    svc.fill_hook = None

    # 2) V2 生效：只失效 S 的当前指针，无关脚本缓存不受影响
    _, v2_version = _make_template(svc, "DATA-CACHE-V2", script_s, "title,price,stock", "v2")
    assert svc._field_cache.get(f"current::{script_u}") is not None, "无关脚本缓存应保留"

    # 3) 同时提交两次 V2 抽取，取消其中等待者：共享填充只读一次模板
    gate.clear()
    fill_started.clear()
    svc.fill_hook = pausable_hook
    token_a, token_b = CancelToken(), CancelToken()
    outcome2: dict[str, object] = {}

    def extract_a():
        try:
            outcome2["a"] = svc.run_extraction(
                script_s, [{"title": "并发A", "price": "9.9", "stock": "3"}], token=token_a)
        except Exception as exc:  # noqa: BLE001
            outcome2["a_error"] = exc

    def extract_b():
        try:
            outcome2["b"] = svc.run_extraction(
                script_s, [{"title": "并发B", "price": "8.8", "stock": "5"}], token=token_b)
        except FillCancelled:
            outcome2["b"] = "cancelled"

    ta = threading.Thread(target=extract_a)
    ta.start()
    assert fill_started.wait(timeout=5), "V2 填充未开始"
    tb = threading.Thread(target=extract_b)
    tb.start()
    deadline = time.time() + 5
    while time.time() < deadline and svc._field_cache.stats()["coalesced"] < 1:
        time.sleep(0.01)
    assert svc._field_cache.stats()["coalesced"] >= 1, "第二次读取未合并进共享填充"
    token_b.cancel()  # 等待者中途取消
    tb.join(timeout=5)
    assert outcome2.get("b") == "cancelled", f"等待者应被取消: {outcome2.get('b')}"
    gate.set()
    ta.join(timeout=5)
    assert "a_error" not in outcome2, f"另一份抽取不应受取消影响: {outcome2.get('a_error')}"
    svc.fill_hook = None

    sample_a = outcome2["a"]
    assert sample_a["mapping_version"] == v2_version, "抽取应采用 V2 的映射版本"
    assert [f["field"] for f in sample_a["fields"]] == ["title", "price", "stock"], \
        "应拿到 V2 的完整字段集"
    assert sample_a["rows"] == [{"title": "并发A", "price": "9.9", "stock": "3"}], \
        "应实际生成结构化行"
    assert svc.template_reads(script_s, v2_version) == 1, "两次并行读取应只读一次模板"

    # 4) V2 再试一次：稳定命中，默认值兜底，模板读取数不增
    again = svc.run_extraction(script_s, [{"title": "再试", "price": "1.0"}])
    assert again["mapping_version"] == v2_version
    assert again["rows"] == [{"title": "再试", "price": "1.0", "stock": "-"}], "缺省字段应取默认值"
    assert svc.template_reads(script_s, v2_version) == 1, "命中后不应重读模板"

    # 旧接口同样走缓存，不再每次重读生效模板
    legacy = svc.extract_fields_for_script(script_s)
    assert [f["field"] for f in legacy] == ["title", "price", "stock"]
    assert svc.template_reads(script_s, v2_version) == 1

    # 5) 无关脚本仍可命中：S 的模板切换后读取数不增
    svc.run_extraction(script_u, [{"sku": "A-3"}])
    assert svc.template_reads(script_u, u_version) == 1, "无关脚本不应受模板切换影响"

    # 6) 样本回查：全新实例（空缓存）按版本读存档，映射与样本一致
    svc_fresh = DataExtractService()
    archived = svc_fresh.mapping_for_version(script_s, v2_version)
    reread = svc_fresh.get_sample(sample_a["sample_id"])
    assert reread is not None, "运行结束后应可按样本编号回查"
    assert reread["mapping_version"] == v2_version
    assert [dict(f) for f in archived.fields] == reread["fields"], "重读映射应与存档一致"
    assert [s["模板编号"] for s in archived.source_templates] == ["DATA-CACHE-V2"], \
        "存档应记录来源模板"

    # 可追溯：V1 填充被暂停未落盘，只有 V2 一条填充记录
    fills = [r for r in persist.read_jsonl("runs", "extract_mapping_cache.jsonl")
             if r.get("script_id") == script_s]
    assert [r["mapping_version"] for r in fills] == [v2_version], \
        f"填充台账应只有 V2 一条: {fills}"

    # 7) 并发完成后再发一个 V2 请求：稳定命中，模板读取数不再增加
    before = svc.template_reads(script_s, v2_version)
    final = svc.run_extraction(script_s, [{"title": "稳定", "price": "2.0", "stock": "1"}])
    assert final["mapping_version"] == v2_version
    assert final["cache_read_seq"] == sample_a["cache_read_seq"], "稳定命中应复用同一次填充的条目"
    assert svc.template_reads(script_s, v2_version) == before, "稳定命中后模板读取数不应再增加"

    print("  模板读取次数分布:", svc.template_reads())
    print("  缓存统计:", svc.cache_stats())


def test_failed_fill_not_published_and_retry_gets_complete_mapping():
    """填充失败不暴露半个字段集，新采集可重新取得完整映射。"""
    from app.services.data_extract import DataExtractService
    from app.services.extract_field_cache import FillFailed

    svc = DataExtractService()
    script_f = "BEHA-CACHE-F1"
    _, f_version = _make_template(svc, "DATA-CACHE-F1", script_f, "name,qty", "f1")

    def boom():
        raise RuntimeError("模板源抖动")

    svc.fill_hook = boom
    try:
        svc.run_extraction(script_f, [{"name": "x", "qty": "1"}])
        raise AssertionError("填充失败应抛 FillFailed")
    except FillFailed:
        pass
    assert svc._field_cache.get(f"current::{script_f}") is None, "失败后当前指针不应有半成品"
    assert svc._field_cache.get(f"versioned::{script_f}::{f_version}") is None, "失败后不应有半个字段集"
    assert persist.read_json("snapshots", f"extract_mapping_{script_f}_{f_version}.json") is None, \
        "失败的填充不应落盘"

    # 失败填充后，新的采集重新取得完整映射
    svc.fill_hook = None
    sample = svc.run_extraction(script_f, [{"name": "恢复", "qty": "2"}])
    assert [f["field"] for f in sample["fields"]] == ["name", "qty"], "重试应取得完整字段集"
    assert sample["rows"] == [{"name": "恢复", "qty": "2"}]
    assert svc.template_reads(script_f, f_version) == 2, "失败 1 次 + 重试 1 次"

    # 重试成功后稳定命中
    svc.run_extraction(script_f, [{"name": "再次", "qty": "3"}])
    assert svc.template_reads(script_f, f_version) == 2, "命中后不应重读模板"
    print("  失败后重试模板读取次数:", svc.template_reads(script_f, f_version))


if __name__ == "__main__":
    test_template_switch_parallel_reads_and_cancel()
    print("PASS: test_template_switch_parallel_reads_and_cancel")
    test_failed_fill_not_published_and_retry_gets_complete_mapping()
    print("PASS: test_failed_fill_not_published_and_retry_gets_complete_mapping")
    print("\nALL 2 TESTS PASSED")
