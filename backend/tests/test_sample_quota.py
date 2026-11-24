"""测试：样本额度判定批次与调查样本领用的稳定性。

覆盖数据流：sample_job(额度校验) → sample_all(判定批次) → store_events(调查样本) → 领用回执

- 未配置 / 明确零额度 / 非法比例三类源事件分流
- 先归并后名额：倒序输入仍取组内最新代表；名额不足一组允许为空
- 零额确认与全采生效的版本冲突（两种顺序）
- 并发确认幂等；等待原行在重试中不遗失
- 回执中断后重开同次调查的对账：不重送、不抹除、新领用遵守零额度
- sample_all → store_events 数据流串联
"""
from __future__ import annotations

import sys
import os
import threading
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.models import EventRecord, EventTypeDef, SampleConfig
from app.core.pipeline import IngestPipeline
from app.core.dedup_sample import DedupSamplePipeline
from app.core.sample_verdict import (
    SampleVerdict, classify_sample_ratio,
)
from app.core.sample_quota import (
    QuotaLedger, SamplePlanRegistry, SampleQuotaCoordinator,
)

BASE = datetime(2026, 11, 24, 12, 0, 0)
T1 = BASE
T2 = BASE + timedelta(seconds=1)
T3 = BASE + timedelta(seconds=2)


def make_event(record_id: str, type_id: str = "EVEN-0001",
               ip: str = "10.0.0.1", event_time: datetime = T1) -> EventRecord:
    return EventRecord(
        record_id=record_id,
        event_type=EventTypeDef(
            type_id=type_id, type_name=f"type-{type_id}",
            category_tag="access", severity="INFO",
        ),
        original_content=f"raw line of {record_id}",
        parsed_fields={"client_ip": ip, "url": "/api/users"},
        masked_fields={},
        enriched_fields={},
        event_time=event_time,
        source_host=ip,
        store_partition="20261124",
    )


def make_plan(ratio, plan_id="SAMP-T", merge_key="client_ip") -> SampleConfig:
    return SampleConfig(
        plan_id=plan_id, event_type_id="EVEN-0001",
        sample_ratio=ratio, merge_key=merge_key,
    )


def make_coordinator(ratio=1.0, plan_id="SAMP-T"):
    registry = SamplePlanRegistry([make_plan(ratio, plan_id)])
    ledger = QuotaLedger()
    store = IngestPipeline()
    return registry, ledger, store, SampleQuotaCoordinator(registry, ledger, store)


# ---------- 缺配 / 零 / 非法配置的真实采样结果 ----------

class TestVerdictBatchSeparation:
    """未配置、明确零额度和非法比例对应的源事件必须分开。"""

    def test_classify_ratio_boundaries(self):
        """比例校验：零是零、非法是非法，负比例不能靠改成零混过去。"""
        assert classify_sample_ratio(0.5) is None
        assert classify_sample_ratio(1.0) is None
        assert classify_sample_ratio(0.0) is SampleVerdict.ZERO_QUOTA
        assert classify_sample_ratio(-0.5) is SampleVerdict.INVALID_RATIO
        assert classify_sample_ratio(1.5) is SampleVerdict.INVALID_RATIO
        assert classify_sample_ratio("abc") is SampleVerdict.INVALID_RATIO
        assert classify_sample_ratio(None) is SampleVerdict.INVALID_RATIO

    def test_batch_separates_unconfigured_zero_invalid(self):
        """判定批次把缺配、零额度、非法比例的源事件分开，库存只收归并许可代表。"""
        configs = [
            SampleConfig(plan_id="SAMP-Z", event_type_id="EVEN-Z",
                         sample_ratio=0.0, merge_key="client_ip"),
            SampleConfig(plan_id="SAMP-I", event_type_id="EVEN-I",
                         sample_ratio=-0.2, merge_key="client_ip"),
            SampleConfig(plan_id="SAMP-L", event_type_id="EVEN-L",
                         sample_ratio=1.0, merge_key="client_ip"),
        ]
        pipeline = DedupSamplePipeline(sample_configs=configs)
        zero_ev = make_event("EVEN-Z-1", type_id="EVEN-Z", event_time=T1)
        bad_ev = make_event("EVEN-I-1", type_id="EVEN-I", event_time=T1)
        legal_old = make_event("EVEN-L-1", type_id="EVEN-L", event_time=T1)
        legal_new = make_event("EVEN-L-2", type_id="EVEN-L", event_time=T2)
        free_ev = make_event("EVEN-U-1", type_id="EVEN-UNCONF", event_time=T1)

        batch = pipeline.sample_all_batch(
            [zero_ev, bad_ev, legal_old, legal_new, free_ev],
            investigation_id="INV-SEP",
        )

        assert batch.zero_quota() == [zero_ev]
        assert batch.invalid_ratio() == [bad_ev]
        assert batch.unconfigured() == [free_ev]
        assert batch.granted() == [legal_new]       # 同键归并后取最新代表
        assert batch.waiting() == [legal_old]

        store = IngestPipeline()
        stored = store.store_events(batch)
        samples = store.investigation_samples()
        assert len(samples) == 1 and samples[0].record is legal_new
        assert samples[0].state == "已派发" and samples[0].delivered_to == "调查员"
        # 未配置类型保持缺省行为：原样写入事件库存
        default_rows = sum(len(v) for v in store.get_event_store().values())
        assert default_rows == 1
        assert stored == 2  # 1 调查样本 + 1 缺省记录；零与非法不落库


# ---------- 合法比例：先归并后名额 ----------

class TestMergeThenQuota:
    """名额按归并后的独立组数计算，再从每个入选组取实际最新代表。"""

    def test_quota_counts_merged_groups(self):
        """两个同键和一个异键的三条输入，比例 0.5 时按两组只取一个代表。"""
        pipeline = DedupSamplePipeline(sample_configs=[make_plan(0.5)])
        e_old = make_event("EVEN-K1-OLD", ip="10.0.0.1", event_time=T1)
        e_new = make_event("EVEN-K1-NEW", ip="10.0.0.1", event_time=T3)
        e_other = make_event("EVEN-K2-ONE", ip="10.0.0.2", event_time=T2)

        batch = pipeline.sample_all_batch([e_old, e_other, e_new])
        granted = batch.granted()
        assert len(granted) == 1                    # 两组 × 0.5 = 一个名额
        assert granted[0] is e_new                  # 入选组的实际最新代表
        assert sorted(r.record_id for r in batch.waiting()) == [
            "EVEN-K1-OLD", "EVEN-K2-ONE"]

    def test_reversed_input_keeps_group_latest(self):
        """倒序输入时被选代表仍是该组最新。"""
        pipeline = DedupSamplePipeline(sample_configs=[make_plan(0.5)])
        e_old = make_event("EVEN-K1-OLD", ip="10.0.0.1", event_time=T1)
        e_new = make_event("EVEN-K1-NEW", ip="10.0.0.1", event_time=T3)
        e_other = make_event("EVEN-K2-ONE", ip="10.0.0.2", event_time=T2)

        batch = pipeline.sample_all_batch([e_new, e_other, e_old])  # 反序到达
        granted = batch.granted()
        assert len(granted) == 1
        assert granted[0] is e_new
        assert granted[0].event_time == T3

    def test_sub_group_quota_may_be_empty(self):
        """低于一组的名额允许为空，不能强行至少一条。"""
        pipeline = DedupSamplePipeline(sample_configs=[make_plan(0.4)])
        events = [
            make_event("EVEN-K1-OLD", ip="10.0.0.1", event_time=T1),
            make_event("EVEN-K1-NEW", ip="10.0.0.1", event_time=T3),
            make_event("EVEN-K2-ONE", ip="10.0.0.2", event_time=T2),
        ]
        config = make_plan(0.4)
        assert pipeline.sample(events, config) == []   # int(2 × 0.4) = 0
        batch = pipeline.sample_all_batch(events)
        assert batch.granted() == []
        assert len(batch.waiting()) == 3               # 原行留待下轮，不遗失


# ---------- 明确零额度与非法比例的结算 ----------

class TestZeroAndInvalidRuns:
    """明确零的运行从不写样本；非法负比例不能靠改成零混过去。"""

    def test_zero_quota_settles_normally_without_samples(self):
        """同计划两笔同键事件反序到达，明确零额度结算正常完成，不写任何样本。"""
        registry, ledger, store, coord = make_coordinator(0.0, "SAMP-Z")
        e1 = make_event("EVEN-Z-1", ip="10.0.0.1", event_time=T1)
        e2 = make_event("EVEN-Z-2", ip="10.0.0.1", event_time=T2)

        run = coord.open_run("SAMP-Z", "INV-Z", [e2, e1])   # 反序到达
        batch = coord.confirm_run(run)

        assert run.state == "已确认" and run.settled_zero
        assert len(batch.zero_quota()) == 2
        assert store.investigation_samples() == []          # 零额度从不写样本
        assert ledger.receipts == {}
        assert len(ledger.waiting_queue("SAMP-Z", "INV-Z")) == 2  # 原行未遗失

    def test_invalid_negative_ratio_not_settled_as_zero(self):
        """非法负比例拒绝结算，不能按零额度混过去；修正后等待原行可继续。"""
        registry, ledger, store, coord = make_coordinator(1.0, "SAMP-N")
        registry.submit("SAMP-N", sample_ratio=-0.5)        # v2 非法负比例
        e1 = make_event("EVEN-N-1", ip="10.0.0.1", event_time=T1)
        e2 = make_event("EVEN-N-2", ip="10.0.0.1", event_time=T2)

        run = coord.open_run("SAMP-N", "INV-N", [e2, e1])
        batch = coord.confirm_run(run)

        assert run.state == "已拒绝"
        assert not run.settled_zero                         # 没有按零结算
        assert len(batch.invalid_ratio()) == 2
        assert batch.zero_quota() == []                     # 非法 ≠ 零
        assert store.investigation_samples() == []
        assert ledger.receipts == {}
        assert registry.effective("SAMP-N").version == 1    # 非法版本未生效

        # 等待的原行不能在重试中遗失：修正配置后重开一轮即可继续
        registry.submit("SAMP-N", sample_ratio=1.0)         # v3 恢复全采
        registry.confirm_version("SAMP-N", 3)
        run2 = coord.open_run("SAMP-N", "INV-N", [])
        coord.confirm_run(run2)
        samples = store.investigation_samples()
        assert len(samples) == 1 and samples[0].record is e2


# ---------- 零额确认与全采生效的版本冲突 ----------

class TestVersionConflict:
    """零版与新额度冲突时按采用的版本处理，优先级决定样本何时写入库存。"""

    def test_settle_zero_first_then_newer_representative_next_round(self):
        """本轮先结清零版：本轮不写，下轮可选时间更新的代表。"""
        registry, ledger, store, coord = make_coordinator(1.0, "SAMP-C")
        registry.submit("SAMP-C", sample_ratio=0.0)         # v2 零额待确认
        e1 = make_event("EVEN-C-1", ip="10.0.0.1", event_time=T1)
        e2 = make_event("EVEN-C-2", ip="10.0.0.1", event_time=T2)
        run1 = coord.open_run("SAMP-C", "INV-A", [e2, e1])
        assert run1.config_version == 2                     # 钉住零版
        registry.submit("SAMP-C", sample_ratio=1.0)         # v3 恢复全采 → 冲突

        batch1 = coord.confirm_run(run1)                    # 本轮先结清零版
        assert run1.settled_zero
        assert store.investigation_samples() == []          # 本轮不写
        assert registry.effective("SAMP-C").version == 2

        registry.confirm_version("SAMP-C", 3)               # 负责人确认全采新版
        e3 = make_event("EVEN-C-3", ip="10.0.0.1", event_time=T3)
        run2 = coord.open_run("SAMP-C", "INV-A", [e3])
        coord.confirm_run(run2)
        samples = store.investigation_samples()
        assert len(samples) == 1
        assert samples[0].record is e3                      # 下轮选时间更新的代表

    def test_adopt_full_version_before_confirm_writes_immediately(self):
        """确认前换用完整新版：本轮立即写该代表。"""
        registry, ledger, store, coord = make_coordinator(1.0, "SAMP-C")
        registry.submit("SAMP-C", sample_ratio=0.0)         # v2 零额待确认
        e1 = make_event("EVEN-C-1", ip="10.0.0.1", event_time=T1)
        e2 = make_event("EVEN-C-2", ip="10.0.0.1", event_time=T2)
        run1 = coord.open_run("SAMP-C", "INV-B", [e2, e1])
        registry.submit("SAMP-C", sample_ratio=1.0)         # v3 恢复全采 → 冲突

        batch1 = coord.confirm_run(run1, adopt_latest=True)  # 换用完整新版
        assert run1.config_version == 3
        assert not run1.settled_zero
        samples = store.investigation_samples()
        assert len(samples) == 1
        assert samples[0].record is e2                      # 本轮立即写该代表
        assert registry.effective("SAMP-C").version == 3


# ---------- 并发确认幂等 ----------

class TestConcurrentConfirm:
    """同一代表在并发确认时只写一次。"""

    def test_concurrent_confirm_writes_representative_once(self):
        registry, ledger, store, coord = make_coordinator(1.0, "SAMP-P")
        e1 = make_event("EVEN-P-1", ip="10.0.0.1", event_time=T1)
        e2 = make_event("EVEN-P-2", ip="10.0.0.1", event_time=T2)
        run = coord.open_run("SAMP-P", "INV-P", [e2, e1])

        threads = [
            threading.Thread(target=coord.confirm_run, args=(run,))
            for _ in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert run.state == "已确认"
        samples = store.investigation_samples()
        assert len(samples) == 1                            # 只写一次
        assert samples[0].record is e2
        assert len(ledger.receipts) == 1                    # 回执也只有一份


# ---------- 回执中断与重开同次调查 ----------

class TestCrashRecovery:
    """库存已派发、额度端未记回执就中断；负责人再次置零后重开同次调查。"""

    def test_reopen_reconciles_without_redelivery_or_erasure(self):
        registry, ledger, store, coord = make_coordinator(1.0, "SAMP-R")
        e1 = make_event("EVEN-R-1", ip="10.0.0.1", event_time=T1)
        e2 = make_event("EVEN-R-2", ip="10.0.0.1", event_time=T2)

        # 恢复全采后：库存收下最新代表并派发，额度端尚未记录回执就中断
        run1 = coord.open_run("SAMP-R", "INV-9", [e2, e1])  # 反序到达
        coord.confirm_run(run1, crash_before_receipt=True)
        assert run1.state == "已中断"
        samples = store.investigation_samples()
        assert len(samples) == 1 and samples[0].record is e2
        assert samples[0].state == "已派发"
        assert ledger.receipts == {}                        # 回执未记

        # 额度端重启（账本与库存仍在），负责人再次把计划置零
        coord2 = SampleQuotaCoordinator(registry, ledger, store)
        registry.submit("SAMP-R", sample_ratio=0.0)         # v2 置零
        registry.confirm_version("SAMP-R", 2)

        # 重开同次调查：核清已领走样本与所占额度
        report = coord2.reopen_investigation("SAMP-R", "INV-9")
        assert report.claimed_sample_ids == [samples[0].sample_id]
        assert len(report.recovered_receipts) == 1
        assert report.recovered_receipts[0].state == "补记"
        assert report.quota_occupied == 1
        assert report.redelivered_count == 0                # 不因待确认再送一次
        assert report.waiting_count == 1                    # 被归并的原行仍在队列
        # 不因现为零就抹去上次实际交付
        samples = store.investigation_samples()
        assert len(samples) == 1 and samples[0].state == "已确认"

        # 新领用遵守零额度：更新的代表到达也不写样本
        e3 = make_event("EVEN-R-3", ip="10.0.0.1", event_time=T3)
        run2 = coord2.open_run("SAMP-R", "INV-9", [e3])
        coord2.confirm_run(run2)
        assert run2.settled_zero
        assert len(store.investigation_samples()) == 1
        assert len(ledger.waiting_queue("SAMP-R", "INV-9")) == 2  # 等待原行有去向

        # 回执失败后的重试不增添记录
        coord2.confirm_run(run1)                            # 中断轮次重试
        assert run1.state == "已确认"
        assert len(store.investigation_samples()) == 1      # 库存未增添
        assert len(ledger.receipts) == 1                    # 回执未重复
        assert registry.effective("SAMP-R").version == 2    # 零额度仍生效

        # 等待原行的后续去向：恢复全采后可继续领用
        registry.submit("SAMP-R", sample_ratio=1.0)         # v3 再恢复全采
        registry.confirm_version("SAMP-R", 3)
        run3 = coord2.open_run("SAMP-R", "INV-9", [])
        coord2.confirm_run(run3)
        samples = store.investigation_samples()
        assert len(samples) == 2
        assert samples[-1].record is e3                     # 等待中的最新代表入库


# ---------- 数据流串联：sample_all → store_events ----------

class TestDataFlowWiring:
    """沿数据流串联入口 sample_all 和 store_events。"""

    def test_sample_all_to_store_events_wiring(self):
        dedup = DedupSamplePipeline(sample_configs=[make_plan(0.5, "SAMP-W")])
        ingest = IngestPipeline()
        e_old = make_event("EVEN-W-1", ip="10.0.0.1", event_time=T1)
        e_new = make_event("EVEN-W-3", ip="10.0.0.1", event_time=T3)
        e_other = make_event("EVEN-W-2", ip="10.0.0.2", event_time=T2)
        free_ev = make_event("EVEN-W-9", type_id="EVEN-0003", event_time=T1)
        records = [e_old, e_other, e_new, free_ev]

        batch = dedup.sample_all_batch(records, investigation_id="INV-W")
        stored = ingest.store_events(batch)

        samples = ingest.investigation_samples()
        assert len(samples) == 1 and samples[0].record is e_new
        assert samples[0].investigation_id == "INV-W"
        assert samples[0].plan_id == "SAMP-W"
        # 未配置类型沿缺省入口写入事件库存
        default_rows = sum(len(v) for v in ingest.get_event_store().values())
        assert default_rows == 1
        assert stored == 2

    def test_sample_all_compat_entry_and_unconfigured_default(self):
        """sample_all 兼容出口：归并许可代表 + 未配置缺省通过。"""
        dedup = DedupSamplePipeline(sample_configs=[make_plan(0.5, "SAMP-W")])
        e_old = make_event("EVEN-W-1", ip="10.0.0.1", event_time=T1)
        e_new = make_event("EVEN-W-3", ip="10.0.0.1", event_time=T3)
        e_other = make_event("EVEN-W-2", ip="10.0.0.2", event_time=T2)
        free_ev = make_event("EVEN-W-9", type_id="EVEN-0003", event_time=T1)

        out = dedup.sample_all([e_old, e_other, e_new, free_ev])
        assert len(out) == 2                                # 1 代表 + 1 未配置
        assert e_new in out and free_ev in out

        # 未配置的类型继续沿原入口处理
        only_free = dedup.sample_all([free_ev])
        assert only_free == [free_ev]


# ---------- 运行入口（确定性测试，不依赖 pytest） ----------

if __name__ == "__main__":
    import traceback

    tests = {
        "classify_ratio_boundaries": TestVerdictBatchSeparation().test_classify_ratio_boundaries,
        "batch_separates_unconfigured_zero_invalid": TestVerdictBatchSeparation().test_batch_separates_unconfigured_zero_invalid,
        "quota_counts_merged_groups": TestMergeThenQuota().test_quota_counts_merged_groups,
        "reversed_input_keeps_group_latest": TestMergeThenQuota().test_reversed_input_keeps_group_latest,
        "sub_group_quota_may_be_empty": TestMergeThenQuota().test_sub_group_quota_may_be_empty,
        "zero_quota_settles_normally": TestZeroAndInvalidRuns().test_zero_quota_settles_normally_without_samples,
        "invalid_negative_ratio_not_zero": TestZeroAndInvalidRuns().test_invalid_negative_ratio_not_settled_as_zero,
        "conflict_settle_zero_first": TestVersionConflict().test_settle_zero_first_then_newer_representative_next_round,
        "conflict_adopt_full_first": TestVersionConflict().test_adopt_full_version_before_confirm_writes_immediately,
        "concurrent_confirm_once": TestConcurrentConfirm().test_concurrent_confirm_writes_representative_once,
        "crash_reopen_reconciles": TestCrashRecovery().test_reopen_reconciles_without_redelivery_or_erasure,
        "wiring_sample_all_store_events": TestDataFlowWiring().test_sample_all_to_store_events_wiring,
        "wiring_compat_and_unconfigured": TestDataFlowWiring().test_sample_all_compat_entry_and_unconfigured_default,
    }

    passed = 0
    failed = 0
    for name, test_func in tests.items():
        try:
            test_func()
            passed += 1
            print(f"  PASS  test_sample_quota: {name}")
        except Exception:
            failed += 1
            print(f"  FAIL  test_sample_quota: {name}")
            traceback.print_exc()

    print(f"\ntest_sample_quota: {passed} passed, {failed} failed, {len(tests)} total")
    sys.exit(1 if failed else 0)
