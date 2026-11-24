"""拾音工坊样本额度协调：计划版本、结算轮次、幂等入库、领用回执与崩溃对账。

数据流串联：sample_job(额度校验) → sample_all(判定批次) → store_events(调查样本) → 领用回执。

- 负责人每次改配在注册表生成新版本（待确认），确认后生效；
- 结算轮次创建时钉住计划当前版本，确认前可换用最新版——零额确认与
  全采生效冲突时按采用的版本处理，实际优先级决定样本何时写入库存；
- 明确零额度的运行正常结算但从不写样本；非法比例拒绝结算，不按零混过去；
- 同一代表并发确认只写一次；等待原行留在队列，重试不遗失；
- 回执中断后重开同次调查：核清已领走样本与所占额度，补记回执，
  不因待确认再送一次，也不因现为零抹去上次实际交付。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from app.core.dedup_sample import PRESET_SAMPLE_CONFIGS
from app.core.models import EventRecord, SampleConfig
from app.core.pipeline import IngestPipeline
from app.core.sample_verdict import (
    ClaimReceipt, InvestigationSample, SampleVerdict, SampleVerdictBatch,
    classify_sample_ratio, judge_sample_records,
)

_UNSET = object()


class SamplePlanRegistry:
    """采样计划注册表：负责人改配在此留下版本，确认后生效。"""

    def __init__(self, configs: list[SampleConfig] | None = None):
        self._versions: dict[str, dict[int, SampleConfig]] = {}
        for config in (configs if configs is not None else PRESET_SAMPLE_CONFIGS):
            self.register(config)

    def register(self, config: SampleConfig) -> None:
        self._versions.setdefault(config.plan_id, {})[config.version] = config

    def plan_ids(self) -> list[str]:
        return sorted(self._versions)

    def version(self, plan_id: str, version: int) -> SampleConfig:
        try:
            return self._versions[plan_id][version]
        except KeyError:
            raise ValueError(f"采样计划 {plan_id} 不存在版本 v{version}") from None

    def current(self, plan_id: str) -> SampleConfig:
        """最新提交的版本（无论是否已生效）。"""
        versions = self._versions.get(plan_id)
        if not versions:
            raise ValueError(f"采样计划 {plan_id} 未配置")
        return versions[max(versions)]

    def effective(self, plan_id: str) -> SampleConfig:
        """实际生效版本：已生效版本里版本号最大者。"""
        versions = self._versions.get(plan_id)
        if not versions:
            raise ValueError(f"采样计划 {plan_id} 未配置")
        confirmed = [v for v, cfg in versions.items() if cfg.state == "已生效"]
        if confirmed:
            return versions[max(confirmed)]
        return self.current(plan_id)

    def submit(self, plan_id: str, *,
               sample_ratio: object = _UNSET,
               merge_key: object = _UNSET,
               merge_window_minutes: object = _UNSET,
               retain_policy: object = _UNSET) -> SampleConfig:
        """负责人改配：基于当前版本生成新版本（待确认）。"""
        base = self.current(plan_id)
        config = SampleConfig(
            plan_id=base.plan_id,
            event_type_id=base.event_type_id,
            sample_ratio=base.sample_ratio if sample_ratio is _UNSET else sample_ratio,
            merge_key=base.merge_key if merge_key is _UNSET else merge_key,
            merge_window_minutes=(base.merge_window_minutes
                                  if merge_window_minutes is _UNSET else merge_window_minutes),
            retain_policy=(base.retain_policy
                           if retain_policy is _UNSET else retain_policy),
            version=base.version + 1,
            state="待确认",
        )
        self.register(config)
        return config

    def confirm_version(self, plan_id: str, version: int) -> SampleConfig:
        """确认采用指定版本：该版本生效，更旧的版本作废，更新的版本不受影响。

        实际优先级：生效版本取版本号最大者——零版先确认则零版先生效，
        新版随后确认则新版取代；确认前换用新版则新版直接生效。
        """
        versions = self._versions.get(plan_id)
        if not versions or version not in versions:
            raise ValueError(f"采样计划 {plan_id} 不存在版本 v{version}")
        target = versions[version]
        target.state = "已生效"
        for ver, cfg in versions.items():
            if ver >= version:
                continue
            if cfg.state in ("已生效", "待确认"):
                cfg.state = "已作废"
        return target


@dataclass
class QuotaRun:
    """一轮额度结算：创建时钉住计划当前版本，确认前可换用最新版。"""
    run_id: str
    investigation_id: str
    plan_id: str
    config_version: int
    intake: list[EventRecord]
    state: str = "待确认"      # 待确认/已确认/已拒绝/已中断
    settled_zero: bool = False
    rejection: str = ""
    batch: SampleVerdictBatch | None = None


class QuotaLedger:
    """额度端账本：回执、等待队列与结算历史。协调器中断后账本仍在。"""

    def __init__(self) -> None:
        self.receipts: dict[str, ClaimReceipt] = {}
        self.receipt_by_sample: dict[str, str] = {}
        self.waiting: dict[tuple[str, str], list[EventRecord]] = {}
        self.runs: dict[str, QuotaRun] = {}
        self._run_seq = 0

    def next_run_id(self, plan_id: str, investigation_id: str) -> str:
        self._run_seq += 1
        return f"RUN-{plan_id}-{investigation_id}-{self._run_seq:04d}"

    def record_receipt(self, receipt: ClaimReceipt) -> None:
        self.receipts[receipt.receipt_id] = receipt
        self.receipt_by_sample[receipt.sample_id] = receipt.receipt_id

    def receipt_for(self, sample_id: str) -> ClaimReceipt | None:
        receipt_id = self.receipt_by_sample.get(sample_id)
        return self.receipts.get(receipt_id) if receipt_id else None

    def waiting_queue(self, plan_id: str, investigation_id: str) -> list[EventRecord]:
        return list(self.waiting.get((plan_id, investigation_id), []))

    def set_waiting(self, plan_id: str, investigation_id: str,
                    records: list[EventRecord]) -> None:
        """整体替换等待队列：未落地的原行留待下轮，重试不遗失。"""
        self.waiting[(plan_id, investigation_id)] = list(records)


@dataclass
class ReconcileReport:
    """重开同次调查的对账结果。"""
    investigation_id: str
    plan_id: str
    claimed_sample_ids: list[str]            # 已经领走的样本
    recovered_receipts: list[ClaimReceipt]   # 补记的领用回执
    quota_occupied: int                      # 已占额度
    waiting_count: int                       # 仍在等待队列的原行
    redelivered_count: int = 0               # 重送次数（必须恒为 0）


class SampleQuotaCoordinator:
    """样本额度协调器：串联判定、入库、回执；崩溃后可换新实例继续。"""

    def __init__(self, registry: SamplePlanRegistry, ledger: QuotaLedger,
                 sample_store: IngestPipeline):
        self.registry = registry
        self.ledger = ledger
        self.sample_store = sample_store
        self._lock = threading.RLock()

    # ---------- 结算轮次 ----------

    def open_run(self, plan_id: str, investigation_id: str,
                 records: list[EventRecord] | tuple[EventRecord, ...] = ()) -> QuotaRun:
        """开启一轮结算：钉住计划当前版本，等待队列的原行随新到事件一起重审。"""
        with self._lock:
            config = self.registry.current(plan_id)
            intake = self.ledger.waiting_queue(plan_id, investigation_id) + list(records)
            run = QuotaRun(
                run_id=self.ledger.next_run_id(plan_id, investigation_id),
                investigation_id=investigation_id,
                plan_id=plan_id,
                config_version=config.version,
                intake=intake,
            )
            self.ledger.runs[run.run_id] = run
            return run

    def judge(self, run: QuotaRun) -> SampleVerdictBatch:
        """对一轮结算做无副作用判定，产出样本额度判定批次。"""
        config = self.registry.version(run.plan_id, run.config_version)
        batch = SampleVerdictBatch(investigation_id=run.investigation_id)
        batch.items.extend(judge_sample_records(run.intake, config))
        return batch

    def confirm_run(self, run: QuotaRun, *,
                    adopt_latest: bool = False,
                    crash_before_receipt: bool = False) -> SampleVerdictBatch:
        """确认一轮结算（幂等：并发确认只执行一次）。

        - adopt_latest：确认前换用完整新版（版本冲突时按采用的版本处理）；
        - 明确零额度：正常结算，从不写样本；
        - 非法比例：拒绝结算，不按零额度混过去；
        - crash_before_receipt：模拟额度端在记录领用回执前中断。
        """
        with self._lock:
            if run.state in ("已确认", "已拒绝"):
                return run.batch  # 幂等：同一轮不重复写
            if run.state == "已中断":
                # 回执中断后的重试：只补记回执，不重判、不动等待队列、
                # 不新增样本——等待的原行不能在重试中遗失。
                batch = run.batch
                self._record_receipts(self._delivered_samples(run, batch))
                run.state = "已确认"
                self.registry.confirm_version(run.plan_id, run.config_version)
                return batch
            if adopt_latest:
                run.config_version = self.registry.current(run.plan_id).version
            config = self.registry.version(run.plan_id, run.config_version)
            batch = self.judge(run)
            run.batch = batch
            ratio_state = classify_sample_ratio(config.sample_ratio)

            if ratio_state is SampleVerdict.INVALID_RATIO:
                run.state = "已拒绝"
                run.rejection = f"非法采样比例 {config.sample_ratio!r}：不按零额度结算"
                self._update_waiting(run, granted=[])
                return batch

            if ratio_state is SampleVerdict.ZERO_QUOTA:
                run.state = "已确认"
                run.settled_zero = True
                self._update_waiting(run, granted=[])
                self.registry.confirm_version(run.plan_id, run.config_version)
                return batch

            # 合法比例：归并许可代表写入库存（幂等），再记领用回执
            run_samples: list[InvestigationSample] = []
            for item in batch.granted_items():
                sample = InvestigationSample(
                    sample_id=InvestigationSample.build_id(
                        run.investigation_id, item.plan_id,
                        item.merge_group, item.record.record_id),
                    investigation_id=run.investigation_id,
                    plan_id=item.plan_id,
                    config_version=item.config_version,
                    merge_group=item.merge_group,
                    record=item.record,
                )
                stored, _ = self.sample_store.store_investigation_sample(sample)
                run_samples.append(stored)
            self._update_waiting(run, granted=batch.granted())
            if crash_before_receipt:
                # 额度端在回执前中断：库存已收下并派发，账本无回执
                run.state = "已中断"
                return batch
            self._record_receipts(run_samples)
            run.state = "已确认"
            self.registry.confirm_version(run.plan_id, run.config_version)
            return batch

    def _update_waiting(self, run: QuotaRun,
                        granted: list[EventRecord]) -> None:
        """等待队列并集更新：本轮未许可的原行留队，已许可代表出队。

        队列只进不出（除许可出队），并发/重试轮次不会冲掉后续到达的原行。
        """
        granted_ids = {record.record_id for record in granted}
        merged: list[EventRecord] = []
        seen: set[str] = set()
        for record in self.ledger.waiting_queue(run.plan_id, run.investigation_id) + list(run.intake):
            if record.record_id in seen or record.record_id in granted_ids:
                continue
            seen.add(record.record_id)
            merged.append(record)
        self.ledger.set_waiting(run.plan_id, run.investigation_id, merged)

    def _delivered_samples(self, run: QuotaRun,
                           batch: SampleVerdictBatch) -> list[InvestigationSample]:
        """找出本轮已交付库存的调查样本（供中断重试补记回执）。"""
        samples: list[InvestigationSample] = []
        for item in batch.granted_items():
            sample_id = InvestigationSample.build_id(
                run.investigation_id, item.plan_id,
                item.merge_group, item.record.record_id)
            sample = self.sample_store.find_investigation_sample(sample_id)
            if sample is not None:
                samples.append(sample)
        return samples

    def _record_receipts(self, samples: list[InvestigationSample]) -> None:
        """为已派发样本登记领用回执（幂等：已登记的跳过）。"""
        for sample in samples:
            if self.ledger.receipt_for(sample.sample_id) is not None:
                sample.state = "已确认"
                continue
            self.ledger.record_receipt(ClaimReceipt(
                receipt_id=f"RECE-{sample.sample_id[5:]}",
                investigation_id=sample.investigation_id,
                plan_id=sample.plan_id,
                sample_id=sample.sample_id,
                merge_group=sample.merge_group,
                config_version=sample.config_version,
                quota_consumed=1,
            ))
            sample.state = "已确认"

    # ---------- 崩溃恢复：重开同次调查 ----------

    def reopen_investigation(self, plan_id: str,
                             investigation_id: str) -> ReconcileReport:
        """重开同次调查：核清已领走样本与所占额度，补记缺失回执。

        不因待确认而再送一次（本方法不做任何新交付），
        也不因现为零就抹去上次实际交付（库存样本保留并补记回执）。
        """
        with self._lock:
            samples = self.sample_store.investigation_samples(
                plan_id=plan_id, investigation_id=investigation_id)
            recovered: list[ClaimReceipt] = []
            for sample in samples:
                if self.ledger.receipt_for(sample.sample_id) is None:
                    receipt = ClaimReceipt(
                        receipt_id=f"RECE-{sample.sample_id[5:]}",
                        investigation_id=sample.investigation_id,
                        plan_id=sample.plan_id,
                        sample_id=sample.sample_id,
                        merge_group=sample.merge_group,
                        config_version=sample.config_version,
                        quota_consumed=1,
                        state="补记",
                    )
                    self.ledger.record_receipt(receipt)
                    recovered.append(receipt)
                sample.state = "已确认"
            return ReconcileReport(
                investigation_id=investigation_id,
                plan_id=plan_id,
                claimed_sample_ids=[s.sample_id for s in samples],
                recovered_receipts=recovered,
                quota_occupied=len(samples),
                waiting_count=len(self.ledger.waiting_queue(plan_id, investigation_id)),
                redelivered_count=0,
            )
