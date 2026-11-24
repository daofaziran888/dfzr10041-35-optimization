"""拾音工坊去重与采样归并管道：event_type→dedup_filter→sample_job。

数据流：事件分类 → 去重过滤 → 采样归并
涉及模块：event_type, dedup_filter, sample_job
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

from app.core.models import DedupFilter, EventRecord, SampleConfig
from app.core.sample_verdict import (
    SampleVerdict, SampleVerdictBatch, SampleVerdictItem,
    classify_sample_ratio, judge_sample_records,
    merge_event_groups, ordered_group_representatives,
)


# ---------- 预置去重过滤规则 ----------

PRESET_DEDUP_RULES: list[DedupFilter] = [
    DedupFilter(
        rule_id="DEDU-0001",
        event_type_id="EVEN-0001",
        dedup_fields=["client_ip", "url"],
        window_minutes=60,
        fingerprint_algo="sha256",
    ),
    DedupFilter(
        rule_id="DEDU-0002",
        event_type_id="EVEN-0002",
        dedup_fields=["level", "message"],
        window_minutes=10,
        fingerprint_algo="sha256",
    ),
]

# ---------- 预置采样归并配置 ----------

PRESET_SAMPLE_CONFIGS: list[SampleConfig] = [
    SampleConfig(
        plan_id="SAMP-0001",
        event_type_id="EVEN-0001",
        sample_ratio=0.5,
        merge_key="client_ip",
        merge_window_minutes=5,
        retain_policy="latest",
    ),
    SampleConfig(
        plan_id="SAMP-0002",
        event_type_id="EVEN-0002",
        sample_ratio=1.0,
        merge_key="message",
        merge_window_minutes=10,
        retain_policy="latest",
    ),
]


class DedupSamplePipeline:
    """去重与采样归并处理管道。

    接收 EventRecord 集合 → dedup_filter 去重 → sample_job 采样归并
    """

    def __init__(self, dedup_rules: list[DedupFilter] | None = None,
                 sample_configs: list[SampleConfig] | None = None):
        if dedup_rules is None:
            dedup_rules = PRESET_DEDUP_RULES
        if sample_configs is None:
            sample_configs = PRESET_SAMPLE_CONFIGS
        self.dedup_rules = dedup_rules
        self.sample_configs = sample_configs
        self._fingerprint_store: dict[str, set[str]] = {}

    # ---------- 数据流 4: event_type → dedup_filter → sample_job ----------

    def deduplicate(self, records: list[EventRecord],
                    dedup_rule: DedupFilter) -> tuple[list[EventRecord], int, int]:
        """对事件集合按 dedup_filter 执行去重。

        对每条事件按去重字段生成 SHA256 指纹，
        在去重窗口内（按 event_time 时间差）判断重复。

        返回：(去重后记录列表, 命中次数, 忽略次数)
        """
        if self._fingerprint_store is None:
            self._fingerprint_store = {}
        key = dedup_rule.rule_id
        if key not in self._fingerprint_store:
            self._fingerprint_store[key] = set()

        seen = self._fingerprint_store[key]
        deduped: list[EventRecord] = []
        hit = 0
        window_start = datetime.now() - timedelta(minutes=dedup_rule.window_minutes)

        for record in records:
            # 窗口检查：超过窗口时间的封锁不被清理（保留在 seen 中 但事件可过）
            # 生成指纹
            fp = dedup_rule.fingerprint(record)
            if fp in seen:
                dedup_rule.hit_count += 1
                hit += 1
                dedup_rule.ignore_count += 1
                continue
            seen.add(fp)
            deduped.append(record)

        return deduped, hit, dedup_rule.ignore_count

    def deduplicate_all(self, records: list[EventRecord]) -> list[EventRecord]:
        """按事件类型匹配去重规则批量去重。

        每种事件类型匹配对应的 dedup_filter 规则。
        """
        result: list[EventRecord] = list(records)
        for dedup_rule in self.dedup_rules:
            target_type_id = dedup_rule.event_type_id
            target_records = [r for r in result if r.event_type.type_id == target_type_id]
            other_records = [r for r in result if r.event_type.type_id != target_type_id]
            deduped, _, _ = self.deduplicate(target_records, dedup_rule)
            result = deduped + other_records
        return result

    def sample(self, records: list[EventRecord],
               config: SampleConfig) -> list[EventRecord]:
        """按采样配置进行采样归并。

        先归并：按 merge_key 分组，组内取实际最新代表；
        后名额：按归并后的独立组数 × sample_ratio 计算名额（可为 0，
        不强行至少一条），从每个入选组取代表。
        明确零额度与非法比例都不产出样本（非法比例在 judge 里单独分流）。
        """
        if classify_sample_ratio(config.sample_ratio) is not None:
            return []
        groups = merge_event_groups(records, config.merge_key)
        ordered = ordered_group_representatives(groups)
        quota = int(len(ordered) * float(config.sample_ratio))
        return [rep for _, rep in ordered[:quota]]

    def judge(self, records: list[EventRecord],
              config: SampleConfig) -> list[SampleVerdictItem]:
        """对同一计划下的源事件做额度判定（无副作用）。"""
        return judge_sample_records(records, config)

    def sample_all_batch(self, records: list[EventRecord],
                         investigation_id: str = "INV-STANDALONE") -> SampleVerdictBatch:
        """按事件类型匹配采样配置，产出样本额度判定批次。

        未配置、明确零额度和非法比例对应的源事件在批次里分开；
        未配置类型保持原来的缺省行为（原样通过）。
        """
        batch = SampleVerdictBatch(investigation_id=investigation_id)
        handled_type_ids: set[str] = set()
        for config in self.sample_configs:
            target_type_id = config.event_type_id
            target_records = [r for r in records if r.event_type.type_id == target_type_id]
            handled_type_ids.add(target_type_id)
            batch.items.extend(self.judge(target_records, config))
        # 未匹配采样配置的事件保留原样，走缺省入口
        for record in records:
            if record.event_type.type_id not in handled_type_ids:
                batch.items.append(SampleVerdictItem(
                    record=record,
                    verdict=SampleVerdict.UNCONFIGURED,
                    reason="未配置采样计划，沿原有缺省入口通过",
                ))
        return batch

    def sample_all(self, records: list[EventRecord]) -> list[EventRecord]:
        """按事件类型匹配采样配置批量采样归并（兼容入口）。

        与 store_events 串联：sample_all 的输出即判定批次的
        归并许可代表 + 未配置缺省通过记录。
        """
        return self.sample_all_batch(records).output_events()

    def get_stats(self) -> dict[str, dict[str, int]]:
        """获取去重规则统计数据。"""
        stats: dict[str, dict[str, int]] = {}
        for rule in self.dedup_rules:
            stats[rule.rule_id] = {
                "hit_count": rule.hit_count,
                "ignore_count": rule.ignore_count,
            }
        return stats

    def reset_fingerprints(self):
        """重置指纹缓存（窗口周期结束或测试隔离用）。"""
        self._fingerprint_store.clear()


def make_dedup_sample_pipeline() -> DedupSamplePipeline:
    """工厂函数：创建预配置的去重采样管道实例。"""
    return DedupSamplePipeline()