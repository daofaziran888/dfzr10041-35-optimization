"""拾音工坊样本额度判定：比例校验、先归并后名额、样本额度判定批次。

修复要点（沿 sample_job → sample_all → store_events 数据流）：
- 未配置 / 明确零额度 / 非法比例三类源事件分开判定，产出样本额度判定批次；
- 合法比例先按归并键归并，按归并后的独立组数计算名额，再取每个入选组的
  实际最新代表，不能先截原行才归并；
- 名额不足一组允许为空，不再强行至少一条；
- 非法比例（负数、超界、非数值）拒绝结算，不允许按零额度混过去。

本模块只做无副作用的判定：写库存、记回执、版本确认都在 sample_quota 协调层。
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from enum import Enum

from app.core.models import EventRecord, SampleConfig


class SampleVerdict(str, Enum):
    """样本额度判定结论——判定批次里每条源事件的归属。"""
    GRANTED = "归并许可"          # 名额内入选组的最新代表，可写入库存
    UNCONFIGURED = "未配置"        # 无采样计划，沿原有缺省入口通过
    ZERO_QUOTA = "明确零额度"      # 明确零额度：正常结算但不写样本
    INVALID_RATIO = "非法比例"     # 非法比例：拒绝结算，不按零额度处理
    WAITING = "等待"              # 名额之外或被同组更新代表归并，留待下轮


def classify_sample_ratio(ratio: object) -> SampleVerdict | None:
    """校验采样比例。返回 None 表示合法比例，否则给出零额度或非法比例判定。

    明确零额度与非法比例必须分开：负比例等非法配置不得靠改成零混过去。
    """
    try:
        value = float(ratio)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return SampleVerdict.INVALID_RATIO
    if math.isnan(value) or math.isinf(value):
        return SampleVerdict.INVALID_RATIO
    if value < 0.0 or value > 1.0:
        return SampleVerdict.INVALID_RATIO
    if value == 0.0:
        return SampleVerdict.ZERO_QUOTA
    return None


_SINGLETON_PREFIX = "__row__"


def merge_event_groups(records: list[EventRecord],
                       merge_key: str) -> dict[str, list[tuple[int, EventRecord]]]:
    """先归并：按归并键分组，保留到达序号用于同刻决胜。

    缺归并键值的记录各自成组（不丢弃原行）；先分组再算名额，
    不能先截原行才归并。
    """
    groups: dict[str, list[tuple[int, EventRecord]]] = {}
    for seq, record in enumerate(records):
        key = ""
        if merge_key:
            key = (
                record.parsed_fields.get(merge_key)
                or record.masked_fields.get(merge_key)
                or ""
            )
        if not key:
            key = f"{_SINGLETON_PREFIX}:{record.record_id}:{seq}"
        groups.setdefault(key, []).append((seq, record))
    return groups


def group_representative(members: list[tuple[int, EventRecord]]) -> EventRecord:
    """组内实际最新代表：按事件时间取最新，同刻取后到达者。

    与输入顺序无关——倒序到达时被选代表仍是该组最新。
    """
    return max(members, key=lambda pair: (pair[1].event_time, pair[0]))[1]


def ordered_group_representatives(
    groups: dict[str, list[tuple[int, EventRecord]]],
) -> list[tuple[str, EventRecord]]:
    """组间排序：代表时间更新的组优先入选，键名兜底保证确定性。"""
    reps = [(key, group_representative(members)) for key, members in groups.items()]
    reps.sort(key=lambda pair: (pair[1].event_time, pair[0]), reverse=True)
    return reps


@dataclass
class SampleVerdictItem:
    """判定批次条目：一条源事件的判定结论与归属。"""
    record: EventRecord
    verdict: SampleVerdict
    plan_id: str = ""
    config_version: int = 0
    merge_group: str = ""
    reason: str = ""


@dataclass
class SampleVerdictBatch:
    """样本额度判定批次：一次判定里全部源事件的分类结果。

    事件库存实际接收批次里经归并许可的代表（存成调查样本），
    未配置类型仍保持原来的缺省行为。
    """
    investigation_id: str = "INV-STANDALONE"
    items: list[SampleVerdictItem] = field(default_factory=list)

    def _items(self, verdict: SampleVerdict) -> list[SampleVerdictItem]:
        return [item for item in self.items if item.verdict is verdict]

    def granted_items(self) -> list[SampleVerdictItem]:
        return self._items(SampleVerdict.GRANTED)

    def waiting_items(self) -> list[SampleVerdictItem]:
        return self._items(SampleVerdict.WAITING)

    def granted(self) -> list[EventRecord]:
        return [item.record for item in self.granted_items()]

    def waiting(self) -> list[EventRecord]:
        return [item.record for item in self.waiting_items()]

    def zero_quota(self) -> list[EventRecord]:
        return [item.record for item in self._items(SampleVerdict.ZERO_QUOTA)]

    def invalid_ratio(self) -> list[EventRecord]:
        return [item.record for item in self._items(SampleVerdict.INVALID_RATIO)]

    def unconfigured(self) -> list[EventRecord]:
        return [item.record for item in self._items(SampleVerdict.UNCONFIGURED)]

    def output_events(self) -> list[EventRecord]:
        """sample_all 的兼容出口：归并许可代表 + 未配置缺省通过。"""
        return self.granted() + self.unconfigured()


def judge_sample_records(records: list[EventRecord],
                         config: SampleConfig) -> list[SampleVerdictItem]:
    """对同一计划下的源事件做额度判定（无副作用）。

    - 非法比例：全部判 INVALID_RATIO，拒绝结算；
    - 明确零额度：全部判 ZERO_QUOTA，正常结算但不写样本；
    - 合法比例：先归并，按独立组数 × 比例计算名额（可为 0），
      入选组的实际最新代表判 GRANTED，其余原行判 WAITING 留待下轮。
    """
    ratio_state = classify_sample_ratio(config.sample_ratio)
    if ratio_state is SampleVerdict.INVALID_RATIO:
        return [
            SampleVerdictItem(
                record=record, verdict=SampleVerdict.INVALID_RATIO,
                plan_id=config.plan_id, config_version=config.version,
                reason=f"非法采样比例 {config.sample_ratio!r}：拒绝结算，不按零额度处理",
            )
            for record in records
        ]
    if ratio_state is SampleVerdict.ZERO_QUOTA:
        return [
            SampleVerdictItem(
                record=record, verdict=SampleVerdict.ZERO_QUOTA,
                plan_id=config.plan_id, config_version=config.version,
                reason="明确零额度：正常结算但不写样本",
            )
            for record in records
        ]

    groups = merge_event_groups(records, config.merge_key)
    ordered = ordered_group_representatives(groups)
    # 名额按归并后的独立组数计算；低于一组的名额允许为空，不强行至少一条。
    quota = int(len(ordered) * float(config.sample_ratio))
    granted_keys = {key for key, _ in ordered[:quota]}

    items: list[SampleVerdictItem] = []
    for key, members in groups.items():
        synthetic = key.startswith(_SINGLETON_PREFIX)
        representative = group_representative(members)
        for _, record in members:
            label = record.record_id if synthetic else key
            if key in granted_keys and record is representative:
                items.append(SampleVerdictItem(
                    record=record, verdict=SampleVerdict.GRANTED,
                    plan_id=config.plan_id, config_version=config.version,
                    merge_group=label,
                    reason="归并许可：名额内入选组的最新代表",
                ))
            elif key in granted_keys:
                items.append(SampleVerdictItem(
                    record=record, verdict=SampleVerdict.WAITING,
                    plan_id=config.plan_id, config_version=config.version,
                    merge_group=label,
                    reason="同组已有更新代表，留待下轮",
                ))
            else:
                items.append(SampleVerdictItem(
                    record=record, verdict=SampleVerdict.WAITING,
                    plan_id=config.plan_id, config_version=config.version,
                    merge_group=label,
                    reason="名额之外，留待下轮",
                ))
    return items


@dataclass
class InvestigationSample:
    """调查样本——事件库存实际收下的归并许可代表。

    状态流转：已入库 → 已派发（发给调查员）→ 已确认（领用回执已记）。
    """
    sample_id: str
    investigation_id: str
    plan_id: str
    config_version: int
    merge_group: str
    record: EventRecord
    state: str = "已入库"
    delivered_to: str = ""

    @property
    def identity(self) -> tuple[str, str, str, str]:
        """同一代表的幂等键：同调查+计划+归并组+记录。"""
        return (self.investigation_id, self.plan_id, self.merge_group,
                self.record.record_id)

    @staticmethod
    def build_id(investigation_id: str, plan_id: str,
                 merge_group: str, record_id: str) -> str:
        digest = hashlib.sha256(
            f"{investigation_id}|{plan_id}|{merge_group}|{record_id}".encode()
        ).hexdigest()[:12]
        return f"ISAM-{digest}"


@dataclass
class ClaimReceipt:
    """领用回执——额度端登记样本已被调查员领走的凭证。

    state：已记录（正常登记）/ 补记（崩溃后重开调查时核对补登）。
    """
    receipt_id: str
    investigation_id: str
    plan_id: str
    sample_id: str
    merge_group: str
    config_version: int
    quota_consumed: int = 1
    state: str = "已记录"
