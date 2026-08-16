"""统一交易成本模型（设计文档 §51）。

Backtest / Paper / Live 必须共享同一 ``TransactionCostModel``，
避免三套链路各自维护成本语义。
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class TransactionCostModel(BaseModel):
    """A 股成本模型：佣金（含最低佣金）+ 印花税 + 过户费 + 滑点。"""

    commission_rate: float = Field(default=0.0003, ge=0)
    min_commission: float = Field(default=5.0, ge=0)
    stamp_duty_rate: float = Field(default=0.001, ge=0)
    transfer_fee_rate: float = Field(default=0.0, ge=0)
    slippage_model: str = "fixed_bps"
    slippage_bps: float = Field(default=10.0, ge=0)

    def apply_slippage(self, price: float, side: str) -> float:
        """买入向上滑点、卖出向下滑点。"""
        shift = price * self.slippage_bps / 10_000.0
        return price + shift if side == "buy" else max(price - shift, 0.0)

    def buy_cost(self, price: float, qty: int) -> dict[str, float]:
        amount = price * qty
        commission = max(amount * self.commission_rate, self.min_commission if amount > 0 else 0.0)
        transfer_fee = amount * self.transfer_fee_rate
        return {"commission": round(commission, 2), "stamp_duty": 0.0, "transfer_fee": round(transfer_fee, 2)}

    def sell_cost(self, price: float, qty: int) -> dict[str, float]:
        amount = price * qty
        commission = max(amount * self.commission_rate, self.min_commission if amount > 0 else 0.0)
        stamp_duty = amount * self.stamp_duty_rate
        transfer_fee = amount * self.transfer_fee_rate
        return {
            "commission": round(commission, 2),
            "stamp_duty": round(stamp_duty, 2),
            "transfer_fee": round(transfer_fee, 2),
        }

    def total(self, cost: dict[str, float]) -> float:
        return round(sum(cost.values()), 2)


__all__ = ["TransactionCostModel"]
