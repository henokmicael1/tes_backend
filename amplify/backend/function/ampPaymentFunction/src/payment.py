from typing import Optional, List
import datetime
from pydantic import BaseModel


class Payment(BaseModel):
    paymentId: Optional[int]
    loadId: List[int]
    grossAmount: float
    incomeAdjustment: float
    adjustedGrossAmount: float
    grossPayable: float
    lessInsurance: float
    lessTransaction: float
    incomeOverExpenses: float
    driverShare: float
    addDriverRefunds: float
    lessDriverAdvances: float
    driverNetSettlement: float
    status: Optional[str]
    sub: str
    paymentUrl: str
    ts_created: datetime.datetime
    ts_updated: datetime.datetime