from typing import Optional
import datetime
from pydantic import BaseModel


class Trailer(BaseModel):
    trailerId: Optional[int]
    trailerRegExp: datetime.date
    licensePlateNumber: str
    fedAnnualInspectionExp: datetime.date
    stateAnnualInspectionExp: datetime.date
    vehicleFk: int
    status: Optional[str]
    sub: str
    trailerType: Optional[str]
    insureStartDt: datetime.date
    insureEndDt: datetime.date
    vin: str
