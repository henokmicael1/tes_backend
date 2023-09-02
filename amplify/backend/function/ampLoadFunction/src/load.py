from typing import Optional, List
from pydantic import BaseModel
import datetime


class Address(BaseModel):
    stopNumber: int
    stopDate: datetime.date
    stopTime: datetime.time
    address1: str
    address2: Optional[str]
    city: str
    state: str
    country: str
    zipCode: int
    locationType: str  # Pick-Up or Drop-Off location



class Load(BaseModel):
    loadId: Optional[int]
    loadNumber: str
    estDriveTime: str
    rate: float
    loadMiles: str
    address: List[Address]
    rcPath: str
    lumper: float
    detention: float
    tonu: float
    bolPath: str
    truckFk: Optional[int]
    driver1Fk: Optional[int]
    driver2Fk: Optional[int]
    trailerFk: Optional[int]
    dispatcherFk: int
    brokerName: str
    status: str
    sub: str


# class Expense(BaseModel):
#     expenseId: Optional[int]
#     driverFk: Optional[int]
#     expensetype: str
#     cost: str
#     status:Optional[str]