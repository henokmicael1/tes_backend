from typing import Optional
import datetime
from pydantic import BaseModel


class Driver(BaseModel):
    driverId: Optional[int]
    firstName: str
    middleName: Optional[str]
    lastName: str
    phoneNumber: str
    email: str
    birthDate: str
    cdlLicenseNumber: str
    cdlLicenseExpDate: datetime.date
    cdlState: str
    einNumber: str
    medCardExp: datetime.date
    mvrExp: datetime.date
    drugClearExp: datetime.date
    address1: str
    address2: str
    state: str
    city: str
    country: str
    zipCode: str
    status: Optional[str]
    sub: str
