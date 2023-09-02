from typing import Optional, List
import datetime
from pydantic import BaseModel


class Document(BaseModel):
    url:str
    documentType:str

class Truck(BaseModel):
    truckId: Optional[int]
    truckRegExp: datetime.date
    licensePlateNumber: str
    fedAnnualInspectionExp: datetime.date
    stateAnnualInspectionExp: datetime.date
    driver1Fk: Optional[int]
    driver2Fk: Optional[int]
    trailerFk: Optional[int]
    vehicleFk: int
    status: Optional[str]
    sub: str
    solo: Optional[bool]
    insureStartDt: datetime.date
    insureEndDt: datetime.date
    documents:Optional[List[Document]]
    vin: str

