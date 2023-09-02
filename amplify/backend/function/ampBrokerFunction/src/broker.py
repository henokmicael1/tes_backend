from typing import Optional
from pydantic import BaseModel


class Broker(BaseModel):
    brokerId: Optional[int]
    name: str
    phoneNumber: str
    email: str
    address1: str
    address2: str
    state: str
    city: str
    country: str
    zipCode: str
    status: Optional[str]
    sub: str
