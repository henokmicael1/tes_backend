# from pydantic import BaseModel, Field, Optional
#
#
# class AdminBaseModel(BaseModel):
#     admin_id: Optional[str] = Field(None, max_length=100)
#     first_name: str = Field(min_length=1, max_length=100)
#     last_name: str = Field(min_length=1, max_length=100)
#     phone_number: str = Field(min_length=10, max_length=15)
#     email: str = Field(min_length=5, max_length=100, regex=r'^[\w\.-]+@[\w\.-]+\.\w+$')
#     address1: str = Field(min_length=1, max_length=200)
#     address2: str = Field(None, max_length=200)
#     state: str = Field(..., min_length=1, max_length=100)
#     city: str = Field(..., min_length=1, max_length=100)
#     country: str = Field(..., min_length=1, max_length=100)
#     zip_code: str = Field(..., min_length=1, max_length=20)
#     status: str = Field(..., min_length=1, max_length=100)
#     sub: str = Field(..., min_length=1, max_length=100)
