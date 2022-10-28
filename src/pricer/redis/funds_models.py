import datetime
from decimal import Decimal
from typing import Optional, List

from aredis_om import HashModel, JsonModel, EmbeddedJsonModel
from pydantic import Field, BaseModel


class FundValueTS(BaseModel):
    timestamp: datetime.datetime
    value: Decimal


class FundNetWorthTS(BaseModel):
    timestamp: datetime.datetime
    value: Decimal


class FundOwnerTS(BaseModel):
    timestamp: datetime.datetime
    value: Decimal


class FundModel(BaseModel):
    document: str = Field(index=True)
    name: str
    fund_id: str
    first_date: datetime.date
    first_query_date: datetime.date
    last_query_date: datetime.date
    active: bool
    value: Optional[FundOwnerTS] = None
    net_worth: Optional[FundNetWorthTS] = None
    owners: Optional[FundValueTS] = None
