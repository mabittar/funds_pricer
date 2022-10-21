import datetime
from decimal import Decimal
from typing import Optional

from aredis_om import HashModel, JsonModel
from pydantic import Field


class FundValueTS(HashModel):
    timestamp: datetime.datetime
    value: Decimal


class FundNetWorthTS(HashModel):
    timestamp: datetime.datetime
    value: Decimal


class FundOwnerTS(HashModel):
    timestamp: datetime.datetime
    value: Decimal


class FundModel(JsonModel):
    document: str = Field(index=True)
    name: str
    fund_id: str
    first_date: datetime.date
    first_query_date: datetime.date
    last_query_date: datetime.date
    active: bool
    value: Optional[FundValueTS]
    net_worth: Optional[FundValueTS]
    owners: Optional[FundValueTS]
