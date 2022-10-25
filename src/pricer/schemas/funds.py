import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel

from src.pricer.redis.funds_models import FundValueTS, FundNetWorthTS, FundOwnerTS


class RequestQuery(BaseModel):
    document: str
    from_date: Optional[str]
    end_date: Optional[str]


class ResponseQuery(RequestQuery):
    document: str
    quotes: Optional[Decimal]
    fund_name: Optional[str]
    fund_id: Optional[str]
    fund_released_on: Optional[datetime.date]
    from_date: Optional[datetime.date]
    active: Optional[bool]
    value: Optional[FundValueTS]
    net_worth: Optional[FundNetWorthTS]
    owners: Optional[FundOwnerTS]

    class Config:
        json_encoders = {
            datetime: lambda v: v.date(),
            Decimal: lambda v: str(v.quantize(Decimal("1.0000")))
        }
