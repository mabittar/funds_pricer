import datetime
from decimal import Decimal
from typing import Optional, List

from pydantic import BaseModel


class RequestQuery(BaseModel):
    document: str
    from_date: Optional[str]
    # TODO: validate MM/YYYY


class TimeSeriesModel(BaseModel):
    timestamp: datetime.datetime
    value: Decimal
    owners: Optional[int]
    net_worth: Optional[float]


class ResponseQuery(RequestQuery):
    document: str
    quotes: Optional[Decimal]
    fund_name: Optional[str]
    fund_id: Optional[str]
    fund_released_on: Optional[datetime.date]
    from_date: Optional[datetime.date]
    active: Optional[bool]
    timeseries: Optional[List[TimeSeriesModel]]


class FundModelLabel(BaseModel):
    document: str
    fund_id: Optional[str]
    fund_released_on: Optional[datetime.date]
    first_date: datetime.date
    active: Optional[bool]
    first_query_date: datetime.date
    last_query_date: datetime.date
    active: bool


class StreamingSchema(BaseModel):
    document: str
    fund_id: Optional[str]
    timeseries: Optional[List[TimeSeriesModel]]
