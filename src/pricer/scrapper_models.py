from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List


@dataclass(order=True)
class TimeSeries:
    sort_index: datetime = field(init=False, repr=False)
    timestamp: datetime
    value: Decimal
    owners: [int]
    net_worth_str: str = field(repr=False)
    net_worth: float = field(init=False)

    def __post_init__(self):
        str_number = self.net_worth_str.replace('.', '').replace(',', '.')
        parsed_num = float(str_number)
        self.net_worth = parsed_num
        self.sort_index = self.timestamp


@dataclass
class FundTS:
    document: Optional[str] = None
    fund_pk: Optional[str] = None
    active: Optional[bool] = False
    fund_name: Optional[str] = None
    released_on: Optional[date] = None
    first_query_date: Optional[date] = None
    last_query_date: Optional[date] = None
    timeseries: Optional[List[TimeSeries]] = None
