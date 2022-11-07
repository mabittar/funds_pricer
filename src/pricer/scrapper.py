import logging
import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from time import sleep
from itertools import chain
from typing import List, Optional, Tuple, Union

from fastapi import HTTPException
from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore

from src.pricer.settings import Settings

settings = Settings()


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
  

class Scrapper:
    def __init__(self, logger=None):
        self.fund_ts_model = FundTS()
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_prefs = {}
        chrome_options.experimental_options["prefs"] = chrome_prefs
        chrome_prefs["profile.default_content_settings"] = {"images": 2}  # disable image
        self.chrome_options = chrome_options
        
    def get_fund_pk(self, url_str: str) -> str:
        find_str = "?PK_PARTIC="
        pk_position = url_str.find(find_str)
        url_str = url_str[pk_position + len(find_str):].split("&")[0]
        self.logger.debug("Fund unique id: %s" % url_str)
        return url_str
    
    @staticmethod
    def str_2_date(str_date) -> date:
        return datetime.strptime(f'01/{str_date}', "%d/%m/%Y").date()

    @staticmethod
    def filter_limit_date(available_date_list: list, from_date: Union[date, None] = None,
                          end_date: Union[date, None] = None) -> List[Tuple]:
        available_date_datetime = [(index, date_str) for index, date_str in enumerate(available_date_list)]
        if from_date is not None:
            available_date_datetime = [(index, date_str) for index, date_str in available_date_datetime if
                                       datetime.strptime(f'01/{date_str}', "%d/%m/%Y").date() >= from_date]
        if end_date is not None:
            available_date_datetime = [(index, date_str) for index, date_str in available_date_datetime if
                                       datetime.strptime(f'01/{date_str}', "%d/%m/%Y").date() <= end_date]
        return available_date_datetime

    async def parse_data(self, wd, i, month_year):
        i += 1
        wd.find_element(By.XPATH, f"//*[@id='ddComptc']/option[{i}]").click()
        # this will click the option which index is defined by position
        sleep(3)
        WebDriverWait(wd, 20).until(EC.presence_of_element_located((By.XPATH, '//*[@id="dgDocDiario"]')))
        rows = wd.find_elements(By.XPATH, '//*[@id="dgDocDiario"]/tbody/tr')
        timeseries_list = []
        for row in rows[1:]:  # linha 0 header da tabela
            daily_data = [field.replace(" ", "") for field in row.text.split(" ")]
            value = daily_data[1]
            date_field = daily_data[0]
            if value != "":
                try:
                    net_worth = daily_data[4]
                    owner_number = daily_data[6]
                    datetime_stamp = datetime.strptime(f'{date_field}/{month_year}', "%d/%m/%Y")
                    timeseries = TimeSeries(
                        timestamp=datetime_stamp,
                        value=Decimal(value.replace(",", ".")),
                        net_worth_str=net_worth,
                        owners=int(owner_number)
                    )
                    timeseries_list.append(timeseries)
                except ValueError as e:
                    self.logger.error(e)
                    self.logger.error(date_field, month_year)
        return timeseries_list

    async def parse_table(
            self,
            wd: Chrome,
            fund_daily_link: str,
            from_date: Union[date, None] = None,
            end_date: Union[date, None] = None
    ) -> List[TimeSeries]:
        wd.get(fund_daily_link)
        # table = wd.find_element(By.ID, 'TABLE1')
        selectors = wd.find_element(By.XPATH, '//*[@id="ddComptc"]')
        selectors_list = selectors.text.split("\n")
        selectors_list = [i.replace(' ', "") for i in selectors_list[:-1]]
        selectors_filtered = self.filter_limit_date(selectors_list, from_date, end_date)
        select = wd.find_element(By.XPATH, '//*[@id="ddComptc"]')
        wd.execute_script(
            "showDropdown = function (element) {var event; event = document.createEvent('MouseEvents'); event.initMouseEvent('mousedown', true, true, window); element.dispatchEvent(event); }; showDropdown(arguments[0]);",
            select)
        # open dropdown options
        parsed_ts = []
        self.logger.debug(f"Found {len(selectors_filtered)} months to scrap")
        # TODO: Make multiple threads
        async with asyncio.Semaphore(20):
            for i, month_year in selectors_filtered:
                task = asyncio.ensure_future(self.parse_data(wd, i, month_year))
                parsed_ts.append(task)
            await asyncio.gather(*parsed_ts, return_exceptions=True)
        time_series_result = [ts.result() for ts in parsed_ts]
        timeseries = list(chain.from_iterable(time_series_result))

        return timeseries

    async def get_funds_details(
            self,
            document_number: str,
            wd: Chrome
    ) -> str:
        wd.get(settings.cvm_url + document_number)  # url = CVM + CNPJ
        WebDriverWait(wd, 20).until(EC.element_to_be_clickable((By.CLASS_NAME, 'HRefPreto')))
        wd.execute_script("__doPostBack('ddlFundos$_ctl0$lnkbtn1','')")
        self.logger.debug("Success executed javascript!")
        pk_url = wd.current_url
        self.fund_ts_model.fund_pk = self.get_fund_pk(pk_url)
        fund_name_elem = wd.find_element(By.XPATH, '//*[@id="lbNmDenomSocial"]')
        self.fund_ts_model.fund_name = fund_name_elem.text
        start_at = wd.find_element(By.XPATH, '//*[@id="lbInfAdc1"]')
        self.fund_ts_model.released_on = datetime.strptime(start_at.text, '%d/%m/%Y').date()
        status_elem = wd.find_element(By.XPATH, '//*[@id="lbSitDesc"]')
        self.fund_ts_model.active = True if status_elem.text == "EM FUNCIONAMENTO NORMAL" else False
        # Lâmina de cotas diárias
        fund_daily = wd.find_element(By.XPATH, '//*[@id="Hyperlink2"]')
        fund_daily_link = fund_daily.get_attribute('href')

        return fund_daily_link

    async def update_fund_data(self, fund_id: str, from_date: str) -> list[TimeSeries]:
        timeseries = None
        from_date = datetime.fromisoformat(from_date) if isinstance(from_date, str) else None
        with webdriver.Chrome('chromedriver', options=self.chrome_options) as wd:
            time_series: list = await self.parse_table(
                wd=wd,
                from_date=from_date,
                fund_daily_link=settings.cvm_fund_url % fund_id
            )
            if time_series is not None:
                timeseries = sorted(time_series)

        return timeseries

    async def get_fund_data(
            self,
            document_number: Union[str, None] = None,
            fund_pk: Union[str, None] = None,
            from_date: Union[date, str, None] = None,
            end_date: Union[date, str, None] = None
    ) -> FundTS:
        if document_number is None and fund_pk is None:
            raise HTTPException(
                status_code=400,
                detail="Document Number or Fund Pk must be used to parse a fund detail"
                )
        if from_date is not None:
            from_date = from_date if hasattr(from_date, "strftime") else self.str_2_date(from_date)
        if end_date is not None:
            end_date = end_date if hasattr(end_date, "strftime") else self.str_2_date(end_date)
        # Detalhes do fundo
        # TODO: validar CNPJ
        with webdriver.Chrome('chromedriver', options=self.chrome_options) as wd:
            self.logger.debug("Success stated webdriver")
            if document_number:
                self.fund_ts_model.document = document_number
                fund_daily_link = await self.get_funds_details(document_number, wd)
            else:
                fund_daily_link = settings.cvm_fund_url % fund_pk
            # parser da tabela
            time_series: list = await self.parse_table(wd, fund_daily_link, from_date, end_date)  # type: ignore

            self.fund_ts_model.timeseries = sorted(time_series) if time_series is not None else None

        return self.fund_ts_model


if __name__ == "__main__":
    import asyncio
    from time import perf_counter

    async def get_data():
        scrapper = Scrapper()
        response = await asyncio.gather(scrapper.get_fund_data(document_number="18993924000100", from_date="06/2022"))
        return response

    s = perf_counter()
    result = asyncio.run(get_data())
    elapsed = perf_counter() - s
    print(f"Script executed in {elapsed:0.2f} seconds.")
    print(result)
