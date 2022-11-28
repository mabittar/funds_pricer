import asyncio
import dataclasses
import functools
import json
import signal
from dataclasses import dataclass, field
from datetime import datetime, date

from decimal import Decimal
from uuid import uuid4
import redis
from redis.exceptions import ResponseError, TimeoutError, ConnectionError
from queue_settings import Settings, logger
import threading
from typing import List, Optional, Tuple, Union

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore


settings = Settings()
thread_local = threading.local()


@dataclass
class PubSubMsg:
    document: str
    fund_pk: str
    month_year: str
    message_id: str = field(default=None)
    saved: bool = field(default=False)
    acked: bool = field(default=False)


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


class InvalidDateTime(Exception):
    pass


class StreamError(Exception):
    pass


class DataParser:
    def __init__(self):
        self.logger = logger
        self.fund_ts_model = FundTS()
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_prefs = dict()
        chrome_options.experimental_options["prefs"] = chrome_prefs
        chrome_prefs["profile.default_content_settings"] = {"images": 2}  # disable image
        self.chrome_options = chrome_options

    @staticmethod
    def select_parse_date(available_date_list: list, parse_date: str) -> Tuple:
        available_date_datetime = [(index, date_str) for index, date_str in enumerate(available_date_list)]
        parse_date_start = datetime.strptime(f'01/{parse_date}', "%d/%m/%Y").date()
        date2parse = next(
            filter(
                lambda x: x[1] == parse_date, available_date_datetime
            )
        )
        # parse_date_end = parse_date_start + relativedelta(months=1)
        # available_date_datetime = [
        #     (index, date_str) for index, date_str in available_date_datetime
        #     if (parse_date_start <= datetime.strptime(f'01/{date_str}', "%d/%m/%Y").date() < parse_date_end)
        # ]
        if not date2parse:
            # TODO: remove msg from queue
            raise InvalidDateTime(
                f"Could not find date:{parse_date} to parse.")
        return date2parse

    async def parse_data(self, wd, i=None, month_year=None):
        if isinstance(wd, tuple):
            i = wd[1]
            month_year = wd[2]
            wd = wd[0]
        self.logger.debug(f"Parsing {month_year}")
        i += 1
        wd.find_element(By.XPATH, f"//*[@id='ddComptc']/option[{i}]").click()
        # this will click the option which index is defined by position
        # await asyncio.sleep(3)
        WebDriverWait(wd, 20).until(EC.presence_of_element_located((By.XPATH, '//*[@id="dgDocDiario"]')))
        rows = wd.find_elements(By.XPATH, '//*[@id="dgDocDiario"]/tbody/tr')
        timeseries_list = await self.read_rows(month_year, rows)
        return timeseries_list

    async def read_rows(self, month_year, rows):
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

    async def parser_fund(
            self,
            msg: PubSubMsg
    ) -> List[TimeSeries]:
        fund_daily_link = settings.cvm_fund_url % msg.fund_pk
        parse_date = msg.month_year
        with webdriver.Chrome('chromedriver', options=self.chrome_options) as wd:
            self.logger.debug("Success started webdriver")
            wd.get(fund_daily_link)
            selectors = wd.find_element(By.XPATH, '//*[@id="ddComptc"]')
            selectors_list = selectors.text.split("\n")
            selectors_list = [i.replace(' ', "") for i in selectors_list[:-1]]
            selectors_filtered: Tuple = self.select_parse_date(selectors_list, parse_date)
            select = wd.find_element(By.XPATH, '//*[@id="ddComptc"]')
            wd.execute_script(
                "showDropdown = function (element) {var event; event = document.createEvent('MouseEvents'); event.initMouseEvent('mousedown', true, true, window); element.dispatchEvent(event); }; showDropdown(arguments[0]);",
                select)
            self.logger.debug(f"Starting parser for {parse_date}.")
            parsed_ts = await self.parse_data(wd, selectors_filtered[0], selectors_filtered[1])
            if parsed_ts is not None:
                parsed_ts = sorted(parsed_ts)
                self.fund_ts_model.timeseries = parsed_ts

        return self.fund_ts_model


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (datetime, date)):
            return o.isoformat()

        return super().default(o)


class QueueConnector:
    def __init__(self):
        self.logger = logger
        self.channel = settings.channel
        self.redis = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            decode_responses=True,
            encoding="utf-8"
        )

    async def add_many_to_timeseries(
            self, key_value_pairs: Tuple, data: list[TimeSeries]
    ):
        """
        Add many samples to several timeseries keys.
        `key_pairs` is an iterable of tuples containing in the 0th position the
        timestamp key into which to insert entries and the 1th position the name
        of the key within th `data` dict to find the sample.
        """
        partial = functools.partial(self.redis.execute_command, 'TS.MADD')
        for entry in data:
            for timeseries_key, attr in key_value_pairs:
                point = entry.__getattribute__(attr)
                v = str(point.quantize(Decimal("1.000000000"))) if isinstance(point, Decimal) else point
                partial = functools.partial(
                    partial,
                    timeseries_key,
                    int(entry.timestamp.timestamp()),
                    v
                )
        return partial()

    async def create_ts(self, key):
        try:
            await self.redis.execute_command(
                'TS.CREATE', key,
                'DUPLICATE_POLICY', 'first',
            )
        except ResponseError as e:
            # Time series probably already exists
            logger.info('Could not create timeseries %s, error: %s', key, e)

    async def check_ts_by_key(self, key: str):
        cached = await self.redis.get(key)
        if cached:
            return True
        else:
            return False

    async def create_ts_key(self, key_list):
        for key in key_list:
            existing = await self.check_ts_by_key(key)
            if not existing:
                self.logger.info("Time series not found, will be created!")
                await self.create_ts(key)

    async def stream_data(self, data: FundTS, msg: PubSubMsg):
        self.logger.debug("Message processed, streaming parsed data")
        doc_number = msg.document
        value_key = f"PRICER_value_{doc_number}"
        owner_key = f"PRICER_owners_{doc_number}"
        networth_key = f"PRICER_networth_{doc_number}"
        await self.create_ts_key([value_key, owner_key, networth_key])
        try:
            await self.add_many_to_timeseries(
                (
                    (value_key, "value"),
                    (owner_key, "owners"),
                    (networth_key, "net_worth"),
                ),
                data.timeseries
            )
        except ResponseError as e:
            logger.error('Error while streaming data series for document: %s, month: %s', (doc_number, msg.month_year))

        # msg.acked = True -> Pure redis doesn't acked msg, only listen
        self.logger.info(f"Data parsed and streamed. Acked {msg.message_id}")

    async def handle_message(self, msg):
        """Kick off tasks for a given message.
        Args:
            msg (PubSubMessage): consumed message to process.
        """
        self.logger.debug("Creating event and start table parser.")
        parser = DataParser()
        data = await asyncio.create_task(parser.parser_fund(msg))
        if data:
            await asyncio.create_task(self.stream_data(data, msg))

    async def consume(self):
        """Consumer client to subscribing to redis publisher.
        """
        while True:
            ps = self.redis.pubsub()
            ps.subscribe(self.channel)
            for raw_message in ps.listen():
                if raw_message["type"] != "message":
                    continue
                message_dict = json.loads(raw_message["data"])
                msg = PubSubMsg(**message_dict)
                msg.message_id = str(uuid4()) if msg.message_id is None else msg.message_id
                if msg.acked:
                    continue
                self.logger.info(f"Consumed {msg.message_id}")
                await asyncio.create_task(self.handle_message(msg), name=msg.message_id)

    @staticmethod
    async def shutdown(loop, signal=None):
        """Cleanup tasks tied to the service's shutdown."""
        if signal:
            logger.info(f"Received exit signal {signal.name}...")
        logger.info("Nacking outstanding messages")
        tasks = [t for t in asyncio.all_tasks() if t is not
                 asyncio.current_task()]

        [task.cancel() for task in tasks]

        logger.info(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"Flushing metrics")
        loop.stop()

    def handle_exception(self, loop, context):
        # context["message"] will always be there; but context["exception"] may not
        msg = context.get("exception", context["message"])
        logger.error(f"Caught exception: {msg}")
        logger.info("Shutting down...")
        asyncio.create_task(self.shutdown(loop))


def main():
    loop = asyncio.get_event_loop()
    queue_connector = QueueConnector()
    # May want to catch other signals too
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s: asyncio.create_task(queue_connector.shutdown(loop, signal=s)))
    # comment out the line below to see how unhandled exceptions behave
    loop.set_exception_handler(queue_connector.handle_exception)

    try:
        loop.create_task(queue_connector.consume())
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Process interrupted")
    finally:
        loop.close()
        logger.info("Successfully shutdown the queue service.")


if __name__ == "__main__":
    main()
