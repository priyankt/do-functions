import typing
import datetime
import pydantic

import httpx
import sqlalchemy
import sqlalchemy.orm

Base: typing.Any = sqlalchemy.orm.declarative_base()


class Holiday(pydantic.BaseModel):
    date: datetime.date
    title: str

    @staticmethod
    def from_row(row: typing.Dict[str, typing.Any]) -> "Holiday":
        """Create Holiday from Dict

        Args:
            Dict[str, Any]
        Returns:
            Holiday object
        Raises:
            None
        """
        date_str: str = typing.cast(str, row.get("tradingDate", ""))
        title: str = typing.cast(str, row.get("description", ""))
        holiday_date: datetime.date = datetime.datetime.strptime(
            date_str, "%d-%b-%Y"
        ).date()
        return Holiday(
            date=holiday_date,
            title=title,
        )


class HolidayFetcher(typing.Protocol):
    def fetch_holidays(self) -> typing.List[Holiday]:
        ...


class NSEHolidayFetcher:
    def fetch_holidays(self) -> typing.List[Holiday]:
        """Fetche holidays from the NSE India API

        Args:
            None
        Returns:
            List of Holiday objects
        Raises:
            None
        """
        holidays_list: typing.List[Holiday] = []
        res: httpx.Response = httpx.Client(http2=True).get(
            "https://www.nseindia.com/api/holiday-master?type=trading",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            },
        )
        if res.status_code == httpx.codes.OK:
            data: typing.Dict[str, typing.Any] = res.json()
            holiday_dict_list: typing.List[typing.Dict[str, str | int]] = data.get(
                "FO", []
            )
            holidays_list = list(
                map(
                    lambda row: Holiday.from_row(row),
                    holiday_dict_list,
                )
            )

        return holidays_list


class DBHoliday(Base):
    __tablename__ = "holidays"

    id = sqlalchemy.Column(sqlalchemy.Integer(), primary_key=True)
    date = sqlalchemy.Column(sqlalchemy.Date, nullable=False, unique=True, index=True)
    title = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)


class HolidayStore(typing.Protocol):
    def store_holidays(self, holidays: typing.List[Holiday]):
        ...

    def query_existing_holiday_dates(self) -> typing.List[str]:
        ...


class PGHolidayStore:
    def __init__(self, db_url: str):
        engine: sqlalchemy.Engine = sqlalchemy.create_engine(url=db_url)
        SessionLocal = sqlalchemy.orm.sessionmaker(
            autocommit=False, autoflush=False, bind=engine
        )
        self.db: sqlalchemy.orm.Session = SessionLocal()
        self.date_format: str = "%Y-%m-%d"
        self.existing_holidays: typing.List[str] = self.query_existing_holiday_dates()

    def exists_holiday_date(self, for_date: datetime.date) -> bool:
        """check if holiday date exists in store

        Args:
            for_date: date to check
        Returns:
            True if holiday date already exists else False
        Raises:
            None
        """
        for_date_str: str = for_date.strftime(self.date_format)
        if not self.existing_holidays:
            self.existing_holidays = self.query_existing_holiday_dates()
        return for_date_str in self.existing_holidays

    def store_holidays(self, holidays: typing.List[Holiday]):
        """store holidays in database

        Args:
            holidays: list of holidays
        Returns:
            None
        Raises:
            None
        """
        for holiday in holidays:
            if not self.exists_holiday_date(for_date=holiday.date):
                self.db.add(
                    DBHoliday(date=holiday.date, title=holiday.title.replace("\r", ""))
                )
        self.db.commit()

    def query_existing_holiday_dates(self) -> typing.List[str]:
        """fetch holidays from store

        Args:
            None
        Returns:
            List of string dates
        Raises:
            None
        """
        existing_holiday_tuples: typing.List[typing.Tuple[datetime.date, None]] = (
            self.db.query(DBHoliday)
            .with_entities(DBHoliday.date)
            # .filter(DBHoliday.date >= datetime.date.today())
            .all()
        )

        return list(
            map(lambda x: x[0].strftime(self.date_format), existing_holiday_tuples)
        )
