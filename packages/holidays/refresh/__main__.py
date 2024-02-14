import os
import typing

import schemas
# import logging


# logging.basicConfig(
#     format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     level=logging.DEBUG,
# )


def main(args: typing.Dict[str, typing.Any] = {}):
    holiday_store: schemas.HolidayStore = schemas.PGHolidayStore(
        db_url=os.getenv("DATABASE_URL", "")
    )
    holiday_fetcher: schemas.HolidayFetcher = schemas.NSEHolidayFetcher()
    refresh_holidays(fetcher=holiday_fetcher, store=holiday_store)

    return {"success": True}


def refresh_holidays(fetcher: schemas.HolidayFetcher, store: schemas.HolidayStore):
    """fetch holidays from api and store

    Args:
        fetcher: holiday fetcher
        store: holiday store
    Returns:
        None
    Raises:
        None
    """
    holidays: typing.List[schemas.Holiday] = fetcher.fetch_holidays()
    store.store_holidays(holidays)


if __name__ == "__main__":
    main()
