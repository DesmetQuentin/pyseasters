import logging
from datetime import datetime, timezone
from typing import Tuple

__all__ = ["localize_time_range"]

log = logging.getLogger(__name__)


def localize_time_range(
    time_range: Tuple[datetime, datetime],
) -> Tuple[datetime, datetime]:
    time_localized = [dt.tzinfo for dt in time_range]
    """Unify or guess the timezones of a tuple of datetime."""
    if not all(time_localized):
        if not any(time_localized):
            log.info("Time range datetimes assumed assumed to be in UTC...")
            time_range = (
                time_range[0].replace(tzinfo=timezone.utc),
                time_range[1].replace(tzinfo=timezone.utc),
            )
        elif not time_localized[0]:
            log.info(
                "Time range start datetime is assumed to be in the same time "
                + "zone as end datetime."
            )
            time_range = (
                time_range[0].replace(tzinfo=time_localized[1]),
                time_range[1],
            )
        else:
            log.info(
                "Time range start datetime is assumed to be in the same time "
                + "zone as start datetime."
            )
            time_range = (
                time_range[0],
                time_range[1].replace(tzinfo=time_localized[0]),
            )
    return time_range
