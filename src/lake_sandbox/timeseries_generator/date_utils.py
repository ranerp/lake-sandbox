import itertools
from datetime import datetime, timedelta


def parse_date_range(
    start_date: str, end_date: str, interval_days: int = 7
) -> list[datetime]:
    """Parse date range and generate list of dates at specified intervals.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        interval_days: Days between each date (default 7 for weekly)

    Returns:
        List of datetime objects
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    current_date = start_dt
    while current_date <= end_dt:
        dates.append(current_date)
        current_date += timedelta(days=interval_days)

    return dates


def group_dates_by_year(dates: list[datetime]) -> dict[int, list[datetime]]:
    """Group dates by year

    Args:
        dates: List of datetime objects (should be sorted by date)

    Returns:
        Dictionary mapping year to list of dates in that year
    """
    return {
        year: list(group)
        for year, group in itertools.groupby(dates, key=lambda d: d.year)
    }
