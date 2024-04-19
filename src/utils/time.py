from datetime import datetime
from dateutil.relativedelta import relativedelta

separator = {
    "month": lambda start_time, end_time: (end_time.year - start_time.year) * 12 + (end_time.month - start_time.month),
    "week": lambda start_time, end_time: (end_time - start_time).days // 7,
    "day": lambda start_time, end_time: (end_time - start_time).days,
    "hour": lambda start_time, end_time: (end_time - start_time).days * 24,
}

add_period = {
    "month": relativedelta(months=+1),
    "week": relativedelta(days=+7),
    "day": relativedelta(days=+1),
    "hour": relativedelta(hours=+1),
}


def divide_time_period(start_time: datetime, end_time: datetime, group_type: str) -> list[datetime]:
    amount_of_periods = separator[group_type](start_time, end_time)
    labels = [start_time]
    for month in range(amount_of_periods):
        start_time = start_time + add_period[group_type]
        labels.append(start_time)
    return labels
