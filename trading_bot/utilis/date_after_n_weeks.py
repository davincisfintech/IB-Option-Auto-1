from datetime import datetime, timedelta

from trading_bot.settings import logger


def date_after_n_weeks(n):
    try:
        if not isinstance(n, int) or n < 0:
            logger.debug(f'Error: Wks values shuld be Positive Integer')
            return None

        today = datetime.today().strftime("%Y%m%d")
        date = datetime.strptime(today, '%Y%m%d').date()
        day_after_n_days = date + timedelta(days=n * 7)
        day_after_n_days = day_after_n_days.strftime('%Y%m%d')
        return day_after_n_days
    except Exception as e:
        pass
        return None
