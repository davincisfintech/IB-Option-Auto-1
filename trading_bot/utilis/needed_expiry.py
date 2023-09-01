from trading_bot.utilis.date_after_n_weeks import date_after_n_weeks
from trading_bot.settings import logger


def needed_expiry(wks, expiries):
    future_day = date_after_n_weeks(wks)
    if future_day is None:
        return None
    min_diff = float('inf')
    result = None
    try:
        for expiry in expiries:
            try:
                expiry_int = int(expiry)
                future_day_int = int(future_day)
                if expiry_int < future_day_int:
                    continue
                if abs(expiry_int - future_day_int) < min_diff:
                    result = expiry
                    min_diff = abs(expiry_int - future_day_int)
            except ValueError:
                continue

        return result
    except ValueError:
        logger.debug(f'Error: Wrong Value of expries or wks/needed_expiry Folder')


# print(needed_expiry(-2,['20230616','20230623','20230630']))


'''


# Test case 1: Valid input
print(date_after_n_days(5))  # Expected output: A date string representing 5 days from today

# Test case 2: Invalid input for date_after_n_days
try:
    print(date_after_n_days(-1))
except ValueError as e:
    print(str(e))  # Expected output: Input 'n' must be a positive integer.

# Test case 3: Invalid input for needed_expiries
days_to_expiry_list = [5, -1, 10, 7]
expiries = ["20230615", "20230610", "20230612", "20230617"]
print(needed_expiries(days_to_expiry_list, expiries))
# Expected output: A sorted list of expiries ["20230615", "20230617"] (ignoring the invalid input -1)

# Test case 4: Invalid expiry format
days_to_expiry_list = [5, 10]
expiries = ["2023-06-15", "20230610", "20230612", "2023-06-17"]
print(needed_expiries(days_to_expiry_list, expiries))
# Expected output: A sorted list of expiries ["20230610", "20230612", "2023-06-15", "2023-06-17"] (ignoring the invalid format)

# Test case 5: No valid expiries found for a future day
days_to_expiry_list = [5]
expiries = ["20230601", "20230602", "20230603"]
print(needed_expiries(days_to_expiry_list, expiries))
# Expected output: A sorted list of expiries [None]


'''
