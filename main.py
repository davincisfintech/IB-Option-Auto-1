#  Parameters start  #

account_mode = 'Paper'  # Choices Live, Paper. Live if Live account is open and Paper if paper account is open in IB TWS

# List of symbols in this format: SYMBOL SECURITY_TYPE CURRENCY EXCHANGE
symbols = {
    'SPY STK USD SMART': {'F': 1, 'WKS': 0, 'NR': 1, },
    'MSFT STK USD SMART': {'F': 1, 'WKS': 0, 'NR': 2},
    'PYPL STK USD SMART': {'F': 1, 'WKS': 0, 'NR': 1},
    'ADBE STK USD SMART': {'F': 1, 'WKS': 0, 'NR': 1},
    'TSLA STK USD SMART': {'F': 1, 'WKS': 0, 'NR': 1},
}

cycle = 'P'

if __name__ == '__main__':
    from trading_bot.controller import run

    run(cycle=cycle, symbols=symbols, account_mode=account_mode)
