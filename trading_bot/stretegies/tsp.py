from trading_bot.settings import logger
from trading_bot.utilis.needed_expiry import needed_expiry


class TSP:
    def __init__(self, client, local_symbol, unique_id_1, unique_id_2, WKS, F, NR, cycle, sec_type, curr):
        self.client = client
        self.local_symbol = local_symbol
        self.WKS = WKS
        self.F = F
        self.NR = NR
        self.cycle = cycle
        self.cf = False
        self.contract_id = unique_id_1
        self.id_2 = unique_id_2
        self.contract_fetched = False
        self.ticker, self.sec_type, self.curr, self.exch = self.local_symbol.split()

    def run(self):
        try:
            if not len(self.client.data_frames[self.id_2]):
                return
            if self.ticker not in self.client.secContract_details_end:
                return
            self.contract_fetched = True
            df = self.client.data_frames[self.id_2]
            PH = max(df['high'])
            PL = min(df['low'])
            PC = df['close'][len(df) - 1]
            SD = self.F * 20 * (PH - PL) / (PH + PL)
            TSC = PC + SD * PC / 100
            TSP = PC - SD * PC / 100
            expiries = sorted(self.client.contract_chain[self.ticker].keys())
            expiry_found = needed_expiry(self.WKS, expiries)
            strikes = sorted(self.client.contract_chain[self.ticker][expiry_found])
            found_strike = None
            minn = float('inf')
            TSP = float(TSP)
            TSC = float(TSC)
            if self.cycle == 'P':
                tsp_or_tsc = TSP
            else:
                tsp_or_tsc = TSC

            found_strike_index = None
            for i, strike in enumerate(sorted(strikes)):
                strike = float(strike)
                if abs(strike - tsp_or_tsc) < minn:
                    found_strike = strike
                    found_strike_index = i
                    minn = abs(strike - tsp_or_tsc)

            logger.debug(f"{self.local_symbol}: PH {PH}, PL {PL}, PC {PC}, SD {SD}, 'TSC {TSC}, 'TSP {TSP}")
            logger.debug(f'{self.local_symbol}: Found expiry {self.ticker} {expiry_found}, WKS {self.WKS}')
            contract = self.client.make_contract(symbol=self.ticker, sec_type='OPT', exch=self.exch, curr=self.curr,
                                                 opt_type=self.cycle, expiry_date=str(expiry_found),
                                                 strike=float(found_strike))
            for strike in sorted(strikes)[found_strike_index:]:
                contract = self.client.make_contract(symbol=self.ticker, sec_type='OPT', exch=self.exch, curr=self.curr,
                                                     opt_type=self.cycle, expiry_date=str(expiry_found),
                                                     strike=float(strike))
                if self.client.validate_opt_contract(contract):
                    break

            return contract

        except Exception as e:
            self.contract_fetched = True
            logger.exception(e)
            return 'error'
