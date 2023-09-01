import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd

from ibapi.client import ExecutionFilter
from trading_bot.clients.ib import IBapi
from trading_bot.database.db import engine, OptTradesData
from trading_bot.database.db_handler import save_trade
from trading_bot.settings import logger
from trading_bot.stretegies.tsp import TSP
from trading_bot.trade_managers.opt_trade_manager import OptTradeManager


class Controller:
    def __init__(self, client):
        self.client = client
        self.strats = dict()
        self.trade_managers = list()

    @staticmethod
    def run_instance(obj):
        return obj.trade()

    def run(self):
        with ThreadPoolExecutor() as executor:
            res = executor.map(self.run_instance, self.trade_managers)
        res = [r for r in res if r is not None]
        for r in res:
            try:
                if isinstance(r, dict):
                    if r['msg']:
                        r_msg = r['msg']
                        logger.debug(r_msg)
                        model_class = OptTradesData
                        for i in r['msg']:
                            if i:
                                for k, v in i.items():
                                    save_trade(model_class, k, v)
                else:
                    if r.trade_ended:
                        self.trade_managers.remove(r)
                        # del self.strats[self.symbol]
                        logger.debug(f'{r.identifier} instance removed from trading manager')
            except Exception as e:
                logger.debug(f'Controller / run / r / for {r}')
                logger.exception(e)

        tsp_obj_list = [obj for obj in self.strats.values() if not obj.contract_fetched]
        if not len(self.trade_managers) and not len(tsp_obj_list):
            logger.debug('All instances closed, Trading ended')
            return 'trade_ended'


def run(symbols, account_mode, cycle):
    what_type = 'TRADES'  # Type of data required, i.e. TRADES, BID_ASK, BID, ASK, MIDPOINT etc
    client = IBapi()
    socket_port = 7497 if account_mode.lower() == 'paper' else 7496
    client.connect('127.0.0.1', socket_port, 1)
    client_thread = threading.Thread(target=client.run, daemon=True)
    client_thread.start()
    time.sleep(3)
    controller = Controller(client=client)
    client.reqAllOpenOrders()
    client.reqExecutions(10001, ExecutionFilter())
    table_name = 'opt_trades_data'

    today_string = str(datetime.today().strftime('%Y%m%d'))
    '''
    OPEN & NON-EXPIRED
    '''
    open_pos_stock_list = pd.read_sql(table_name, engine)
    mask = (open_pos_stock_list['trading_mode'].str.upper() == account_mode.upper()) & \
           (open_pos_stock_list['expiry_date'].astype(int) >= int(today_string)) & \
           ((open_pos_stock_list['position_status'] == "OPEN") | (open_pos_stock_list['entry_order_status'] == 'OPEN'))
    open_pos_stock_list = open_pos_stock_list[mask]
    open_pos_stock_list = list(open_pos_stock_list.T.to_dict().values())
    open_pos_symbols = {s['symbol']: s for s in open_pos_stock_list}

    '''
    OPEN & EXPIRED
    '''
    open_pos_stock_list_expired = pd.read_sql(table_name, engine)
    mask = (open_pos_stock_list_expired['trading_mode'].str.upper() == account_mode.upper()) & \
           (open_pos_stock_list_expired['expiry_date'].astype(int) < int(today_string)) & \
           ((open_pos_stock_list_expired['position_status'] == "OPEN") | (
                   open_pos_stock_list_expired['entry_order_status'] == 'OPEN'))
    open_pos_stock_list_expired = open_pos_stock_list_expired[mask]
    open_pos_stock_list_expired = list(open_pos_stock_list_expired.T.to_dict().values())
    open_pos_symbols_expired = {s['symbol']: s for s in open_pos_stock_list_expired}

    for i, trade in enumerate(open_pos_stock_list, start=client.nextorderId):
        logger.info(f"Open position/order found in {trade['symbol']} {trade['opt_type']} "
                    f"option, reading parameters...")
        entry_order_filled = False if trade['entry_order_status'] == 'OPEN' else True
        bought = True if trade['side'] == 'LONG' else False
        sold = True if trade['side'] == 'SHORT' else False
        contract = client.make_contract(symbol=trade['symbol'],
                                        sec_type=trade['symbol_type'], exch=trade['exchange'],
                                        prim_exch=trade['exchange'], curr='USD',
                                        opt_type=trade['opt_type'],
                                        expiry_date=trade['expiry_date'], strike=float(trade['strike']))
        contract.lot_size = trade['lot_size']
        kwargs = {
            'client': client, 'unique_id': i, 'trading_mode': account_mode, 'contract': contract,
            'side': trade['side'], 'entered': True, 'entry_order_filled': entry_order_filled,
            'bought': bought, 'sold': sold, 'instruction': trade['instruction'], 'qty': trade['quantity'],
            'trade_id': trade['trade_id'], 'entry_order_id': trade['entry_order_id'],
            'NR': trade['NR'], 'WKS': trade['WKS'], 'F': trade['F'], 'curr': trade['curr'],
            'sec_type': trade['sec_type']}

        controller.trade_managers.append(OptTradeManager(**kwargs))
        client.nextorderId += 1

    for key, value in symbols.items():
        local_symbol = key
        local_symbol = local_symbol.split()
        ticker, sec_type, curr, exch = local_symbol
        F, WKS, NR = float(value['F']), int(value['WKS']), int(value['NR'])
        if ticker in open_pos_symbols:
            logger.info(f'{ticker}: already have open order/position so trading existing instance and '
                        f'not starting strategy instance')
            continue
        client.nextorderId += 1
        contract_id = client.nextorderId

        contract_1 = client.make_contract(symbol=ticker, sec_type=sec_type, exch=exch, curr=curr)
        if not client.validate_opt_contract(contract_1): continue
        client.reqContractDetails(contract_id, contract_1)
        logger.debug(f'waiting For con id to be fetched for {ticker}')
        while ticker not in client.ticker_to_conId:
            pass
        con_id = client.ticker_to_conId[ticker]
        client.nextorderId += 1
        reqId = client.nextorderId
        client.sec_id_to_local_symbol[reqId] = key
        client.reqSecDefOptParams(reqId=reqId, underlyingSymbol=ticker, futFopExchange="", underlyingSecType=sec_type,
                                  underlyingConId=con_id)
        client.nextorderId += 1
        contract_2 = client.make_contract(symbol=ticker, sec_type=sec_type, curr=curr, exch=exch)
        if not client.validate_opt_contract(contract_2): continue
        id_2 = client.nextorderId
        client.reqMarketDataType(3)
        client.reqHistoricalData(reqId=id_2, contract=contract_2, durationStr='1 Y', barSizeSetting='1 day',
                                 whatToShow=what_type, useRTH=1, endDateTime='', formatDate=1, keepUpToDate=False,
                                 chartOptions=[])
        controller.strats[ticker] = TSP(client=client, local_symbol=str(key), unique_id_1=contract_id, unique_id_2=id_2,
                                        WKS=WKS, F=F, NR=NR, cycle=cycle, sec_type=sec_type, curr=curr)

    for ticker in open_pos_symbols_expired:
        try:
            database_row = open_pos_symbols_expired[ticker]
            expiry_date = database_row['expiry_date']
        except KeyError:
            logger.debug(f'Error: {ticker} is not available in database')
            continue
        today_string = str(datetime.today().strftime('%Y%m%d'))

        if database_row['position_status'] == 'OPEN' and database_row['entry_order_status'] == 'FILLED':
            # Closing The ticker , if not in symbols dict
            result = [[key, value] for key, value in symbols.items() if key.split()[0] == ticker]
            if not result:
                params = {'symbol': ticker, 'trade_id': database_row['trade_id']}
                save_trade(OptTradesData, action='status_closed', params=params)
                continue
            key, value = result[0]
            local_symbol = key
            local_symbol = local_symbol.split()
            ticker, sec_type, curr, exch = local_symbol
            F, WKS, NR = float(value['F']), int(value['WKS']), int(value['NR'])
            # last_closing_date
            client.nextorderId += 1
            contract_hist = client.make_contract(symbol=ticker, sec_type=sec_type, curr=curr, exch=exch)
            if not client.validate_opt_contract(contract_hist): continue
            id_2 = client.nextorderId
            client.reqMarketDataType(3)
            client.reqHistoricalData(reqId=id_2, contract=contract_hist, durationStr='1 Y', barSizeSetting='1 day',
                                     whatToShow=what_type, useRTH=1, endDateTime='', formatDate=1, keepUpToDate=False,
                                     chartOptions=[])
            while not client.validate_hist_data_reqId(id_2):
                pass
            last_closing_stk_price = client.data_frames[id_2].iloc[-1]['close']
            # Cycle Logic
            if database_row['opt_type'] == 'P':
                if float(database_row['strike']) < last_closing_stk_price:
                    cycle = 'P'
                else:
                    cycle = 'C'
            elif database_row['opt_type'] == 'C':
                if float(database_row['strike']) > last_closing_stk_price:
                    cycle = 'C'
                else:
                    cycle = 'P'
            # Closing From Database
            params = {'symbol': ticker, 'trade_id': database_row['trade_id']}
            save_trade(OptTradesData, action='status_closed', params=params)
            # Tsp Findinding starts
            client.nextorderId += 1
            contract_id = client.nextorderId
            contract_1 = client.make_contract(symbol=ticker, sec_type=sec_type, exch=exch, curr=curr)
            if not client.validate_opt_contract(contract_1): continue
            client.reqContractDetails(contract_id, contract_1)
            logger.debug(f'Waiting For con Id  to be fetched for : {ticker}')
            while ticker not in client.ticker_to_conId:
                pass
            con_id = client.ticker_to_conId[ticker]
            client.nextorderId += 1
            reqId = client.nextorderId
            client.sec_id_to_local_symbol[reqId] = key
            client.reqSecDefOptParams(reqId=reqId, underlyingSymbol=ticker, futFopExchange="",
                                      underlyingSecType=sec_type, underlyingConId=con_id)
            client.nextorderId += 1
            contract_2 = client.make_contract(symbol=ticker, sec_type=sec_type, curr=curr, exch=exch)
            if not client.validate_opt_contract(contract_2): continue
            id_2 = client.nextorderId
            client.reqMarketDataType(3)
            client.reqHistoricalData(reqId=id_2, contract=contract_2, durationStr='1 Y', barSizeSetting='1 day',
                                     whatToShow=what_type, useRTH=1, endDateTime='', formatDate=1, keepUpToDate=False,
                                     chartOptions=[])
            controller.strats[ticker] = TSP(client=client, local_symbol=str(key), unique_id_1=contract_id,
                                            unique_id_2=id_2, WKS=WKS, F=F, NR=NR, cycle=cycle, sec_type=sec_type,
                                            curr=curr)

    def run_instance(obj):
        return obj.run()

    while True:
        tsp_obj_list = [obj for obj in controller.strats.values() if not obj.contract_fetched]
        for tsp_obj in tsp_obj_list:
            tsp_return_contract = run_instance(tsp_obj)
            if tsp_return_contract == 'error':
                logger.debug(f'errr getting contract for {tsp_obj.symbol}')
                continue
            if tsp_return_contract:
                client = tsp_obj.client
                client.nextorderId += 1
                NR = tsp_obj.NR
                F = tsp_obj.F
                WKS = tsp_obj.WKS
                sec_type = tsp_obj.sec_type
                curr = tsp_obj.curr
                order_obj = OptTradeManager(client=client, unique_id=client.nextorderId, NR=NR, F=F, WKS=WKS,
                                            sec_type=sec_type, curr=curr,
                                            trading_mode=account_mode, contract=tsp_return_contract,
                                            side='SHORT')
                controller.trade_managers.append(order_obj)
        if len(controller.trade_managers):
            msg = controller.run()
            if msg == 'trade_ended': break
        elif not len(tsp_obj_list):
            break

    client.disconnect()
    client_thread.join()

    # os.kill(os.getpid(), signal.SIGTERM)
