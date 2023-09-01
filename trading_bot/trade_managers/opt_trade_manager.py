import uuid
from datetime import datetime

from trading_bot.settings import logger, TZ


class OptTradeManager:
    def __init__(
            self,
            client,
            unique_id,
            NR,
            F,
            WKS,
            sec_type,
            curr,
            trading_mode,
            contract,
            side=None,
            entered=False, entry_order_filled=False,
            exit_order_filled=False, bought=False, sold=False,
            instruction=None, qty=None, sl=None, final_sl=None, trade_id=None, entry_order_id=None,
            entry_order_price=None, exit_pending=False, entry_order_status=None, exit_order_id=None,
            exit_order_price=None, entry_price=None, ref_price=None, trades_limit=None, ):
        self.client = client
        self.NR = NR
        self.F = F
        self.WKS = WKS
        self.sec_type = sec_type
        self.curr = curr
        self.id = unique_id
        self.trading_mode = trading_mode
        self.contract = contract
        self.symbol = contract.symbol
        self.symbol_type = contract.secType
        self.sec_type = sec_type
        self.exchange = contract.exchange
        if self.symbol_type == 'OPT':
            self.expiry_date = contract.lastTradeDateOrContractMonth
            self.strike = contract.strike
            self.opt_type = contract.right
        else:
            self.expiry_date = ''
            self.strike = 0
            self.opt_type = ''
            self.lot_size = 1
        self.trades_limit = trades_limit
        self.side = side.upper()
        self.entered = entered
        self.bought = bought
        self.sold = sold
        self.instruction = instruction
        self.qty = qty
        self.sl = sl
        self.final_sl = final_sl
        self.ref_price = ref_price
        self.entry_order_price = entry_order_price
        self.entry_order_time = None
        self.entry_order_id = entry_order_id
        self.entry_order_filled = entry_order_filled
        self.entry_order_status = entry_order_status
        self.entry_price = entry_price
        self.entry_time = None
        self.exit_order_time = None
        self.exit_order_price = exit_order_price
        self.exit_order_id = exit_order_id
        self.exit_order_status = None
        self.exit_order_filled = exit_order_filled
        self.exit_pending = exit_pending
        self.exit_time = None
        self.exit_price = None
        self.exit_type = None
        self.position_status = None
        self.trade_ended = False
        self.ltp = None
        self.position_check = True
        self.time_based_exit = False
        self.change_sl = False
        self.messages = []
        self.trade_id = str(uuid.uuid4()) if trade_id is None else trade_id
        self.identifier = f'{self.symbol} {self.opt_type} {self.expiry_date} {self.strike}' if self.symbol_type == 'OPT' \
            else self.symbol
        logger.debug(f"""{trading_mode} Trading bot {self.symbol}, {self.identifier} instance started, 
                         parameters: unique ID: {self.id}, side: {self.side},  option type: {self.opt_type}, 
                         expiry date: {self.expiry_date}, strike: {self.strike}, trade_id: {self.trade_id}, t
                         rades limit: {self.trades_limit}""")

    def __repr__(self):
        return f"trading_mode: {self.trading_mode}, id: {self.id}, instrument: {self.identifier}"

    def trade(self):
        if self.trade_ended:
            return self
        self.messages = []
        if self.is_valid_entry():
            self.make_entry()
        if self.entered and not self.entry_order_filled:
            self.confirm_entry()
        if self.entered and self.entry_order_filled:
            logger.info(f'{self.identifier}: Entry order already filled, closing instance')
            self.trade_ended = True
        return {'msg': self.messages}

    def is_valid_entry(self):
        if self.entered:
            return False
        if self.side == 'LONG':
            logger.info(f'{self.identifier}: Long signal generated at {datetime.now(tz=TZ)}')
            self.bought = True
            self.instruction = 'BUY'
            return True
        elif self.side == 'SHORT':
            logger.info(f'{self.identifier}: Short signal generated at {datetime.now(tz=TZ)}')
            self.sold = True
            self.instruction = 'SELL'
            return True

    def make_entry(self):
        self.qty = self.NR
        logger.debug(f'{self.identifier}: Quantity set to {self.qty}')
        if self.qty < 1:
            logger.debug(f'{self.identifier}: Quantity less than 0, please increase trade size,  '
                         f'lot size: {self.NR},')
            self.trade_ended = True
            return

        self.client.nextorderId += 1
        self.entry_order_id = self.client.nextorderId
        self.client.placeOrder(self.entry_order_id, self.contract,
                               self.client.make_order(self.instruction, self.qty,
                                                      order_type='MKT'))
        self.entry_order_time = datetime.now(tz=TZ)
        self.entered = True
        self.entry_order_filled = False
        self.entry_order_status = 'OPEN'

        # Update account balance after taking position
        self.client.reqAccountSummary(9002, "All", "$LEDGER")

        logger.debug(f"""{self.identifier}: Entry order Placed to {self.instruction} qty: {self.qty}, 
                         price: {self.entry_order_price}, time:{self.entry_order_time}, 
                         order id: {self.entry_order_id}""")
        self.trade_id = str(uuid.uuid4())
        logger.debug(f'{self.identifier} Instance, new trade_id: {self.trade_id}')
        entry_data = self.save_trade(action='make_entry')
        self.messages.append(entry_data)

    def confirm_entry(self):
        for exec_order in self.client.exec_orders:
            if str(exec_order['exec_order_id']) == str(self.entry_order_id) and exec_order['symbol'] == self.symbol and \
                    exec_order['exec_qty'] == self.qty:
                self.entry_price = self.ref_price = exec_order['exec_avg_price']
                self.entry_time = exec_order['exec_time']
                self.entry_order_filled = True
                self.entry_order_status = 'FILLED'
                self.position_status = 'OPEN'
                logger.debug(
                    f"{self.identifier}: Entry order Filled to {self.instruction}, price: {self.entry_price},"
                    f" qty:{self.qty}, time:{self.entry_time}")
                entry_data = self.save_trade(action='confirm_entry')
                self.position_check = False
                self.messages.append(entry_data)
                self.trade_ended = True
                return

        for order in self.client.orders:
            if str(order['order_id']) == str(self.entry_order_id) and order['status'] in ['Cancelled', 'Inactive']:
                logger.debug(f'{self.identifier} Entry order to {self.instruction} {order["status"]}')
                self.entered = False
                self.bought, self.sold = False, False
                self.entry_time = None
                self.entry_price = None
                self.sl = None
                self.entry_order_status = order['status']
                self.position_status = None

                entry_data = self.save_trade(action='confirm_entry')
                self.messages.append(entry_data)
                self.trade_ended = True
                logger.info(f'{self.identifier}: Entry order cancelled, Closing instance')
                return

    def save_trade(self, action):
        message = dict()
        if action == 'make_entry':
            message[action] = {'symbol': self.symbol, 'symbol_type': self.symbol_type,
                               'side': self.side, 'entry_order_time': self.entry_order_time,
                               'entry_order_price': self.entry_order_price, 'instruction': self.instruction,
                               'entry_order_id': self.entry_order_id, 'entry_order_status': self.entry_order_status,
                               'quantity': self.qty, 'trade_id': self.trade_id, 'trading_mode': self.trading_mode,
                               'opt_type': self.opt_type, 'expiry_date': self.expiry_date,
                               'strike': self.strike, 'exchange': self.exchange,
                               'trades_limit': self.trades_limit, 'F': self.F, 'WKS': self.WKS, 'NR': self.NR,
                               'sec_type': self.sec_type, 'curr': self.curr}
            return message

        elif action == 'confirm_entry':
            message[action] = {'symbol': self.symbol, 'trade_id': self.trade_id, 'entry_time': self.entry_time,
                               'entry_price': self.entry_price, 'reference_price': self.ref_price,
                               'final_stop_loss': self.final_sl, 'entry_order_status': self.entry_order_status,
                               'position_status': self.position_status}
            return message

        elif action == 'make_exit':
            message[action] = {'symbol': self.symbol, 'trade_id': self.trade_id, 'instruction': self.instruction,
                               'exit_order_id': self.exit_order_id, 'exit_order_time': self.exit_order_time,
                               'exit_order_price': self.exit_order_price, 'exit_order_status': self.exit_order_status,
                               'reference_price': self.ref_price, 'final_stop_loss': self.final_sl}
            return message

        elif action == 'confirm_exit':
            message[action] = {'symbol': self.symbol, 'trade_id': self.trade_id, 'exit_time': self.exit_time,
                               'exit_price': self.exit_price, 'exit_type': self.exit_type,
                               'exit_order_status': self.exit_order_status, 'position_status': self.position_status}
            return message
