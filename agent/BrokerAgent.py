# Type of trading agent that takes retail agent orders and forwards
# them to an exchange, if it can't match against its own book.
# Aims to mimic IRL retail methods
# Inherits from exchange agent, as essentially functions as its own exchange

from agent.ExchangeAgent import ExchangeAgent
from message.Message import Message
from util.OrderBook import OrderBook
from util.util import log_print

import datetime as dt

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

import pandas as pd
pd.set_option('display.max_rows', 500)

from copy import deepcopy


class RetailExecutionAgent(ExchangeAgent): 
    def __init__(self, id, name, type, exchange, delay=0, random_state=None):

        # Get exchange stats from paired exchange
        self.exchange = exchange
        
        # Open an exchange with the same stats as the real exchange
        super().__init__(id, name, type, exchange.mkt_open, exchange.mkt_close, exchange.symbols, exchange.book_freq, exchange.wide_book, exchange.pipeline_delay, exchange.computation_delay, 
                        exchange.stream_history, exchange.days, exchange.log_orders, random_state)
        

        # Delay for going to the broker
        self._delay = delay

        def receiveMessage(self, currentTime, msg):
            self.setComputationDelay(self.computation_delay)

            # Close logic for first days
            # Use exchange, not super, as super not updating for market days
            if self.exchange.checkMarketClosed(): 
                if msg.body['msg'] in ['LIMIT_ORDER', 'MARKET_ORDER', 'CANCEL_ORDER', 'MODIFY_ORDER']:
                    self.sendMessage(msg.body['sender'], Message({"msg": "MKT_CLOSED"}))
                    return

            # For final day
            if self.exchange.currentTime > self.exchange.mkt_close and self.exchange.currentTime > self.exchange.mkt_open and (self.exchange.sim_days == self.exchange.current_day):
                super.sendMessage(msg.body['sender'], Message({"msg": "FINAL_CLOSE"}))
                return


             # If the message is an order, check if we can match it in our books
            # otherwise forward it to the exchange
            # TODO: how do we add to our books? check literature
            # IDEA: if limit, check against main exchange (+ ourselves) for match, otherwise we take it?
            # TODO: if market check literature !

            if msg.body['msg'] == 'LIMIT_ORDER':
                pass
            elif msg.body['msg'] == 'MARKET_ORDER':
                pass
            elif msg.body['msg'] == 'CANCEL_ORDER':
                pass
            elif msg.body['msg'] == 'MODIFY_ORDER':
                pass
            else:
                # Forward all other messages to the exchange
                self.exchange.receiveMessage(currentTime, msg)
        
        # TODO: orderbook logging?


