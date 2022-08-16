# Type of agent that takes retail agent orders and forwards
# them to an exchange, if it can't match against its own book.
# Aims to mimic IRL retail methods
# Inherits from exchange agent, as essentially functions as its own exchange

from agent.ExchangeAgent import ExchangeAgent
from message.Message import Message
from util import order
from util.BrokerOrderBook import BrokerOrderBook
from util.util import log_print
from util.order.Order import Order
from copy import deepcopy
from util.order.MarketOrder import MarketOrder
from util.order.LimitOrder import LimitOrder
import datetime as dt

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

import pandas as pd
pd.set_option('display.max_rows', 500)

from copy import deepcopy


class BrokerAgent(ExchangeAgent): 
    # FUNCTIONALITY
    # must receive order messages from traders with broker
    # must store, match, or combine to forward to exchange
    # TODO: could also trade against from own portfolio? - complicated

    def __init__(self, id, name, type, exchange, delay=0, random_state=None):

        # Get exchange stats from paired exchange
        self.exchange = exchange
        self.exchangeID = exchange.id

        # Open an exchange with the same stats as the real exchange
        super().__init__(id, name, type, self.exchange.mkt_open, self.exchange.mkt_close, self.exchange.symbols, self.exchange.book_freq,
                        self.exchange.wide_book, self.exchange.pipeline_delay, self.exchange.computation_delay, 
                        self.exchange.stream_history, self.exchange.sim_days, self.exchange.log_orders, random_state, broker_book=True)
        

        # Delay for going to the broker
        self._delay = delay
        self.odd_lot_size = 5
        self.order_num = self.id * 1000

        self.combined_orders = {} # {order_id: set of orders}
        self.book_orders = {} # {new_order_id: old_order_id}
    
    def receiveMessage(self, currentTime, msg):
        self.setComputationDelay(self.computation_delay)

        # Use exchange, not super, as super not updating for market days
        if self.exchange.checkMarketClosed(): 
            if msg.body['msg'] in ['LIMIT_ORDER', 'MARKET_ORDER', 'CANCEL_ORDER', 'MODIFY_ORDER']:
                self.sendMessage(msg.body['sender'], Message({"msg": "MKT_CLOSED"}))
                return

        # For final day
        if self.exchange.currentTime > self.exchange.mkt_close and self.exchange.currentTime > self.exchange.mkt_open and (self.exchange.sim_days == self.exchange.current_day):
            super().sendMessage(msg.body['sender'], Message({"msg": "FINAL_CLOSE"}))
            return


        # If the message is an order, check if odd in which case must go to our books
        # Otherwise,  send to an exchange
        if msg.body['msg'] == 'LIMIT_ORDER':
            # TODO: add limit order functionality when broker has clients that place them
            pass

        elif msg.body['msg'] == 'MARKET_ORDER':
            order = msg.body['order']        
            symbol = order.symbol
            if order.symbol not in self.order_books:
                # discard, not in book
                log_print("Market Order discarded.  Unknown symbol: {}", order.symbol)

            if order.quantity % self.odd_lot_size != 0: # if odd lot
                # Can we combine it with existing orders to make an even lot?
                evenLotOrder = self.checkEvenLot(order)
                
                if evenLotOrder is not None:
                   # print("exchange combined", order.quantity, "with", evenLotOrder.quantity - order.quantity)
                    #print(self.order_books[symbol].getAskVolume(), self.order_books[symbol].getBidVolume())
                    #print()
                    # Send even lot order to exchange
                    self.logEvent("ORDER_COMBINED_TO_EXCHANGE")
                    self.sendMessage(self.exchangeID, Message({"msg" : "MARKET_ORDER", "sender": self.id, "order": evenLotOrder}))
                
                else:
                    # Odd lot
                    # Otherwise try to match in books # TODO: combine book orders to do this? is this realistic?
                    # Do this as a LIMIT order to disallow splitting orders
                    # get price as current best in main exchange
                    if order.is_buy_order:
                        # get best price in exchange at current time 
                        best_price = self.exchange.order_books[symbol].getBestAsk()

                    else:
                        # get best price in exchange at current time 
                        best_price = self.exchange.order_books[symbol].getBestBid()

                    if not best_price:
                        # No price estimation, so send to exchange
                        self.logEvent("ORDER_SENT_TO_EXCHANGE")
                        self.sendMessage(self.exchangeID, Message({"msg" : "MARKET_ORDER", "sender": self.id, "order": order}))
                        return
                    # print(order.is_buy_order, "to broker book", order.quantity)
                    self.logEvent("ORDER_SENT_TO_BOOK")
                    limit_order = LimitOrder(order.agent_id, order.time_placed, order.symbol, order.quantity, order.is_buy_order, limit_price=best_price, best=best_price, order_id=order.order_id)
                    self.order_books[symbol].handleLimitOrder(limit_order)
                    self.order_num += 1


            else:   # if even lot
                # Send to exchange
             #   print("exchange EVEN")
              #  print()
                self.logEvent("ORDER_SENT_TO_EXCHANGE")
                self.sendMessage(self.exchangeID, Message({"msg" : "MARKET_ORDER", "sender": self.id, "order": order}))
        
        elif msg.body['msg'] == 'ORDER_ACCEPTED':
            order = msg.body['order']
            # only enters here when combined as exchange sends straight to agent_id if not
            order_set = self.combined_orders[order.order_id]

            for o in order_set:
                self.sendMessage(o.agent_id, Message({"msg": "ORDER_ACCEPTED", "order": o}))
        
        elif msg.body['msg'] == 'ORDER_EXECUTED':
            order = msg.body['order']
            
            if msg.body['sender'] == self.exchangeID:
                # If executed by exchange, send to all agents
                # only enters here when combined as exchange sends straight to agent_id if not
                order_set = self.combined_orders[order.order_id]
                for o in order_set:   
                    o.fill_price = order.fill_price
                    s = o.fill_price - o.limit_price  if o.is_buy_order else o.limit_price - o.fill_price
                    t = self.getCurrentTime() - o.time_placed
                    self.sendMessage(o.agent_id, Message({"msg": "FILLED", "order_id": o.order_id, 
                                                    "sender":self.id,
                                                    "fill_price": order.fill_price, 
                                                    "fill_time": t,
                                                    "quantity": o.quantity,
                                                    "slip": s,
                                                    "fill_type": "COMBINED_BOOK"}))
                    self.sendMessage(o.agent_id, Message({"msg": "ORDER_EXECUTED", "order": o}))
            else: 
                # Executed in our books,
                # Message should be sent through to original agent so don't need to do anything here?
                pass


        elif msg.body['msg'] == 'ORDER_CANCELLED':
             # only enters here when combined as exchange sends straight to agent_id if not
            agents = self.combined_orders[order.order_id]

            for agent_id in agents: 
                self.sendMessage(agent_id, Message({"msg": "ORDER_EXECUTED", "order": order}))

     
        else:
            # Forward all other messages to the exchange
            self.exchange.receiveMessage(currentTime, msg)

        # TODO: orderbook logging?

    def checkEvenLot(self, order):
        evenSet = []
        evenOrder = None
        quantity = order.quantity
        id = order.order_id
        # check if combining with existing order quantities creates and even lot - the more the better (?)

        if order.is_buy_order and len(self.order_books[order.symbol].bids) != 0:
            all_orders = self.order_books[order.symbol].bids[0]
            
        elif len(self.order_books[order.symbol].asks) != 0: # sell order
            all_orders = self.order_books[order.symbol].asks[0]

        else:
            return 
        all_id_quants = [(o.order_id, o.quantity) for o in all_orders] 

        if len(all_id_quants) == 0:
            # orderbook empty
            return None

        subset_ids = self.findSubsetsSumDivisbleN(quantity, all_id_quants, self.odd_lot_size)

        if subset_ids is not None:
            for id in subset_ids:
                for o in all_orders: 
                    if o.order_id == id:
                         evenSet.append(o)
                         quantity += o.quantity
                
                evenOrder = MarketOrder(self.id, self.getCurrentTime(), order.symbol, quantity, order.is_buy_order,  order_id=self.order_num ,combined=True, best=o.limit_price)
                self.order_num += 1

            # Record order and agents involved
            self.combined_orders[evenOrder.order_id] = evenSet

            # Remove orders from order books
            for o in evenSet:
                self.order_books[order.symbol].cancelOrder(o, quiet=True)
                # print(o.quantity, "removed from", order.symbol)
                # quiet since agents don't need to know, as it hasn't really been cancelled, just combined

            return evenOrder
        return None

    def findSubsetsSumDivisbleN(self, q, arr, n):
        # TODO: this is a bit of a hack solution, 
        # if possible use full dynamic programming method that finds largest subset 
        # takes pairs of ids and quantities
        # find all subsets of arr with sum divisible by n
        # uses greedy/dynamic algorithm with a search limit

        # first check all pairs
        pair_sums = []
        for id, qq in arr:
            if (q + qq) % n == 0:
                return ([id])
            else:
                pair_sums.append(([id], (q + qq)))

        # if no pair found, check all triplets
        triplet_sums = []
        for ids, qq in pair_sums:
            for id, qqq in arr:
                if id not in ids:
                    if (qq + qqq) % n == 0:
                        ids.append(id)
                        return (ids)
                    else:
                        a = deepcopy(ids)
                        a.append(id)
                        triplet_sums.append((a, (qq + qqq)))

        # finally check quadruplets
        quadruplet_sums = []   
        for ids, qq in triplet_sums:
            for id, qqq in arr:
                if id not in ids:
                    if (qq + qqq) % n == 0:
                        ids.append(id)
                        return (ids)
                    else:
                        a = deepcopy(ids)
                        a.append(id)
                        quadruplet_sums.append((a, (qq + qqq)))
        
        return None # no subset found

    def getCurrentTime(self):
        return(self.exchange.currentTime)



          





        