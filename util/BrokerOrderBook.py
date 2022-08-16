# Class for a broker's order book
# needs to be different from a standard order book
# because it doesn't care about price, only quantity, as they are techinally limit orders
# and don't want to do partial fills for my experiment

import sys

from message.Message import Message
from util.OrderBook import OrderBook
from util.order.LimitOrder import LimitOrder
from util.util import log_print, be_silent

from copy import deepcopy
import pandas as pd
from pandas.io.json import json_normalize
from functools import reduce
from scipy.sparse import dok_matrix
from tqdm import tqdm


class BrokerOrderBook(OrderBook):
    def __init__(self, owner, symbol):
        super().__init__(owner, symbol)
        
        self.brokerBook = True

    # just need to overwrite matching algo

    def handleLimitOrder(self, order):
        # Matches a limit order or adds it to the order book.  Handles partial matches piecewise,
        # consuming all possible shares at the best price before moving on, without regard to
        # order size "fit" or minimizing number of transactions.  Sends one notification per
        # match.
        if order.symbol != self.symbol:
            print("{} order discarded.  Does not match OrderBook symbol: {}", order.symbol, self.symbol)
            return

        if (order.quantity <= 0) or (int(order.quantity) != order.quantity):
            print("{} order discarded.  Quantity ({}) must be a positive integer.", order.symbol, order.quantity)
            return

        # Add the order under index 0 of history: orders since the most recent trade.
        self.history[0][order.order_id] = {'entry_time': self.owner.getCurrentTime(),
                                           'quantity': order.quantity, 'is_buy_order': order.is_buy_order,
                                           'limit_price': order.limit_price, 'transactions': [],
                                           'modifications': [],
                                           'cancellations': []}

        matching = True

        self.prettyPrint()

        executed = []

        while matching:
            matched_order = deepcopy(self.executeOrder(order))  

            if matched_order:
                # Decrement quantity on new order and notify traders of execution.
                filled_order = deepcopy(order)
                filled_order.quantity = matched_order.quantity
                filled_order.fill_price = matched_order.fill_price
                filled_order.fill_time = self.owner.getCurrentTime() - filled_order.time_placed
                filled_order.slippage = 0

                if matched_order.is_buy_order:
                    matched_order.slippage = matched_order.fill_price - matched_order.limit_price

                else:  matched_order.slippage = matched_order.limit_price - matched_order.fill_price 

                filled_order.filled = True
                # ensure change is permeated through all copies
                id = filled_order.order_id 
                a_id = filled_order.agent_id
                s = filled_order.slippage 
                self.owner.sendMessage(a_id, Message({"msg": "FILLED", "order_id": id, 
                                                    "sender":self.owner.id,
                                                    "fill_price": filled_order.fill_price, 
                                                    "fill_time": filled_order.fill_time, 
                                                    "quantity": filled_order.quantity,
                                                    "slip":s, 
                                                    "fill_type": "INSTANT"}))
                
                id = matched_order.order_id 
                a_id = matched_order.agent_id
                s = matched_order.slippage
                self.owner.sendMessage(a_id, Message({"msg": "FILLED", "order_id": id, 
                                                    "sender":self.owner.id,             
                                                    "fill_price": filled_order.fill_price, 
                                                    "fill_time": matched_order.fill_time, 
                                                    "quantity": filled_order.quantity,
                                                    "slip": s,
                                                    "fill_type": "BOOK"}))

                order.quantity -= filled_order.quantity

                log_print("MATCHED: new order {} vs old order {}", filled_order, matched_order)
                log_print("SENT: notifications of order execution to agents {} and {} for orders {} and {}",
                          filled_order.agent_id, matched_order.agent_id, filled_order.order_id, matched_order.order_id)

                self.owner.sendMessage(order.agent_id, Message({"msg": "ORDER_EXECUTED", "sender":self.owner.id, "order": filled_order}))
                self.owner.sendMessage(matched_order.agent_id,
                                       Message({"msg": "ORDER_EXECUTED", "sender":self.owner.id, "order": matched_order}))

                # Accumulate the volume and average share price of the currently executing inbound trade.
                executed.append((filled_order.quantity, filled_order.fill_price))

                if order.quantity <= 0:
                    # Order completely filled
                    
                    matching = False


            else:
                # No matching order was found, so the new order enters the order book.  Notify the agent.
                self.enterOrder(deepcopy(order))

                log_print("ACCEPTED: new order {}", order)
                log_print("SENT: notifications of order acceptance to agent {} for order {}",
                          order.agent_id, order.order_id)

                self.owner.sendMessage(order.agent_id, Message({"msg": "ORDER_ACCEPTED", "sender":self.owner.id, "order": order}))

                matching = False

        # Also log the last trade (total share quantity, average share price).
        if executed:
            trade_qty = 0
            trade_price = 0
            for q, p in executed:
                log_print("Executed: {} @ {}", q, p)
                trade_qty += q
                trade_price += (p * q)

            avg_price = int(round(trade_price / trade_qty))
            log_print("Avg: {} @ ${:0.4f}", trade_qty, avg_price)
            self.owner.logEvent('LAST_TRADE', "{},${:0.4f}".format(trade_qty, avg_price))

            self.last_trade = avg_price

            # Transaction occurred, so advance indices.
            self.history.insert(0, {})

            # Truncate history to required length.
            self.history = self.history[:self.owner.stream_history + 1]


        self.last_update_ts = self.owner.getCurrentTime()
        self.prettyPrint()
        #print(self.getInsideAsks(), self.getInsideBids())
       

    def isMatch(self, order, o):
        # Returns True if order 'o' can be matched against input 'order'.
        if order.is_buy_order == o.is_buy_order:
            print("WARNING: isMatch() called on orders of same type: {} vs {}".format(order, o))
            return False

        if (order.quantity == o.quantity):
            # print("match")
            return True

        return False

    def enterOrder(self, order):
        # Enters a limit order into the OrderBook in the appropriate location.
        # This does not test for matching/executing orders -- this function
        # should only be called after a failed match/execution attempt.
        if order.is_buy_order:
            book = self.bids
        else:
            book = self.asks

        if not book:
            # There were no orders on this side of the book.
            book.append([order])
        # no longer need price levels
        # put everthing on level 0 for compatibility with old code
        else:
            book[0].append(order)
        


    def executeOrder(self, order):
        # Finds a single best match for this order.
        # Returns the matched order or None if no match found.  DOES remove,
        # or decrement quantity from, the matched order from the order book
        # (i.e. executes at least a partial trade, if possible).

        # Track which (if any) existing order was matched with the current order.
        if order.is_buy_order:
            book = self.asks
        else:
            book = self.bids
        

        # First, examine the correct side of the order book for a match.
        if not book:
            # No orders on this side.
            return None

        else:
            # Note that book[i] is a LIST of all orders (oldest at index book[i][0]) at the same price.
            # NOTE that now book only has one price level
            
            for i in range(len(book)):
                matched_order = book[0][i]
                
                if self.isMatch(order, matched_order):
                    # Found a match           
                    matched_order = book[0].pop(i)

                    matched_order.fill_price = order.limit_price
                    matched_order.fill_time = self.owner.getCurrentTime() - matched_order.time_placed
                    matched_order.slippage = order.limit_price - matched_order.limit_price


                    if not book[0]:
                        #print("delet level")
                        del book[0]
            

                    # The incoming order is guaranteed to exist under index 
                    self.history[i][order.order_id]['transactions'].append((self.owner.getCurrentTime(), order.quantity))

                    # The pre-existing order may or may not still be in the recent history.
                    for idx, orders in enumerate(self.history):
                        if matched_order.order_id not in orders: continue

                        # Found the matched order in history.  Update it with this transaction.
                        self.history[idx][matched_order.order_id]['transactions'].append(
                            (self.owner.getCurrentTime(), matched_order.quantity))
                        return matched_order

           
            return None

    def getBidVolume(self):
        #print("bids", self.getInsideBids())
        return sum([o[1] for o in self.getInsideBids()])
    
    def getAskVolume(self):
       #print("asks", self.getInsideAsks())
        return sum([o[1] for o in self.getInsideAsks()])

