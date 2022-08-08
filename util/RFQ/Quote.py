# Quote type for agents to send to the RFQ exchange in response to a Quote
# Loosely based on the Order class for LOB exchanges

from copy import deepcopy

class Quote:
    quote_id = 0
    _quote_ids = set()

    def __init__(self, agent_id, request, time_placed, symbol, quantity, buy, price, quote_id=None, tag=None):
        
        # Agent making quote
        self.agent_id = agent_id

        # Corresponding request quote is responding to
        self.request = request

        self.quote_id = self.generateQuoteId() if not quote_id else quote_id
        Quote._Quote_ids.add(self.quote_id) 
        
        # Time agent made quote
        self.time_placed = pd_Timestamp = time_placed
        
        # Symbol quote is made on 
        self.symbol = symbol
        
        # Size, direction and price of quote
        self.quantity = quantity
        self.buy = buy
        self.price = price

        # So quote can be deleted if discarded by requester
        self.viewed = False
        
        self.tag = tag

    def generateQuoteId(self):
        # generates a unique Quote ID if the Quote ID is not specified
        if not Quote.quote_id in Quote._quote_ids:
            id = Quote.quote_id
        else:
            Quote.quote_id += 1
            id = self.generateQuoteId()
        return id

    def to_dict(self):
        as_dict = deepcopy(self).__dict__
        as_dict['time_placed'] = self.time_placed.isoformat()
        return as_dict

    def __copy__(self):
        raise NotImplementedError

    def __deepcopy__(self, memodict={}):
        raise NotImplementedError
