# Request type for agents to send to the RFQ exchange
# Loosely based on the "Order" class for LOB exchanges

from copy import deepcopy

class Request:
    request_id = 0
    _request_ids = set()

    def __init__(self, agent_id, time_placed, symbol, best_prices, req_id=None, tag=None):
        # TODO: need to specify buy or sell ?
        
        self.agent_id = agent_id

        self.req_id = self.generateReqId() if not req_id else req_id
        Request._req_ids.add(self.req_id) 
        
        # Time request was made by agent
        self.time_placed = pd_Timestamp = time_placed   
        
        # Equity symbol for order
        self.symbol = symbol

        # Best prices for symbol at time of request, for slippage calc 
        # TODO: how in RFQ
        self.best_prices = best_prices

        self.tag = tag

    def generateReqId(self):
        # generates a unique ID if the ID is not specified
        if not Request.req_id in Request._req_ids:
            id = Request.req_id
        else:
            Request.req_id += 1
            id = self.generateRequestId()
        return id

    def to_dict(self):
        as_dict = deepcopy(self).__dict__
        as_dict['time_placed'] = self.time_placed.isoformat()
        return as_dict

    def __copy__(self):
        raise NotImplementedError

    def __deepcopy__(self, memodict={}):
        raise NotImplementedError
