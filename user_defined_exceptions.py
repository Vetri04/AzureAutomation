class APIError(Exception):
    '''Meant for catching exceptions with API data retrievals (like option chains or stock prices etc)'''
    pass


class TickerFetchError(Exception):
    '''Meant for catching exceptions with Ticker retrieval APIs only'''
    pass
