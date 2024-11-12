from requests import get, exceptions, Session
from pandas import DataFrame, to_datetime, read_csv, Series, concat
from multiprocessing import Pool
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import os

import sys
sys.path.append(os.path.normpath(os.path.join(os.path.dirname(__file__), '../Commonly Used Functions')))
from user_defined_exceptions import *  # fetches all exceptions used below (even though they error out in IDEs)

"""This file contains implementations for connecting to frequently used REST endpoints provided by xignites, and to
process the json data returned into familiar tabular formats. The correct method for large REST connections is to use
a session object with well defined parameters instead of a tacked on URL. That change will be made at some point."""


xignites_token = {'_token': 'C1A20CF1E08D49428EDC3B187C8A0EFA'}


def fundamentals_factset(tickers):
    """This API returns fundamentals data for tickers from the Factset database. We use only a subset of it from the
    json response."""

    url = 'https://factsetfundamentals.xignite.com/xFactSetFundamentals.json/GetFundamentals'
    params = {'IdentifierType': 'Symbol', 'Identifiers': '', 'FundamentalTypes': 'MarketCapitalization',
              'AsOfDate': '', 'ReportType': 'Annual', 'ExcludeRestated': 'False', 'UpdatedSince': ''}
    params.update(xignites_token)
    session_object = Session()

    df = DataFrame(columns=['Symbol', 'Sector', 'Subsector', 'MarketCap'])
    for ticker in tickers:
        params.update(Identifiers=ticker)
        try:
            resp = session_object.get(url, params=params, timeout=5)
        except exceptions.Timeout:
            print('Timed out getting fundamentals for {}'.format(ticker))
            continue

        if resp.json()[0]['Outcome'] == 'Success':
            resp = resp.json()[0]
            df = df.append({'Symbol': resp['Company']['Symbol'], 'Sector': resp['Company']['Industry'],
                            'Subsector': resp['Company']['Sector'],
                            'MarketCap': resp['FundamentalsSets'][0]['Fundamentals'][0]['Value']}, ignore_index=True)
    return df


def fetch_only_tickers(exchange, append_exchange=False):
    """This API returns only ticker data. Used as a starting point for any strategy relying on a ticker subset."""

    params = {'Exchange': exchange}
    params.update(xignites_token)
    resp = get('https://globalrealtimeoptions.xignite.com/xglobalrealtimeoptions.json/ListUnderlyingSymbols', params)

    if resp.json()['Outcome'] != 'Success':
        print('Error fetching tickers for exchange ' + exchange)
        raise TickerFetchError

    frame = DataFrame(resp.json()['UnderlyingSecurityDescriptions'])

    if append_exchange:
        tickers = (frame['Symbol'].astype(str) + '.' + frame['Exchange'].astype(str)).tolist()
    else:
        tickers = frame['Symbol'].tolist()

    return tickers


def fetch_tickers_from_chain(last_date: str, exchange: str = 'OPRA'):
    """The original API returns the entire option chain for all possible tickers as of last_date. This particular
    function simply uses the returned data to determine all active tickers being indexed."""

    df = DataFrame()
    params = {'Exchange': exchange, 'Date': last_date, 'Type': 'Calls'}
    params.update(xignites_token)

    resp = get('https://cloudfiles.xignite.com/xGlobalOptionsFile.json/GetFile', params)
    if resp.json()['Outcome'] == 'Success':
        df = df.append(read_csv(resp.json()['Url']))

    params.update({'Type': 'Puts'})
    resp = get('https://cloudfiles.xignite.com/xGlobalOptionsFile.json/GetFile', params)
    if resp.json()['Outcome'] == 'Success':
        df = df.append(read_csv(resp.json()['Url']))

    tickers = df.UnderlyingSymbol.unique().tolist()
    return tickers


def retrieve_options_chain(ticker, expiry_month=None, expiry_year=None, option_exchange='OPRA'):
    """Returns option chain for a ticker for all options expiring in the specified month & year."""

    params = {'IdentifierType': 'Symbol', 'Identifier': ticker, 'Month': expiry_month, 'Year': expiry_year,
              'SymbologyType': '', 'OptionExchange': option_exchange}
    params.update(xignites_token)
    url = 'https://globalrealtimeoptions.xignite.com/xglobalrealtimeoptions.json/GetEquityOptionChain'

    if expiry_month is None and expiry_year is None:
        url = url.replace('GetEquity', 'GetAllEquity')
        del params['Month'], params['Year']

    try:
        resp = get(url, params, timeout=5)
    except exceptions.Timeout:
        print('Timed out for ' + ticker)
        raise APIError

    if resp.status_code != 200:
        print('Options chain retrieval error for {}'.format(ticker))
        raise APIError

    resp = resp.json()
    if resp['Outcome'] == 'RequestError':
        raise APIError

    # cannot use this beautiful one-liner because exp['Calls'] or exp['Puts'] may be NoneType and cannot be concatenated
    # df = DataFrame([x for exp in resp['Expirations'] for x in exp['Calls']+exp['Puts']])

    # hence this abomination
    try:
        df = []
        for exp in resp['Expirations']:
            if exp['Calls'] is not None:
                df.append(DataFrame(exp['Calls']))
            if exp['Puts'] is not None:
                df.append(DataFrame(exp['Puts']))
        df = concat(df)
    except (TypeError, ValueError):
        print('No option chain data returned for {}'.format(ticker))
        return None

    try:
        df.loc[:, 'ExpirationDate'] = to_datetime(df.loc[:, 'ExpirationDate'])
    except KeyError:
        print('ExpirationDate key error for ' + ticker)
        raise APIError

    df.loc[:, 'Spot'] = resp['Quote']['Last']
    df.drop('Symbol', axis=1, inplace=True)
    df.rename(columns={'BaseSymbol': 'Symbol'}, inplace=True)

    return df


def daily_options_chain(dates, filter_ticker: str = None, exchange='OPRA'):
    url = 'https://cloudfiles.xignite.com/xGlobalOptionsFile.json/GetFile'
    params = {'Exchange': exchange}
    params.update(xignites_token)

    df = DataFrame()
    for dt in dates:
        params.update({'Date': dt.strftime('%m/%d/%Y')})
        for type_instrument in ['Calls', 'Puts']:
            params.update({'Type': type_instrument})
            resp = get(url, params).json()
            if resp['Outcome'] == 'Success':
                if filter_ticker is not None:
                    df = df.append(read_csv(resp['Url']).query('UnderlyingSymbol=="' + filter_ticker + '"'))
                else:
                    df = df.append(read_csv(resp['Url']))

    if not df.empty:
        df.loc[:, ['Date', 'ExpiryDate']] = df.loc[:, ['Date', 'ExpiryDate']].apply(to_datetime)
        df.drop('Symbol', axis=1, inplace=True)
        df.rename(columns={'UnderlyingSymbol': 'Symbol'}, inplace=True)

    return df


def retrieve_earnings(ticker):
    """Returns earnings date for ticker along with before / after market indicator."""

    params = {'IdentifierType': 'Symbol', 'Identifier': ticker}
    params.update(xignites_token)
    url = 'https://www.xignite.com/xEarningsCalendar.json/GetAnnouncement'

    resp = get(url, params)

    if resp.status_code != 200:
        print('Earnings retrieval error for {}'.format(ticker))
        return None

    resp = resp.json()
    if resp['Security'] is None:
        return DataFrame()

    return DataFrame([resp['Security']['Symbol'], (to_datetime(resp['EarningsDate']).date() if resp['EarningsDate'] is not None
                                                   else resp['EarningsDate']),
                      resp['TimeType']], index=['Symbol', 'Earnings Date', 'TimeType']).T


def weekly_stock_OHLC(ticker, start_date, end_date, is_index=False):
    url_1 = 'https://globalhistorical.xignite.com/v3/xGlobalHistorical.json/GetGlobalHistoricalWeeklyQuotesRange?' \
            'AdjustmentMethod=PriceReturn&IdentifierType=Symbol&Identifier='
    url_2 = '&IdentifierAsOfDate=&AdjustmentMethod=All&StartDate='
    url_3 = '&EndDate='
    url_4 = '&_token=C1A20CF1E08D49428EDC3B187C8A0EFA'
    params = {'IdentifierType': 'Symbol', 'Identifier': ticker, 'AdjustmentMethod': 'All', 'IdentifierAsOfDate': '',
              'StartDate': start_date.strftime('%m/%d/%Y'), 'EndDate': end_date.strftime('%m/%d/%Y')}
    params.update(xignites_token)
    url = 'https://globalhistorical.xignite.com/v3/xGlobalHistorical.json/GetGlobalHistoricalWeeklyQuotesRange'

    if is_index:
        url = 'https://globalindiceshistorical.xignite.com/xglobalindiceshistorical.json/GetHistoricalIndexWeeklyValues'
        # url_1 = 'https://globalindiceshistorical.xignite.com/xglobalindiceshistorical.json/GetHistoricalIndexWeekly' \
        #         'Values?IdentifierType=Symbol&Identifier='
        # url_2 = '&StartDate='

    # url = url_1 + ticker + url_2 + start_date.strftime('%m/%d/%Y') + url_3 + end_date.strftime('%m/%d/%Y') + url_4

    resp = get(url, params)

    if resp.status_code != 200:
        print('Weekly OHLC API did not respond properly for ' + ticker)
        return None

    resp = resp.json()
    if resp['Outcome'] == 'RequestError' or len(resp['HistoricalQuotes']) == 0:
        print('Weekly OHLC data not properly returned for ' + ticker)
        return None

    frame = DataFrame(resp['HistoricalQuotes'])
    frame['ticker'] = [ticker] * len(frame)
    frame.Date = to_datetime(frame.Date)
    return frame.sort_values('Date')


def daily_stock_OHLC(ticker, start_date, end_date, is_index=False):
    url_1 = 'https://globalhistorical.xignite.com/v3/xGlobalHistorical.json/GetGlobalHistoricalQuotesRange?' \
            'AdjustmentMethod=PriceReturn&IdentifierType=Symbol&Identifier='
    url_2 = '&IdentifierAsOfDate=&AdjustmentMethod=All&StartDate='
    url_3 = '&EndDate='
    url_4 = '&_token=C1A20CF1E08D49428EDC3B187C8A0EFA'

    if is_index:
        url_1 = 'https://globalindiceshistorical.xignite.com/xglobalindiceshistorical.json/GetHistoricalIndexValues?' \
                'IdentifierType=Symbol&Identifier='
        url_2 = '&StartDate='

    url = url_1 + ticker + url_2 + start_date.strftime('%m/%d/%Y') + url_3 + end_date.strftime('%m/%d/%Y') + url_4

    resp = get(url)

    if resp.status_code != 200:
        print('Daily OHLC API did not respond properly for ' + ticker)
        return None

    resp = resp.json()
    if resp['Outcome'] == 'RequestError' or len(resp['HistoricalQuotes']) == 0:
        print('Daily OHLC data not properly returned for ' + ticker)
        return None

    frame = DataFrame(resp['HistoricalQuotes'])
    frame['ticker'] = [ticker] * len(frame)
    frame.Date = to_datetime(frame.Date)
    return frame.sort_values('Date')


def retrieve_latest_quote(ticker):
    url = 'http://globalquotes.xignite.com/v3/xGlobalQuotes.json/GetGlobalDelayedQuote'
    params = {'IdentifierType': 'Symbol', 'Identifier': ticker}
    params.update(xignites_token)

    resp = get(url, params)

    if resp.status_code != 200:
        print('Cannot retrieve latest quote for ' + ticker)
        return None

    resp = resp.json()
    latest_quote = Series(resp)
    latest_quote.Date = to_datetime(latest_quote.Date)
    return latest_quote


def main():
    # output = fundamentals_factset(['AAPL', 'MSFT', 'IBM'])

    # daily options chain retrieval test
    # options_data = daily_options_chain([date(2016, 6, 30)])

    # stock OHLC retrieval test
    # ticker = ''
    # end_date = date.today()
    # start_date = end_date - relativedelta(weeks=52)
    # price_data = weekly_stock_OHLC(ticker, start_date, end_date)
    # exit(0)

    # options chain retrieval test
    # try:
    #     ticker = '8TRA.XSTO'
    #     option_data = retrieve_options_chain(ticker, option_exchange='_OMXN')
    #     pass
    # except APIError:
    #     print('API issue retrieving options chain for {}'.format(ticker))
    # except:
    #     print('Some undefined error while retrieving options chain for {}'.format(ticker))

    # earnings date retrieval test
    tickers = ['BRK.B', 'MSFT']
    print('wut')
    earnings = retrieve_earnings(tickers[0])
    pass

    # ticker fetch test
    last_working_day = date.today()-timedelta(days=1) if date.today().weekday() not in [0, 5, 6]\
        else date.today()-timedelta(days=(date.today().weekday()-4) % 7)
    try:
        tickers = fetch_tickers_from_chain(last_working_day.strftime('%m-%d-%Y'))
    except TickerFetchError:
        print('API error while fetching tickers')

    try:
        for exchange in ['OPRA', 'XMOD', '_OMXN']:
            print(exchange, len(fetch_only_tickers(exchange)))
    except TickerFetchError:
        print('API error while fetching tickers')


if __name__ == '__main__':
    main()
