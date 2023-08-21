import yfinance as yf
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import pandas as pd
import yfinance as yf
import csv

def create_engine(name):
    engine = sqlalchemy.create_engine('sqlite:///'+name+'_db')
    return engine
    
#get metrics about each company, some of which are updated daily
def get_daily_metrics(symbol_list):
    SQLALCHEMY_DATABASE_URL = 'sqlite:///stock_info'
    engine = db.create_engine(SQLALCHEMY_DATABASE_URL)
    connection = engine.connect()
    session = sessionmaker()
    session.configure(bind=engine)
    Base = declarative_base()
    Base.metadata.create_all(bind=engine)
    metadata = db.MetaData()
    
    metrics_df = pd.DataFrame()
    for symbol in symbol_list:
        ticker = yf.Ticker(symbol)
        #reducing the fields that need to be retrieved
        fields_to_retrieve = ['zip', 'sector', 'longBusinessSummary', 'city', 'phone', 'state', 'country', 'website', 'address1', 'industry',
        'ebitdaMargins', 'profitMargins', 'grossMargins', 'operatingCashflow', 'revenueGrowth', 'operatingMargins',
        'ebitda', 'targetLowPrice', 'recommendationKey', 'grossProfits', 'freeCashflow', 'targetMedianPrice',
        'currentPrice', 'earningsGrowth', 'currentRatio', 'returnOnAssets', 'targetMeanPrice', 'debtToEquity',
        'returnOnEquity', 'targetHighPrice', 'totalCash', 'totalDebt', 'totalRevenue', 'financialCurrency', 
        'revenuePerShare', 'exchange', 'shortName', 'longName', 'exchangeTimezoneName', 'exchangeTimeZoneShortName',
        'symbol', 'market', 'enterpriseEbitda', 'forwardEPS', 'sharesOutstanding', 'trailingEps', 'SandP52WeekChange',
        'beta', 'forwardPE', 'previousClose', 'regularMarketOpen', 'twoHundredDayAverage', 'fiftyDayAverage',
        'regularMarketDayLow', 'currency', 'trailingPE', 'regularMarketVolume', 'marketCap', 'averageVolume', 'dayLow', 'ask',
        'askSize', 'volume', 'fiftyTwoWeekHigh', 'fiftyTwoWeekLow', 'bid', 'dayHigh', 'regularMarketPrice', 'logo_url']
        
        daily_metrics = {key:ticker.info[key] for key in ticker.info if key in fields_to_retrieve}
        metrics_df = metrics_df.append([daily_metrics], ignore_index=True)
    return metrics_df
    


if __name__ == "__main__":
    symbol_list = []
    with open('symbols_TSX.csv', newline='') as inputfile:
        for row in csv.reader(inputfile):
            symbol_list.append(row[0])
    print(len(symbol_list))
    metrics_df = get_daily_metrics(symbol_list) #retrieve daily metrics and assign to df
    metrcs_df.to_sql('daily_metrics', con=engine, if_exists='replace', index=False) #replace existing
    
    
 


