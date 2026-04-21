import pandas as pd
import numpy as np
import logging

#concatenate the data frames into one single table
def concatenate (dataframes):
    logging.info("concatenating df")
    concat_df = pd.concat(dataframes, axis = 0, ignore_index = True)
    logging.info('concatenation done')
    return concat_df

#clean the data
def clean_data (df):
    #declaring variables
    logging.info("cleaning df")
    clean_df = df.copy()
    cols = ['open','high','low','close']
    
    #drop messy data
    logging.info("dropping messy data")
    clean_df = clean_df.drop_duplicates(subset=['date','symbol'])
    clean_df = clean_df.dropna()
    
    #setting right datatype
    logging.info("setting right datatype")
    clean_df['date'] = pd.to_datetime(clean_df['date'])
    clean_df[cols] = clean_df[cols].astype(float)
    clean_df['volume'] = clean_df['volume'].astype(int)
    clean_df = clean_df.sort_values(by = ['symbol','date']).reset_index(drop = True)

    logging.info("cleaning done")
    return clean_df

#adding columns of features to my data
def create_features (df):
    logging.info("adding features")
    new_df = df.copy()
    new_df = new_df.sort_values(by = ['symbol', 'date'])
    
    #price change on day
    new_df['price_change'] = new_df['close'] - new_df['open']
    
    #percentage of daily return
    new_df['daily_return'] = (new_df['close'] - new_df['open']) / new_df['open'].replace([0, np.inf, -np.inf], np.nan)
    
    #adding logarithmic returns from previous close to current close
    prev_close = new_df.groupby('symbol')['close'].shift(1)
    ratio = new_df['close'] / prev_close
    ratio = ratio.replace([0, np.inf, -np.inf], np.nan)
    new_df['log_return'] = np.log(ratio)

    #adding 7 last days moving average (MA)
    new_df['MA_7days'] = new_df.groupby('symbol')['close'].transform(lambda x: x.rolling(window = 7, min_periods=1).mean())

    #adding 21 last days moving avegare (MA)
    new_df['MA_21days'] = new_df.groupby('symbol')['close'].transform(lambda y: y.rolling(window = 21, min_periods=1).mean())
    
    #adding 50 last days moving average (MA)
    new_df['MA_50days'] = new_df.groupby('symbol')['close'].transform(lambda z: z.rolling(window = 50, min_periods=1).mean())

    #distance percentage of current close price in each moving average 
    new_df['distance_MA7'] = (new_df['close'] - new_df['MA_7days']) / new_df['MA_7days'].replace(0, np.nan)
    new_df['distance_MA21'] = (new_df['close'] - new_df['MA_21days']) / new_df['MA_21days'].replace(0, np.nan)
    new_df['distance_MA50'] = (new_df['close'] - new_df['MA_50days']) / new_df['MA_50days'].replace(0, np.nan)

    #adding volatility measure feature
    new_df['volatility'] = new_df.groupby('symbol')['log_return'].transform(lambda x: x.rolling(window = 21, min_periods=10).std())

    #adding relative volume measure (for how much the current volume is different from past volumes)
    new_df['relative_volume'] = new_df['volume'] / new_df.groupby('symbol')['volume'].transform(lambda x: x.rolling(window = 21, min_periods=10).mean()).replace([0, np.inf, -np.inf], np.nan)

    #adding momentum formula
    past_close = new_df.groupby('symbol')['close'].shift(21)
    past_close = past_close.replace([0, np.inf, -np.inf], np.nan)
    new_df['momentum'] = (new_df['close'] / past_close) - 1
 
    #adding week day and month
    weekday = new_df['date'].dt.weekday
    new_df['weekday_cos'] = np.cos((weekday / 6) * 2 * np.pi)
    new_df['weekday_sin'] = np.sin((weekday / 6) * 2 * np.pi)

    month = new_df['date'].dt.month
    new_df['month_cos'] = np.cos(((month-1) / 11) * 2 * np.pi)
    new_df['month_sin'] = np.sin(((month-1) / 11) * 2 * np.pi)

    #adding future return (in percentage) from current stock
    future_price = new_df.groupby('symbol')['close'].shift(-5)
    new_df['future_return'] = (future_price / new_df['close'].replace(0, np.nan)) - 1

    logging.info('features done')
    return new_df

#adding a new aggregation table
def create_aggregations (df):
    logging.info("aggregating df")
    new_df = df.copy().groupby('symbol').agg({
        'volume' : ['sum', 'mean', 'std'],
        'close' : ['mean', 'min', 'max', 'std'],
        'log_return' : ['mean', 'std']
    }).reset_index()

    new_df.columns = ['symbol', 'total_volume', 'volume_mean', 'volume_std', 'price_mean', 'min_price', 'max_price', 'price_std', 'return_mean', 'return_std']
    logging.info("aggregation done")
    return new_df
