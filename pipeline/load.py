import pandas as pd
from pathlib import Path
import logging

#saves the cleaned df with features
def load_clean_data (df):    
    logging.info('loading clean df')
    df.to_csv(Path(__file__).resolve().parent.parent / 'processed_data' / 'clean_data.csv', index=False)
    logging.info("loading done")

#saves the aggregated df
def load_aggregated_data (df):
    logging.info("loading aggregated df")
    df.to_csv(Path(__file__).resolve().parent.parent / 'processed_data' / 'agg_data.csv', index=False)
    logging.info("load done")
