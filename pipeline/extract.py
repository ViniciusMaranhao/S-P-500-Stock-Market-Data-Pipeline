import pandas as pd
from pathlib import Path
import logging

def extract():
    #dataframe list
    dataframes = []

    logging.info('extracting df')

    #scan raw-data folder  
    folder = Path(__file__).resolve().parent.parent / 'raw_data'

    #detect csv files and add them in a list
    for file in folder.glob('*.csv'):
        
        logging.info(f'extracting file: {file.name}')
        
        #adding a symbol column to indentfy the file (i know the file already has a name column, this is just for learning)
        current_file = pd.read_csv(file)
        parts = file.name.split('_')
        symbol = parts[0]
        current_file['symbol'] = symbol

        #adding it in the df list
        dataframes.append(current_file)
    
    logging.info("extraction done")
    return dataframes