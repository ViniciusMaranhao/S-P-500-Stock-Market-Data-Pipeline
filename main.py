from pipeline import extract as ex
from pipeline import transform as tf
from pipeline import load as ld
import sys
import logging
from logging_config import logging_setup as lgs

#declaring logging configs
lgs()

#preparing data
def build_dataset():
   
    raw_df = ex.extract()
    logging.info('extract done')
    
    concatenated_df = tf.concatenate(raw_df)
    logging.info('concatenation done')
    
    clean_df = tf.clean_data(concatenated_df)
    logging.info('cleaning done')
    
    df = tf.create_features(clean_df)
    logging.info('feature adding done')
    
    return df

#load clean df
def run_clean_data ():
    
    logging.info('starting clean function')
    
    df = build_dataset()
    
    ld.load_clean_data(df)
    logging.info('load done')
    
    logging.info('end of clean function')
    print()
    

#load agg df
def run_agg_data ():
    
    logging.info('starting agg function')
    df = build_dataset()
    
    df = tf.create_aggregations(df)
    logging.info('agg done')

    ld.load_aggregated_data(df)
    logging.info('load done')
    
    logging.info('end of agg function')
    print()
            
#argv commands
if __name__=='__main__':
    # if the user types only python main.py (it does both functions)
    if (len(sys.argv) == 1):
            df = build_dataset()
            
            logging.info('starting clean function')
            ld.load_clean_data(df)
            logging.info('load done')
            logging.info('end of clean function')
            print()

            logging.info('starting agg function')
            ld.load_aggregated_data(tf.create_aggregations(df))
            logging.info('load done')
            logging.info('end of agg function')
            print()
    
    #if user types an second argv
    else:
        command = sys.argv[1].lower()

        # python main.py clean
        if (command == 'clean'):
            run_clean_data()

        # python main.py agg
        elif (command == 'agg'):
            run_agg_data()

        # python main.py all
        elif (command == 'all'):
            df = build_dataset()
            
            logging.info('starting clean function')
            ld.load_clean_data(df)
            logging.info('load done')
            logging.info('end of clean function')
            print()

            logging.info('starting agg function')
            ld.load_aggregated_data(tf.create_aggregations(df))
            logging.info('load done')
            logging.info('end of agg function')
            print()

        # python main.py incorrect_input
        else:
            print('''invalid command, here is the available commands:
                  python main.py (do both functions)
                  python main.py clean (do the clean function)
                  python main.py agg (do the aggregated function)
                  python main.py all (do both functions)''')
