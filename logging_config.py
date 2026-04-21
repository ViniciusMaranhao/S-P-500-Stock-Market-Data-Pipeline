import logging as lg

def logging_setup():
    lg.basicConfig(
        filename = 'log_files/pipeline.log',
        level = lg.INFO,
        format = '%(asctime)s - %(levelname)s - %(message)s'
    )