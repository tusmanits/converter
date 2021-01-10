import logging

class Logger:
    def __init__(self):
        pass
        

    def getLogger(self):
        logger = logging
        logger.basicConfig(filename='../logs/converter.log',level=logging.INFO, filemode='a', format = '%(asctime)s:%(levelname)s:%(message)s')
        return logger


