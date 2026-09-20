import logging
import inspect
from src.config import log_file_name

#set class/method name from where it is called
logger_name = inspect.currentframe().f_back.f_code.co_name

#create logger
logger = logging.getLogger(logger_name)

logger.setLevel(logging.DEBUG)

#create file handler
file_handler = logging.FileHandler(log_file_name)

#create the formater
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s -%(name)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

#add formatter to file_handler
file_handler.setFormatter(log_formatter)

#add file_handler to logger
logger.addHandler(file_handler)
                    


