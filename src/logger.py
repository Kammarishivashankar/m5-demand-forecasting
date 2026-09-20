import logging
import inspect
from src.config import log_file_name

#create logger
logger = logging.getLogger("M5-pipeline")

logger.setLevel(logging.DEBUG)

#create file handler
file_handler = logging.FileHandler(log_file_name)

#create the formater
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
                                        datefmt="%m/%d/%Y %I:%M:%S %p")

#add formatter to file_handler
file_handler.setFormatter(log_formatter)

#add file_handler to logger
logger.addHandler(file_handler)
