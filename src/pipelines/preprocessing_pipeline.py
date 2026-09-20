import sys

from src.config import data_paths
from src.data_ingestion import DataLoader
from src.logger import logger
from src.exception import M5Exception


#DataLoader initialization
try:
    data_loader_obj = DataLoader(data_paths)
    logger.info("DataLoader initialized successfully!")
except Exception as e:
    logger.info("DataLoader failed!")
    raise M5Exception(str(e),sys) from None

#get input raw datasets
try:
    input_data = data_loader_obj.get_input_data()
    logger.info("data_loader_obj.get_input_data ran successfully!")
except Exception as e:
    logger.info("data_loader_obj.get_input_data run failed!")
    raise M5Exception(str(e),sys) from None


