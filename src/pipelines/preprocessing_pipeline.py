import sys

from src.config import data_paths
from src.data_ingestion import DataLoader,DataMerger
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

#Data Transformer initialization
try:
    data_merger_obj = DataMerger(input_data)
    logger.info("DataMerger obj initialized successfully!")
except Exception as e:
    logger.info("DataMerger failed!")
    raise M5Exception(str(e),sys) from None

#merge raw datasets
try:
    joined_data = data_merger_obj.merge_raw_datasets()
    logger.info("data_merger_obj.merge_raw_datasets ran successfully!")
except Exception as e:
    logger.info("data_merger_obj.merge_raw_datasets run failed!")
    raise M5Exception(str(e),sys) from None

print(joined_data)