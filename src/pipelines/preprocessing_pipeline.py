import sys
from src.config import data_paths, filter_map
from src.data_ingestion import DataLoader,DataMerger,Preprocessor
from src.logger import logger
from src.exception import M5Exception


logger.info("=============== | M5 Preprocessing Pipeline Started! | ==================")
#DataLoader initialization
try:
    data_loader_obj = DataLoader(data_paths)
    logger.info("DataLoader initialized successfully!")
except Exception as e:
    logger.exception("DataLoader failed!")
    raise M5Exception(str(e),sys) from None


#get input raw datasets
try:
    input_data = data_loader_obj.get_input_data()
    logger.info("data_loader_obj.get_input_data ran successfully!")
except Exception as e:
    logger.exception("data_loader_obj.get_input_data run failed!")
    raise M5Exception(str(e),sys) from None

#DataMerge initialization
try:
    data_merger_obj = DataMerger(input_data)
    logger.info("DataMerger obj initialized successfully!")
except Exception as e:
    logger.exception("DataMerger failed!")
    raise M5Exception(str(e),sys) from None

#merge raw datasets
try:
    sales_long, calendar,price,joined_data = data_merger_obj.merge_raw_datasets()
    logger.info("data_merger_obj.merge_raw_datasets ran successfully!")
except Exception as e:
    logger.exception("data_merger_obj.merge_raw_datasets run failed!")
    raise M5Exception(str(e),sys) from None

#Preprocessor initialization
try:
    preprocessor_obj = Preprocessor(joined_data,filter_map)
    logger.info("Preprocessor obj initialized successfully!")
except Exception as e:
    logger.exception("Preprocessor failed!")
    raise M5Exception(str(e),sys) from None


#proprecessing to weekly data
try:
    weekly_sales_data = preprocessor_obj.process()
    logger.info("preprocessor_obj.process() successful!")
except Exception as e:
    logger.exception("preprocessor_obj.process() failed!")
    raise M5Exception(str(e),sys) from None

logger.info("=============== | M5 Preprocessing Pipeline completed successfully! | ==================")
print(weekly_sales_data)

