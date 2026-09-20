from pyspark.sql import DataFrame
import sys

from src.config import data_paths
from src.utils import read_data
from src.logger import logger
from src.exception import M5Exception


class DataLoader:
    def __init__(self,data_paths:dict[dict]):
        self.data_paths = data_paths

    def get_input_data(self)->DataFrame:
        """takes data paths dictionary and returns the input raw datasets"""
        try:
            sales_wide = read_data(self.data_paths['bronze']['raw_paths']['sales'],format="csv")
            logger.info('sales wide data reading completed!')
        except Exception as e:
            logger.info('sales wide data reading failed!')
            raise M5Exception(str(e),sys) from None

        try:
            calendar = read_data(self.data_paths['bronze']['raw_paths']['calendar'],format="csv")
            logger.info('calendar data reading completed!')
        except Exception as e:
            logger.info('calendar data reading failed!')
            raise M5Exception(str(e),sys) from None

        try:
            price = read_data(self.data_paths['bronze']['raw_paths']['price'],format="csv")
            logger.info('price data reading completed!')
        except Exception as e:
            logger.info('price data reading failed!')
            raise M5Exception(str(e),sys) from None

        return {'sales_wide' : sales_wide,
                'calendar_raw' : calendar,
                'price_raw' : price}
    


