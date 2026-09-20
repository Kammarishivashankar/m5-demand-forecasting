from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.functions as f
import sys

from src.config import data_paths,filter_map
from src.utils import read_data
from src.logger import logger
from src.exception import M5Exception


class DataLoader:
    """class to load the raw datasets
        returns the loaded datasets as dictionary of pyspark dataframes"""
    
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
    

class DataMerger:
    """Class to perform the initial transformation required on the raw datasets.
        returns tranaformed raw datasets"""
    
    def __init__(self,input_data:dict[DataFrame]):
        self.input_data = input_data

    def merge_raw_datasets(self):
        """unpivots the wide sales dataframe
            joins calendar and price datasets with unpivoted sales data
            returns complete dataset with sales, events, selling_price"""

        self.sales_long = self.input_data['sales_wide']\
                                                .unpivot(ids=filter_map['sales_id_cols'],
                                                    values = [c for c in self.input_data['sales_wide'].columns if c.startswith('d_')],
                                                    variableColumnName = "d",
                                                    valueColumnName = "sales")
        self.sales_long = self.sales_long.withColumn('id',F.regexp_replace(F.col('id'),'_evaluation',""))

        #read calendar data and convert the data column to date datatype
        self.calendar = self.input_data['calendar_raw']\
                                        .withColumn('date',F.to_date('date',format="yyyy-MM-dd"))

    
        self.price = self.input_data['price_raw']

        self.joined_data = self.sales_long.join(self.calendar,on=['d'],how='left')\
                                            .join(self.price,on=['wm_yr_wk','store_id','item_id'],how='left')
        
        return self.sales_long,self.calendar,self.price,self.joined_data
        


