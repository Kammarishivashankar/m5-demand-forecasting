from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.functions as f
import sys

from src.config import data_paths,filter_map
from src.utils import read_data, write_data
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
        

class Preprocessor:
    def __init__(self, joined_data: DataFrame, filter_map: dict)->DataFrame:
        self.joined_data = joined_data
        self.filter_map = filter_map

    def normalize_event_name(self, event):
        return (
            event.lower()
            .replace(" ", "_")
            .replace("'", "")
            .replace("-", "_")
        )

    def encode_events(self,joined_data):    
        event1_values = [r["event_name_1"] for r in joined_data.select("event_name_1").distinct().collect() if r["event_name_1"] is not None]
        event2_values = [r["event_name_2"] for r in joined_data.select("event_name_2").distinct().collect() if r["event_name_2"] is not None]

        all_events = set(event1_values) | set(event2_values)

        event_exprs = {}
        for event in all_events:
            normalized_event = self.normalize_event_name(event)
            event_exprs[f"f_event_{normalized_event}"] = F.when(
                (F.col("event_name_1") == event) |
                (F.col("event_name_2") == event),
                1
            ).otherwise(0)

        joined_data_events_encoded = joined_data.withColumns(event_exprs)
        return joined_data_events_encoded

    def agg_joined_data(self,joined_data):
        """
        Aggregates daily M5 data to the wm_yr_wk weekly grain.
        """
        group_cols = self.filter_map['sales_id_cols'] + ['wm_yr_wk']
        
        event_cols = [c for c in joined_data.columns if c.startswith('f_event_')]
        
        event_aggs = [F.max(c).alias(c) for c in event_cols]

        self.weekly_data = joined_data.groupby(*group_cols).agg(
            F.sum('sales').alias('weekly_sales'),
            # Price is static per item per wm_yr_wk in M5, so first() is used
            F.first('sell_price').alias('sell_price'), 
            # Sum the SNAP flags to get the count of SNAP days in that week (0 to 7)
            F.sum(F.col('snap_CA').cast('int')).alias('snap_CA_days'),
            F.sum(F.col('snap_TX').cast('int')).alias('snap_TX_days'),
            F.sum(F.col('snap_WI').cast('int')).alias('snap_WI_days'),
            *event_aggs
        )
        return self.weekly_data

    
    def process(self):
        joined_data_events_encoded = self.encode_events(self.joined_data)
        weekly_data = self.agg_joined_data(joined_data_events_encoded)
        write_data(weekly_data,data_paths['bronze']['processed_data']['joined_data'],['state_id'])
        return weekly_data
  
