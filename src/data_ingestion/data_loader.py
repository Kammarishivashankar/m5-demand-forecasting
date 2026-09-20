from pyspark.sql import DataFrame
from src.config import data_paths
from src.utils import read_data
from 

class DataLoader:
    def __init__(self,data_paths:dict[dict]):
        self.data_paths = data_paths

    def get_input_data(self)->DataFrame:
        """takes data paths dictionary and returns the input raw datasets"""
        try:
            sales_wide = read_data(self.data_paths['bronze']['raw_paths']['sales'])
