from typing import Dict

from pyspark.sql import SparkSession,DataFrame

import logging
class DataFrameCache:
    _cache:Dict[str,DataFrame]
    def __init__(self,spark:SparkSession):
        self.spark = spark
    def get_dataframe(self,key:str=None,schema=None,path:str="C:\\Users\\Administrateur\\Documents\\formation\\pythonProject3\\test\\data\\exemple.json")-> DataFrame:
        return self.spark.read.json(path)
    def get_dataframe_catalog(self,key:str,table:str)-> DataFrame:
        pass
    def apply_filter(self,key:str,filter_condition:str):
        pass
    def repartition_dataframe(self,key:str,num_partition:int):
        if key in self._cache:
            logging.info(f"Repartition Dataframe {key} to {num_partition} partitions")
            return self._cache[key].repartition(num_partition)
        logging.warning(f"DataFrame {key} not found in cache.")
        return None

