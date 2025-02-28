from pyspark.sql import DataFrame

from src.core.dataframe_cache import DataFrameCache
from src.core.integrator import Integrator
from src.mapping.output_schema import output_schema
from src.mapping.core import Mapper


class EXIntegrator(Integrator):
    def __init__(self,spark,dataframe_cache:DataFrameCache)-> None:
        self.spark = spark
        self.dataframe_cache=dataframe_cache
        self.mapper =Mapper(self.spark,output_schema)

    def _build_dependencies(self):
        pass
    def _load_dataframes(self):
        self.start_dataframe = self.dataframe_cache.get_dataframe()
    def _map_dataframes(self):
        self.final_dataframe = self.mapper(self.start_dataframe)
    def _save_dataframes(self):
        pass
