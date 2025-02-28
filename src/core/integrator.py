from abc import ABC
#configure logging
import logging

from pyspark.sql import DataFrame

from src.core.dataframe_cache import DataFrameCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Integrator(ABC):
    def __init__(self,spark,dataframe_cache:DataFrameCache):
        self.dataframe_cache=dataframe_cache
        self.spark=spark
        self.start_dataframe: DataFrame = None

    def _build_dependencies(self):
        pass
    def _load_dataframes(self):
        pass
    def _map_dataframes(self):
        pass
    def _save_dataframes(self):
        pass

    def pipline(self):
        self._build_dependencies()
        self._load_dataframes()
        self._map_dataframes()
        self._save_dataframes()

