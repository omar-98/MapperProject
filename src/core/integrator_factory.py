#configure logging
import logging

from pyspark.sql import SparkSession

from src.core.dataframe_cache import DataFrameCache
from src.core.integrator import Integrator
from src.integrators.integrator_x import EXIntegrator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class IntegratorFactory:
    """
    A Factory class to create different types of Integrators
    """
    def __init__(self, spark:SparkSession):
        self.dataframe_cache = DataFrameCache(spark)
        self.spark = spark
    def create_integrator(self,integrator_type: str) ->Integrator:
        if integrator_type == "EX":
            return EXIntegrator(spark=self.spark,dataframe_cache=self.dataframe_cache)
        else:
            raise ValueError(f"Unknown Integrator type: {integrator_type}")


