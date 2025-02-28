import unittest

from pyspark.sql import SparkSession

from src.core.integrator_factory import IntegratorFactory


class IntegratorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark : SparkSession = (SparkSession.builder.master("local")
                                    .getOrCreate())
 #   def tearDownClass(cls):
  #      cls.spark.stop()
    def test_integrator(cls):
        factory =IntegratorFactory(cls.spark)
        integrator = factory.create_integrator("EX")
        integrator.pipline()
        #integrator.start_dataframe.show()
        integrator.final_dataframe.show()

