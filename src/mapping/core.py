from pyspark.sql.functions import when, lit, col
from src.mapper.base import ClassMapper, management_rule_dependency

class Mapper(ClassMapper):
    def __init__(self, spark,schema):
        self.spark = spark

        super().__init__(schema)



    def ID_TEC_XPN(self):
        return col("id")

    def ID_TEC_XPN(self):
        return col("id")

    def ID_VALUE(self):
        return col("value")

    @management_rule_dependency(["ID_VALUE"])
    def ID_FLAG(self):
        return when(self.column("ID_VALUE") < 100, col("enabled")).otherwise(None)











