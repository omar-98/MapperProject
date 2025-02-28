from pyspark.sql.types import StructType, StructField,StringType,BooleanType,IntegerType

output_schema = StructType([
    StructField("ID_TEC_XPN",StringType(),True),
    StructField("ID_VALUE",IntegerType(),True),
    StructField("ID_FLAG",BooleanType(),True)
])
