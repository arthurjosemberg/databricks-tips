from pyspark.sql.types import *

def fn_equivalent_type(datatype):
  """
  Choose the PySpark datatype that is equivalent to Pandas datatype.
  
  Args:
    datatype: Parameter with string value with Pandas datatype.

  Returns:
    The PySpark datatype that is equivalent to Pandas datatype passed.
  """  
  choices = {
    'datetime64[ns]': TimestampType(), 
    'int64': LongType(),
    'int32': IntegerType(),
    'float64': FloatType()
  }
  return choices.get(datatype, StringType())

def fn_define_structure(string, format_type):
  """
  Receive a column name and Pandas datatype and return the a Struct datatype 
  with the column name, the PySpark datatype and the nullable parameter with 
  value TRUE.

  Args: 
    string: Parameter that will pass the column name.
    format_type: Parameter that will pass the Pandas datatype.

  Returns:
    The PySpark struct datatype with the column name and a datatype with 
    PySpark correspondent value.
  """
  try: typo = fn_equivalent_type(format_type)
  except: typo = StringType()
  return StructField(string, typo)

def fn_pandas_to_spark(pandas_df):
  """
  Recive a pandas dataframe and return a schema to be used to create a 
  PySpark dataframe.

  Args:
    pandas_df: a Pandas dataframe that will be converted. 

  Returns:
    A pySpark dataframe with datatypes that correspond correctly to Pandas
    dataframe that was passed.
  """
  columns = list(pandas_df.columns)
  types = list(pandas_df.dtypes)
  struct_list = []
  for column, typo in zip(columns, types): 
    struct_list.append(fn_define_structure(column, str(typo)))
    p_schema = StructType(struct_list)
    spark_df = spark.createDataFrame(pandas_df, p_schema)
  return spark_df