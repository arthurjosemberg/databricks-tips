from pyspark.sql.functions import *
from pyspark.sql.window import Window

def fn_replicate_ddd_per_row (dataframe,cpf_column,list_of_ddd_columns,list_of_phone_columns):
  array_of_ddd_columns = []
  array_of_ddd_columns = list_of_ddd_columns.split(",")
  array_of_phone_columns = []
  array_of_phone_columns = list_of_phone_columns.split(",")
  
  for index in range(len(array_of_ddd_columns)):
    ddd_column = array_of_ddd_columns[index]
    telefone_column = array_of_phone_columns[index]
    
    temp_df = (
      dataframe
        .select(
          col(cpf_column),
          col(ddd_column),
           when(col(ddd_column) != '','1')
          .when(col(ddd_column) == '','0')
          .alias('possui_ddd')
        )
    )
    
    partitionBy = (
      Window
        .partitionBy(cpf_column)
        .orderBy(asc(cpf_column),desc("possui_ddd"))
    )
    
    temp_df = (
      temp_df
        .select(
            col(cpf_column),
            col(ddd_column),
            row_number().over(partitionBy).alias("id_ddd"))
        .filter(col('id_ddd') == 1)
    )
    
    columns = []
    columns = dataframe.columns
    
    dataframe = (
      dataframe
        .drop(ddd_column)
    )
    
    dataframe = (
      dataframe
        .join(temp_df,cpf_column,'inner')
        .drop(col('id_ddd'))
    )
    
  return dataframe