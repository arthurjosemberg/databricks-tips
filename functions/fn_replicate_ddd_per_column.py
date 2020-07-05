from pyspark.sql.functions import *
from pyspark.sql.window import Window

def fn_replicate_ddd_per_column(dataframe,cpf_column,list_of_ddd_columns,list_of_phone_columns):
  array_of_ddd_columns = []
  
  array_of_ddd_columns = list_of_ddd_columns.split(",")
  array_of_phone_columns = []
  array_of_phone_columns = list_of_phone_columns.split(",")

# Cria um id unico por cpf para cada registo no dataframe
  partitionBy = (
    Window
      .partitionBy(cpf_column)
      .orderBy(cpf_column)
  )

  dataframe = (
      dataframe
        .withColumn("cpf_id", row_number().over(partitionBy))
    )

# Cria o array de forma dinamica com as informações + todas as colunas de DDD
  array = []
  row_id = 0
 
  for row in dataframe.collect():
    row_id += 1
    for ddd_index in range(len(array_of_ddd_columns)):
      if ddd_index == 0:
        columns_array = [row[cpf_column],row["cpf_id"],row[(array_of_ddd_columns[ddd_index])]]
        array.append(columns_array)
      else:
        ddd_column = [row[(array_of_ddd_columns[ddd_index])]]
        array[row_id-1].extend(ddd_column)
          
# Replica o primeiro DDD encontrado entre as demais colunas de DDD com valor vazio
  for row in array:
    ddd = ''
    ddds_blank = []
    for index in range(len(row)):
      if index >= 2:
        if row[index] != '':
          ddd = row[index]
        else:
          ddds_blank.append(index)
    for ddd_id in ddds_blank:
      row[ddd_id] = ddd

# Cria um dataframe com as novas colunas de DDD 
  array_of_columns = []
  array_of_columns.append(cpf_column + '_2')
  array_of_columns.append('cpf_id_2')
  for ddd_column in array_of_ddd_columns:
    array_of_columns.append(ddd_column)
    
  df = (sc.parallelize(array).toDF(array_of_columns))

# Deleta do dataframe original as colunas de DDD
  for ddd_column in array_of_ddd_columns:
    dataframe = (
      dataframe
        .drop(ddd_column)
      )
    
  join_clause = (dataframe[cpf_column] == df[cpf_column + '_2']) & (dataframe['cpf_id'] == df['cpf_id_2'])
  
# insere no dataframe original as colunas de DDD tratadas
  dataframe = (
    dataframe
      .join(df,join_clause,'inner')
      .drop(cpf_column + '_2','cpf_id','cpf_id_2')
  )
  
  return dataframe