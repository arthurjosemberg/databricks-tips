from pyspark.sql.functions import *
from pyspark.sql.window import Window

# %run /general/functions/fnc_remove_especial_char

def fn_replicate_ibge_ddd(dataframe,cpf_column,list_of_ddd_columns,list_of_phone_columns,city_column,uf_column):

  array_of_ddd_columns = []
  array_of_ddd_columns = list_of_ddd_columns.split(",")
  array_of_phone_columns = []
  array_of_phone_columns = list_of_phone_columns.split(",")

# Le o de-para de ddds - dados provenientes do IBGE
  de_para_ddds_ibge = (
    spark
      .read
      .format('csv')
      .option('inferSchema','true')
      .option('header','true')
      .option('sep',';')
      .option("encoding","ISO-8859-1")
      .load('/mnt/raw/marketing/de_para/ddds_ibge.csv')
  )

# Transforma o dataframe em UTF-8
  array = []
  for row in de_para_ddds_ibge.collect():
    columns_array = [row['uf'],row['municipio'],str(row['codigo_nacional'])]
    array.append(columns_array)
    
  for row in array:
    row[1] = fnc_remove_especial_char(row[1])
    
  de_para_ddds_ibge = (sc.parallelize(array).toDF(['uf_ibge','cidade_ibge','ddd_ibge']))
  
  # Cria um id unico por cpf para cada registo no dataframe
  partitionBy = (
    Window
      .partitionBy(cpf_column)
      .orderBy(cpf_column)
  )
  
  dataframe = (
      dataframe
        .withColumn('cpf_id', row_number().over(partitionBy))
    )
  
  for index in range(len(array_of_ddd_columns)):
    ddd_column = array_of_ddd_columns[index]
    ddd_column_temp = ddd_column + '_temp'
    telefone_column = array_of_phone_columns[index]
      
    join_clause = (dataframe[city_column] == de_para_ddds_ibge['cidade_ibge']) & (dataframe[uf_column] == de_para_ddds_ibge['uf_ibge']) 
    
    temp_df = (
      dataframe
        .join(de_para_ddds_ibge,join_clause,'left')
        .where(col('cidade_ibge').isNotNull())
        .select(
          col('cidade_ibge'),
          col('uf_ibge'),
          col(ddd_column).alias(ddd_column_temp),
          col('ddd_ibge'),
          col(cpf_column),
          col('cpf_id'))
        .distinct()
    )
    
    dataframe = dataframe.drop(ddd_column)
    
    join_clause = (dataframe[cpf_column] == temp_df[cpf_column]) & (dataframe['cpf_id'] == temp_df['cpf_id'])
    
    dataframe = (
      dataframe
        .join(temp_df,join_clause,'left')
        .select (
          dataframe["*"],
           when(col(telefone_column) == '', '')
          .when(col(ddd_column_temp) == '', col('ddd_ibge'))
            .otherwise(col(ddd_column_temp)).alias(ddd_column)
        )
    )
    
  dataframe = dataframe.drop('cpf_id')

  return dataframe