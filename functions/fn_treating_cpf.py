from pyspark.sql.functions import *

def fn_treating_cpf (dataframe,cpf_column,with_mask='n'):
  cpf_column_new = cpf_column + '_treated'
  array = []
  
  for row in dataframe.collect():
    columns_array = [row[cpf_column],row[cpf_column]]
    array.append(columns_array)
    
  for row in array:
    qtde_zeros = 11 - len(str(row[1]).strip())
    zeros = '0' * qtde_zeros
    row[1] = zeros + str(row[1])
    
  if with_mask in ['y','Y']:
    for row in array:
      row[1] = row[1][0:3] + '.' + row[1][3:6] + '.' + row[1][6:9] + '-' + row[1][9:11]
    
  df = sc.parallelize(array).toDF([cpf_column,cpf_column_new])
  
  dataframe = (
    dataframe
      .join(df,cpf_column,'inner')
  )
  
  return dataframe