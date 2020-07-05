from pyspark.sql.functions import *

def fn_treating_cnpj (dataframe,cnpj_column,with_mask='n'):
  cnpj_column_new = cnpj_column + '_treated'
  array = []
  
  for row in dataframe.collect():
    columns_array = [row[cnpj_column],row[cnpj_column]]
    array.append(columns_array)
    
  for row in array:
    qtde_zeros = 14 - len(str(row[1]).strip())
    zeros = '0' * qtde_zeros
    row[1] = zeros + str(row[1])
    
  if with_mask in ['y','Y']:
    for row in array:
      row[1] = row[1][0:2] + '.' + row[1][2:5] + '.' + row[1][5:8] + '/' + row[1][8:12] + '-' + row[1][12:14]
    
  df = sc.parallelize(array).toDF([cnpj_column,cnpj_column_new])
  
  dataframe = (
    dataframe
      .join(df,cnpj_column,'inner')
  )
  
  return dataframe