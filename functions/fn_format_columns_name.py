import databricks.koalas as ks
import pandas as pd
import re

def fn_get_column_names (dataframe):
  """
  Receive a dataframe and return a list with all dataframe columns name.
  
  Args:
    dataframe: dataframe in PyPark, Koalas or Pandas.

  Returns:
    A list with all dataframe columns name.
  """
  columns_list = []
  columns_list = dataframe.columns
  return columns_list

def fn_format_columns_name (dataframe):
  """
  Receive a dataframe (PySpark, Koalas or Pandas) and return another dataframe 
  with the same type of the input one with formated columns name.
  
  Args:
    dataframe: dataframe in PyPark, Koalas or Pandas.

  Returns:
    A dataframe with formated columns name.
  """
  column_names = fn_get_column_names(dataframe)
  special_characters = [['[áãàâäåæ]','[éêèęėēë]','[íîìïįī]','[óõôòöœøō]','[úüùûū]','[ç]','[ñ]','[/\-\n\r.,]','[(){};:º*&^%$#@!+=]'],
                        ['a','e','i','o','u','c','n','_','']]
  amount_of_types = len(special_characters[0])
  
  if isinstance(dataframe, (pd.DataFrame,ks.DataFrame)):  
    
    for column in column_names:
      column_name = column
    
      for index_regex in range(amount_of_types):
        column_name = re.sub(special_characters[0][index_regex], special_characters[1][index_regex], column_name) 
      dataframe.rename(columns={column:column_name}, inplace=True)
    
    dataframe.columns = dataframe.columns.str.replace(' ', '_')
    dataframe.columns = dataframe.columns.str.lower()
  
  else:
    
    for column in column_names:
      column_name = column
      column_name = column_name.lower().replace(' ','_')
      
      for index_regex in range(amount_of_types):
        column_name = re.sub(special_characters[0][index_regex], special_characters[1][index_regex], column_name) 
      dataframe = dataframe.withColumnRenamed(column,column_name)
  
  return dataframe