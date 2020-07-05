from pyspark.sql.functions import *
from datetime import *

def fn_read_incremental_data (directory_name, path_of_directory):
  
  # Create a database and table to control data load
  spark.sql("""
      CREATE DATABASE IF NOT EXISTS load_control
    """)
  spark.sql("""
      CREATE TABLE IF NOT EXISTS load_control.periodicity_control (
        table_name VARCHAR(100),
        last_date_inserted VARCHAR(8),
        amount_inserted_on_last_loud INT
      )
      USING delta
      LOCATION "/mnt/silver/corporativo/load_control/periodicity_control"
    """)
  
  # Create the start_date variable 
  periodicity_control = spark.read.format("delta").load("/mnt/silver/corporativo/load_control/periodicity_control/")
  start_date = periodicity_control.filter(col('table_name') == directory_name).select(col('last_date_inserted'))
  if len(start_date.head(1)) == 0:
    start_date = '19000101'
  else:
    start_date = [str(row['last_date_inserted']) for row in start_date.collect()][0]

  # Create the variables with year, month and day information
  start_year = int(start_date[0:4])
  start_month = int(start_date[4:6])
  start_day = int(start_date[6:8])

  # Create the delta_date variable
  delta_date = date.today() + timedelta(days=15)
  delta_date = delta_date.strftime("%Y%m%d")
  
  # Create the variables with year, month and day information with delta date (D-1)
  delta_year = int(delta_date[0:4])
  delta_month = int(delta_date[4:6])
  delta_day = int(delta_date[6:8])

  # Create array with range of all dates to be loaded
  s_date = date(start_year, start_month, start_day)
  e_date = date(delta_year, delta_month, delta_day)
  delta = e_date - s_date

  all_dates = []

  for index in range(delta.days + 1):
    loop_date = s_date + timedelta(days=index)
    all_dates.append(loop_date.strftime("%Y%m%d"))
  
  # Create string with all dates separated by comman to be used in path
  string_with_dates = ""
  amount_of_dates = len(all_dates)
  
  for index in range(amount_of_dates):
    if index+1 < amount_of_dates:
      string_with_dates = string_with_dates + (str(all_dates[index]) + ",")
    else:
      string_with_dates = string_with_dates + (str(all_dates[index]))
  
  directory = path_of_directory + directory_name + "/{" + string_with_dates + "}/{*}"

  try:
    dataframe = (spark.read.parquet(directory))
    return dataframe
  except:
    if 'org.apache.spark.sql.AnalysisException':
      return print("Diretório(s) não existe(m).")
    else:
      return print("Ocorreu um erro.")