def fn_export_csv_file (
  dataframe,
  temp_directory,
  final_directory,
  file_name,
  file_format,
  save_mode = 'overwrite',
  file_enconding = 'UTF-8',
  file_header = 'true',
  file_separator = ',',
  file_quote = '\"',
  file_date_format = 'yyyy-MM-dd HH:mm:ss.S'
):
  
  if final_directory[len(final_directory)-1] != '/':
    final_directory + '/'
    
  if temp_directory[len(temp_directory)-1] != '/':
    temp_directory + '/'
    
  file_path = final_directory + file_name
  
  (
    dataframe
      .coalesce(1)
      .write
      .mode(save_mode)
      .format(file_format)
      .option("header",file_header)
      .option("encoding",file_enconding)
      .option("sep",file_separator)
      .option("quote",file_quote)
      .option("dateFormat",file_date_format)
      .save(temp_directory)
  )
  
  for file in dbutils.fs.ls(temp_directory):
    if file.name[0:5] == 'part-':
      original_file_name = file.name
      
  original_file_local = "dbfs:" + temp_directory + original_file_name
  
  dbutils.fs.mv(original_file_local, file_path)
  
  return print('The file with name: ' + file_name + ' was saved on the follow path: ' + final_directory + '.')