def fn_file_exists(path):
  try:
    dbutils.fs.ls(path)
    return 1
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return 0
    else:
      raise