from pyspark.sql.functions import *
import re

def fn_treating_email (dataframe,id_column,email_column):

# Define as variáveis que serão usadas
  array = []
  provedores = []
  provedores = [ 
    ['gmail','hotmail','bol','msn','ig.com','globomail','icatuseguros','sicredi','banrisul'],
    ['@gmail.com', '@hotmail.com', '@bol.com.br', '@msn.com', '@ig.com.br', '@globomail.com', '@icatuseguros.com.br', '@sicredi.com.br', '@banrisul.com.br']
  ]
  wrong_mail = ['naotem', 'naopossui', 'seminformacao', 'clientenaopossuiemail','nao.possui','clientenaopossui','naopossuiemail','naoposui','naopossue','x','não tem','nt.','nt']
  columns = []
  columns = dataframe.columns
  columns.append(email_column + '_treated')

# transforma os dados advindos das colunas de id e de email em um array 
  for row in dataframe.collect():
    columns_array = [row[id_column],(row[email_column]).lower().strip().replace(' ','')]
    array.append(columns_array)
    
# Realiza o tratamento dos registros de email com provedor conhecido
  for index in range(len(provedores[0])):
    if (row[1]).find(provedores[0][index]) != -1:
      position = (row[1]).find(provedores[0][index])
      start_email = (row[1][0:position]).replace('@','')
      row[1] = start_email + provedores[1][index]
      
  # Elimina os registros de emails que não possuem "@"
  for row in array:
    if (row[1]).find('@') == -1:
      row[1] = ''
      
  # Substitui ',' (virgula) por '.' (ponto) nos registros
  for row in array:
    if (row[1]).find(',') != -1:
      row[1] = row[1].replace(',','.')

# Elimina os registros de emails que não possuem "."
  for row in array:
    if (row[1]).find('.') == -1:
      row[1] = ''
  
# Elimina os registros de emails que possuirem os valores identificados como errados
  for index in range(len(wrong_mail)):
    for row in array:
      string_treated = (fnc_remove_especial_char(row[1])).lower()
      if string_treated.find(wrong_mail[index]) != -1:
        row[1] = ''

# Transforma o array com os dados tratados em um dataframe
  df = sc.parallelize(array).toDF([id_column,email_column + '_treated'])

# Realiza o join do dataframe criado com os dados tratados com o dataframe original
  dataframe = (
    dataframe
      .join(df,id_column,'inner')
      .select(columns)
  )
  
  return dataframe