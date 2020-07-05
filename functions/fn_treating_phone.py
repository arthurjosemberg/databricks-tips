from pyspark.sql.functions import *
import numpy as np
import re

def fn_treating_phone (dataframe,id_column,list_of_phone_columns):
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
  
  # Cria o array com todos os DDDs do Brasil para ser usado na Função
  all_ddds_from_br = []
  
  ddds_ibge = (
    de_para_ddds_ibge.select(col('codigo_nacional').cast("string")).distinct()
  )
  
  for row in ddds_ibge.collect():
    all_ddds_from_br.append(row.codigo_nacional)
    break

  for index in range(len(array_of_phone_columns)):
    columns = []
    columns = dataframe.columns
      
    phone_column_name = 'telefone_' + str(index+1) + '_treated'
    ddd_column_name = 'ddd_' + str(index+1) + '_treated'
    columns.append(phone_column_name)
    columns.append(ddd_column_name)
    array = []
      
# Cria o array e retira os espacos em brando do começo e fim da string de telefone
    for row in dataframe.collect():
      columns_array = [row[id_column],row[array_of_phone_columns[index]].strip()]
      array.append(columns_array)
      
# Retira os caracteres especiais e espaços em branco do telefone
    for row in array:
      remove_special_caract_from_tel = row[1]
      remove_special_caract_from_tel = re.sub('[^A-Za-z0-9]+', '', remove_special_caract_from_tel)
      row[1] = remove_special_caract_from_tel.replace(' ','')
  
# Remove os caracteres alfanumericos do telefone.
    for row in array:
      remove_row_with_string = row[1]
      if remove_row_with_string.isdigit() == False:
        tel = row[1]
        tel_correct = ''
        for character in tel:
          if character.isdigit() == True:
            tel_correct = tel_correct + character
        row[1] = tel_correct
          
# Remove os telefones com todos os números repetidos
    for row in array:
      find_repeted_number = row[1]
      if (find_repeted_number + find_repeted_number).find(find_repeted_number,1,-1) == 1:
        row[1] = ''
          
# Remove o "0" do inicio dos telefone
    for row in array:
      remove_zero_from_start = row[1]
      if (len(remove_zero_from_start) >= 8):
        if (remove_zero_from_start[0] == '0'):
          row[1] = remove_zero_from_start[1:len(remove_zero_from_start)]
            
# Remove os numeros de telefone com menos de 8 ou mais de 11 caracteres
    for row in array:
      number_last_eight_or_more_eleven = row[1]
      if (len(number_last_eight_or_more_eleven) < 8) | (len(number_last_eight_or_more_eleven) > 11):
        row[1] = ''
      
# Separa o DDD do número de Telefone
    for index in range(len(array)):
      find_tels_with_ddd = (array[index])[1]
      if len(find_tels_with_ddd) in [10,11]:
        ddd = find_tels_with_ddd[0:2]
        (array[index]) = (np.append((array[index]), ddd)).tolist()
        (array[index])[1] = find_tels_with_ddd[2:len(find_tels_with_ddd)]
      else:
        (array[index]) = (np.append((array[index]), '')).tolist()
          
# Remove os numeros de Telefone cujo DDD nao existe
    for row in array:
      find_right_ddd = row[2]
      if (find_right_ddd != ''):
        if (any(item in find_right_ddd for item in all_ddds_from_br)):
          None
        else:
          row[1] = ''
          row[2] = ''
            
# Remove telefones com mais de 8 digitos cujo primeiro seja diferente de "9"
    for row in array:
      find_right_tel = row[1]
      if (len(find_right_tel) == 9) & (find_right_tel[0:1] != '9'):
        row[1] = ''
        row[2] = ''
          
# Inclui o nono digito aos telefones que iniciem por 9,8,7 e 6 e tenham somente 8 digitos.
    for row in array:
      insert_ninth_digit = row[1]
      if (len(insert_ninth_digit) == 8) & (insert_ninth_digit[0:1] in ['9','8','7','6']):
        row[1] = '9' + insert_ninth_digit
      
# Transforma o array com os dados tratados em um dataframe
    df = (sc.parallelize(array).toDF([id_column,phone_column_name,ddd_column_name]))
      
# Realiza o join do dataframe criado com os dados tratados com o dataframe original
    dataframe = (
      dataframe
        .join(df,id_column,'inner')
        .select(columns)
    )
    
  return dataframe