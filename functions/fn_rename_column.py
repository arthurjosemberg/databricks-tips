import pyspark.sql.functions

def fn_rename_column(dataframe, nomes_antes, nomes_depois):
    mapping = dict(zip(nomes_antes, nomes_depois))
    dataframe = dataframe.select([col(coluna).alias(mapping.get(coluna, coluna)) for coluna in nomes_antes])
    return dataframe