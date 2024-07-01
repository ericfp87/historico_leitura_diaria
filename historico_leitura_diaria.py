import os
import pandas as pd
import shutil

pastaunmt1 = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNMT"
pastaunmt2 = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNMT\\Gerados" 
pasta1 = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNLE"
pasta2 = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNLE\\Gerados"
arquivo_parquet_unle = r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\historico_UNLE.parquet"
arquivo_parquet_unmt = r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\historico_UNMT.parquet"

#DADOS UNLE
df_total_unle = pd.DataFrame()

#LISTA ARQUIVOS CSV
for filename in os.listdir(pasta1):
    if filename.endswith('.csv'):

        #LÊ ARQUIVO CSV
        df = pd.read_csv(os.path.join(pasta1, filename), delimiter=';')
        
        #CONCATENA NO DATAFRAME
        df_total_unle = pd.concat([df_total_unle, df])

        #MOVE O ARQUIVO
        shutil.move(os.path.join(pasta1, filename), pasta2)
        
#GERA ARQUIVO CSV UNIFICADO
df_total_unle.to_csv(os.path.join(pasta1, 'unificado.csv'), index=False, sep=';')

#LÊ ARQUIVO PARQUET
df_parquet = pd.read_parquet(arquivo_parquet_unle)

#CONCATENA O DATAFRAME DO CSV COM O DATAFRAME PARQUET
df_parquet = pd.concat([df_parquet, df_total_unle])

#CRIA O NOVO ARQUIVO PARQUET
df_parquet.to_parquet(arquivo_parquet_unle)

#REMOVE O ARQUIVO UNIFICADO
os.remove(os.path.join(pasta1, 'unificado.csv'))



#DADOS UNMT
df_total_unmt = pd.DataFrame()

#LISTA ARQUIVOS CSV
for filename in os.listdir(pastaunmt1):
    if filename.endswith('.csv'):
        
        #LÊ ARQUIVO CSV
        df = pd.read_csv(os.path.join(pastaunmt1, filename), delimiter=';')

        #CONCATENA NO DATAFRAME
        df_total_unmt = pd.concat([df_total_unmt, df])

        #MOVE O ARQUIVO
        shutil.move(os.path.join(pastaunmt1, filename), pastaunmt2)

#GERA ARQUIVO CSV UNIFICADO
df_total_unmt.to_csv(os.path.join(pastaunmt1, 'unificado.csv'), index=False, sep=';')

#LÊ ARQUIVO PARQUET
df_parquet = pd.read_parquet(arquivo_parquet_unmt)

#CONCATENA O DATAFRAME DO CSV COM O DATAFRAME PARQUET
df_parquet = pd.concat([df_parquet, df_total_unmt])

#CRIA O NOVO ARQUIVO PARQUET
df_parquet.to_parquet(arquivo_parquet_unmt)

#REMOVE O ARQUIVO UNIFICADO
os.remove(os.path.join(pastaunmt1, 'unificado.csv'))




