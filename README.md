## Descrição do Projeto

Este projeto processa arquivos CSV de duas pastas específicas, combina os dados em arquivos unificados, e atualiza arquivos `.parquet` existentes com os novos dados. O objetivo é manter os dados de leituras em tempo real atualizados e organizados em formatos eficientes para consulta.

## Funcionamento do Código

### Importação das Bibliotecas

```python
import os
import pandas as pd
import shutil
```

O código começa importando as bibliotecas `pandas` para manipulação de dados, `os` para operações com o sistema de arquivos, e `shutil` para mover arquivos entre pastas.

### Definição de Caminhos

```python
pastaunmt1 = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNMT"
pastaunmt2 = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNMT\\Gerados"
pasta1 = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNLE"
pasta2 = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNLE\\Gerados"
arquivo_parquet_unle = r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\historico_UNLE.parquet"
arquivo_parquet_unmt = r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\historico_UNMT.parquet"
```

Os caminhos para as pastas de origem e destino, assim como os arquivos `.parquet`, são definidos.

### Processamento dos Arquivos UNLE

#### Inicialização do DataFrame

```python
df_total_unle = pd.DataFrame()
```

Um DataFrame vazio é inicializado para armazenar os dados combinados dos arquivos CSV.

#### Leitura e Movimentação de Arquivos CSV

```python
for filename in os.listdir(pasta1):
    if filename.endswith('.csv'):
        df = pd.read_csv(os.path.join(pasta1, filename), delimiter=';')
        df_total_unle = pd.concat([df_total_unle, df])
        shutil.move(os.path.join(pasta1, filename), pasta2)
```

O código percorre todos os arquivos CSV na pasta especificada, lê cada arquivo e concatena os dados no DataFrame. Após a leitura, os arquivos são movidos para outra pasta.

#### Geração do Arquivo CSV Unificado

```python
df_total_unle.to_csv(os.path.join(pasta1, 'unificado.csv'), index=False, sep=';')
```

Os dados combinados são salvos em um arquivo CSV unificado.

#### Atualização do Arquivo Parquet

```python
df_parquet = pd.read_parquet(arquivo_parquet_unle)
df_parquet = pd.concat([df_parquet, df_total_unle])
df_parquet.to_parquet(arquivo_parquet_unle)
os.remove(os.path.join(pasta1, 'unificado.csv'))
```

O arquivo `.parquet` existente é lido e combinado com os novos dados. O resultado é salvo de volta no arquivo `.parquet`, e o arquivo CSV unificado é removido.

### Processamento dos Arquivos UNMT

O mesmo processo é repetido para os arquivos na pasta UNMT.

#### Inicialização do DataFrame

```python
df_total_unmt = pd.DataFrame()
```

Um DataFrame vazio é inicializado para armazenar os dados combinados dos arquivos CSV.

#### Leitura e Movimentação de Arquivos CSV

```python
for filename in os.listdir(pastaunmt1):
    if filename.endswith('.csv'):
        df = pd.read_csv(os.path.join(pastaunmt1, filename), delimiter=';')
        df_total_unmt = pd.concat([df_total_unmt, df])
        shutil.move(os.path.join(pastaunmt1, filename), pastaunmt2)
```

O código percorre todos os arquivos CSV na pasta especificada, lê cada arquivo e concatena os dados no DataFrame. Após a leitura, os arquivos são movidos para outra pasta.

#### Geração do Arquivo CSV Unificado

```python
df_total_unmt.to_csv(os.path.join(pastaunmt1, 'unificado.csv'), index=False, sep=';')
```

Os dados combinados são salvos em um arquivo CSV unificado.

#### Atualização do Arquivo Parquet

```python
df_parquet = pd.read_parquet(arquivo_parquet_unmt)
df_parquet = pd.concat([df_parquet, df_total_unmt])
df_parquet.to_parquet(arquivo_parquet_unmt)
os.remove(os.path.join(pastaunmt1, 'unificado.csv'))
```

O arquivo `.parquet` existente é lido e combinado com os novos dados. O resultado é salvo de volta no arquivo `.parquet`, e o arquivo CSV unificado é removido.

## Como Executar

1. Certifique-se de que os arquivos CSV estão nas pastas especificadas.
2. Atualize os caminhos das pastas e dos arquivos `.parquet`, se necessário.
3. Execute o script em um ambiente Python com Pandas e PyArrow instalados.
4. Verifique os arquivos `.parquet` gerados nos diretórios de destino.

## Requisitos

- Python 3.x
- Pandas
- PyArrow (para manipulação de arquivos `.parquet`)

```sh
pip install pandas pyarrow
```

## Contribuições

Sinta-se à vontade para contribuir com melhorias para este projeto. Abra um pull request ou reporte um problema no repositório.

---

Espero que isso ajude a entender o funcionamento do código e como ele pode ser usado para combinar e atualizar dados de leituras em tempo real!