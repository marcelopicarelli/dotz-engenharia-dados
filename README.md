## Teste Dotz - Engenharia de Dados

O presente problema se refere aos dados de uma empresa que produz e vende máquinas industriais. O objetivo deste desafio é **(1) Fazer a modelagem conceitual dos dados**, **(2) Criar a infraestrutura necessária** e **(3) criar todos os artefatos necessários** para carregar os arquivos para o banco criado.
Em todo laboratório foi utilizado o Google DataFlow, Google BigTable, Cloud Shell Editor


## Arquivos (Datasets)

Os seguintes arquivos contém os dados que deverão ser importados para o banco de dados:
- price_quote.csv
- bill_of_materials.csv
- comp_boss.csv

Os arquivos foram movidos para um bucket do cloud-storage que serão consultados para criação de tabelas no Google BigTable, que permitirão consultas e exploração dos dados.

## Pré-Configuração do Servidor

Os comandos a seguir foram executados num ambiente virtual:


    python3 -m pip install virtualenv
    virtualenv -p python3 venv
    source venv/bin/activate
    
    pip install 'apache-beam[gcp]'
    pip install gcsfs
    pip install pandas
    
    export PROJECT=stellar-acre-294711
Arquivos criados:

 - load_bill_of_materials.py
 - load_comp_boss.py
 - load_price_quote.py
 - setup.py

## Print-Screens

