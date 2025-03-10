# my-money-family
Minha vida financeira familiar


comando:

[linux]
source env/bin/activate
cd admin/src/
python main.py

[windows]
.\env\Scripts\activate
cd .\admin\src
python .\main.py

# operar etl dos dados do excel

Na pasta do excel ./src/excel/, contam os arquivos baixados do itau, precisa entrar na conta e gerar execl do extrato conta corrente e cartão de credito

para execução padrão entrar na pasta ./src/ e digitar 
python .\main.py [windows] ou python main.py [linux]

Então, 
executa na pasta do excel todos os arquivos, e os dados são gerados nas tabelas associadas que estão no dataset [dev_domestico]

*Para execução de um unico arquivo excel
foi incluido uma def que extrai os dados de um arquivo especifico, como executar:

[debito]
python .\main.py 'debito' '2023_08' '2023-08-01'
apartir de 2024 
python .\main_2024.py 'debito' '2024_08' '2024-08-01'


[credito]
python .\main.py 'credito' '2023_08' '2023-08-01'
apartir de 2024 
python .\main_2024.py 'credito' '2024_08' '2024-08-01'
rodar geral
python .\main_2024.py 'credito' 'None'

[geral]
python .\main.py 'geral'