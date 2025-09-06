# ETL dos arquivos baixados do itau

### RAW

1. 
    1. instalacao.sh

        preparar ambiente para execução do etl
        ./admin/raw/etl_sh/instalacao.sh 


    2. etl.sh

        gera um unico arquivos unidos dtodos os arquvios baixados mes a mes, com nome sugestivo de custo_ano_mes

        arquvoo baixaods estao localmente neste local:
        C:\Users\andre\Documents\github\my-money-family\admin\etl\excel

        - base extrato da conta corrente
        ./admin/raw/etl_sh/etl.sh 

        - base custo cartão creditos
        ./admin/raw/etl_sh/etlcred.sh 


    3. main.py

        import o arquivo para sheets online

        dentro da maq virtualenv 
        python ./admin/raw/src/main.py 

    4. sheets online

        gerado neste arquivo:

        https://docs.google.com/spreadsheets/d/1UpB2k12Jy6yPkg0PHG-AVXTmQo_B2Wh5N-sFsdHL-N4/edit?gid=0#gid=0

        compartilhado: devsamelo-dev@devsamelo2.iam.gserviceaccount.com

### PROCESS

1. 
    1. main.py

        import o arquivo para sheets online

        dentro da maq virtualenv 
        python ./admin/process/src/main.py 

    2. sheets online

        gerado neste arquivo:

        https://docs.google.com/spreadsheets/d/1by76kJeADccF_ZIZnTSZcGd8TPJUYMo_zg021re5RLA/edit?gid=1194138400#gid=1194138400

        compartilhado: devsamelo-dev@devsamelo2.iam.gserviceaccount.com