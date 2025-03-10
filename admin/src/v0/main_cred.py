import os
import sys
from datamyfin.myextractcred_2024 import my_extract_excel_cred, my_credits

def limpeza_dados(sql):
    from resource.mygcpjobfinfam import my_execute_job
    my_job = my_execute_job

    my_job(sql)

class Main:

    def __init__(self):
        pass

        # argsv[1] = tipo da operação
        # argsv[2] = nome do arquivo somente ano_mes ex: 2023_08
        # argsv[3] = data base do arquivo somente ano-mes-dia ex: 2023-08-01
        # argsv[4] = ano base ex: 2024

    def credito(self, mes_ano, ano_base):

        my_fin = my_extract_excel_cred
        my_cred = my_credits

        path = os.getcwd()
        #print(path)
        #file = 'custo_2023_07.xls'
        #pushout
        #pulling
        #saldo

        #total de arquivos para leitura

        if mes_ano:

            if os.path.exists(f'{path}/v0/data/excel/credito_{mes_ano}.xls'):
                print('\n')
                df = my_fin(path, mes_ano, f'credito_{mes_ano}.xls')
                
                print(f"TAMANHO TOTAL DF: {len(df)}")

                print('\n')
                my_cred(df, path, f'credito_{mes_ano}.xls', ano_base)
            else:
                print('\n')
                print(f'arquivo {path}/v0/data/excel/custo_{mes_ano}.xls não existe') 



if __name__=='__main__':

    if sys.argv[1]=='credito':
        ano_base = sys.argv[4]

        print(f'Primeira etapa gerar dados controle do cartão de credito: Limpeza de dados da tabela de credito do mes e ano base: {sys.argv[3]} !!!','\n')

        query = f'DELETE FROM dev_domestico.credito_{ano_base}_excel where dt_mes_base = "{sys.argv[3]}"'
        limpeza_dados(query)

        import time
        wait_time = 10
        time.sleep(wait_time)
        
        #CREDITO
        m = Main()
        m.credito(sys.argv[2], sys.argv[4])