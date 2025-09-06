import os
import sys
from datamyfin.myextract_2024 import my_extract_excel_deb, my_type_modal_finance

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

    def debito(self, mes_ano, ano_base):

        my_fin = my_extract_excel_deb
        my_type_modal_fin = my_type_modal_finance 

        path = os.getcwd()
        #print(path)
        #file = 'custo_2023_07.xls'
        #pushout
        #pulling
        #saldo

        #total de arquivos para leitura

        if mes_ano:

            if os.path.exists(f'{path}/v0/data/excel/custo_{mes_ano}.xls'):
                print('\n')
                df = my_fin(path, f'custo_{mes_ano}.xls')
                
                print(f"TAMANHO TOTAL DF: {len(df)}")

                print('\n')
                my_type_modal_fin(df, path, f'custo_{mes_ano}.xls', 'pulling', ano_base)
                print('\n')
                my_type_modal_fin(df, path, f'custo_{mes_ano}.xls', 'pushout', ano_base)
                print('\n')
                my_type_modal_fin(df, path, f'custo_{mes_ano}.xls', 'stop', ano_base)
            else:
                print('\n')
                print(f'arquivo {path}/v0/data/excel/custo_{mes_ano}.xls não existe') 



if __name__=='__main__':

    if sys.argv[1]=='debito':
        ano_base = sys.argv[4]
        lista = [f'recebido_{ano_base}_excel',f'custo_{ano_base}_excel',f'saldo_{ano_base}_excel']

        for tab in lista:

            print(f'Primeira etapa gerar dados controle da conta debito: Limpeza de dados da tabela:{tab}!!!','\n')

            query = f'DELETE FROM dev_domestico.{tab} where dt_mes_base = "{sys.argv[3]}"'
            limpeza_dados(query)

            import time
            wait_time = 10
            time.sleep(wait_time)
        
        #DEBITO
        m = Main()
        m.debito(sys.argv[2], sys.argv[4])