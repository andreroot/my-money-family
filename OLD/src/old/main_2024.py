import os

from datamyfin.myextract_2024 import my_extract_excel_deb, my_type_modal_finance
from datamyfin.myextractdatafake import my_extract_excel_deb_fake
from datamyfin.myextractcred_2024 import my_extract_excel_cred, my_credits

def debitofake(mesde, mesfake):

    my_fin_fake = my_extract_excel_deb_fake
    path = os.getcwd()
    #print(path)

    my_fin_fake(path, f'custo_{mesde}.xls', mesfake, 'pulling')
    my_fin_fake(path, f'custo_{mesde}.xls', mesfake, 'pushout')
    my_fin_fake(path, f'custo_{mesde}.xls', mesfake, 'stop')

def debito(mes_ano, ano_base):

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

    else:
        for i in range(1,13):
            if i <= 9:
                if os.path.exists(f'{path}/v0/data/excel/custo_2024_0{i}.xls'):
                    print('\n')
                    df = my_fin(path, f'custo_2024_0{i}.xls')
                    
                    print(f"TAMANHO TOTAL DF: {len(df)}")

                    print('\n')
                    my_type_modal_fin(df, path, f'custo_2024_0{i}.xls', 'pulling','2024')
                    print('\n')
                    my_type_modal_fin(df, path, f'custo_2024_0{i}.xls', 'pushout','2024')
                    print('\n')
                    my_type_modal_fin(df, path, f'custo_2024_0{i}.xls', 'stop','2024')
                else:
                    print('\n')
                    print(f'arquivo {path}/v0/data/excel/custo_2024_0{i}.xls não existe')
            else:
                if os.path.exists(f'{path}/v0/data/excel/custo_2024_{i}.xls'):

                    print('\n')
                    df = my_fin(path, f'custo_2024_{i}.xls')
                    
                    print(f"TAMANHO TOTAL DF: {len(df)}")

                    print('\n')
                    my_type_modal_fin(df, path, f'custo_2024_{i}.xls', 'pulling','2024')
                    print('\n')
                    my_type_modal_fin(df, path, f'custo_2024_{i}.xls', 'pushout','2024')
                    print('\n')
                    my_type_modal_fin(df, path, f'custo_2024_{i}.xls', 'stop','2024')
                else:
                    print('\n')
                    print(f'arquivo {path}/v0/data/excel/custo_2024_{i}.xls não existe')
                
def credito(mes_ano):

    my_fin = my_extract_excel_cred
    my_cred = my_credits
    
    path = os.getcwd()
    print(path)
    #file = 'custo_2023_07.xls'
    #pushout
    #pulling
    #saldo

    if mes_ano:
        if os.path.exists(f'{path}/v0/data/excel/credito_{mes_ano}.xls'):
            print('\n')
            df = my_fin(path, mes_ano, f'credito_{mes_ano}.xls')

            print(f"TAMANHO TOTAL DF: {len(df)}")

            print('\n')

            my_cred(df, path, f'credito_{mes_ano}.xls')
        else:
            print('\n')
            print(f'arquivo {path}/v0/data/excel/credito_{mes_ano}.xls não existe')  

    else:
        for i in range(1,13):
            if i <= 9:
                if os.path.exists(f'{path}/v0/data/excel/credito_2024_0{i}.xls'):
                    print('\n')
                    df = my_fin(path, f'2024_0{i}', f'credito_2024_0{i}.xls')
                    
                    print(f"TAMANHO TOTAL DF: {len(df)}")

                    print('\n')

                    my_cred(df, path, f'credito_2024_0{i}.xls')
                else:
                    print('\n')
                    print(f'arquivo {path}/v0/data/excel/credito_2024_0{i}.xls não existe')                
            else:

                if os.path.exists(f'{path}/data/excel/credito_2024_{i}.xls'):
                    df = my_fin(path, f'credito_2024_{i}.xls')

                    print(f"TAMANHO TOTAL DF: {len(df)}")

                    print('\n')

                    my_cred(df, path, f'credito_2024_{i}.xls')
                else:
                    print('\n')
                    print(f'arquivo {path}/v0/data/excel/credito_2024_{i}.xls não existe')

def limpeza_dados(sql):
    from resource.mygcpjobfinfam import my_execute_job
    my_job = my_execute_job

    my_job(sql)


if __name__=='__main__':

    import sys
    # argsv[1] = tipo da operação
    # argsv[2] = nome do arquivo somente ano_mes ex: 2023_08
    # argsv[3] = data base do arquivo somente ano-mes-dia ex: 2023-08-01
    # argsv[4] = ano base ex: 2024

    # python .\main.py 'geral'

    # python .\main.py 'debito' '2023_09' '2023-09-01'
    # python .\main.py 'credito' '2023_09' '2023-09-01'
    # 
    
    if sys.argv[1]=='debito' or sys.argv[1]=='credito':
        
        if sys.argv[1]=='credito':

            if sys.argv[2]!='None':

                print(f'Primeira etapa gerar dados controle do cartão de credito: Limpeza de dados da tabela de credito 2024 do mes base: {sys.argv[3]} !!!','\n')

                query = f'DELETE FROM dev_domestico.credito_2024_excel where dt_mes_base = "{sys.argv[3]}"'
                limpeza_dados(query)

                import time
                wait_time = 10
                time.sleep(wait_time)

                #CREDITO
                credito(sys.argv[2])
            else:

                print(f'Primeira etapa gerar dados controle do cartão de credito: Limpeza geral de dados da tabela de credito 2024 !!!','\n')

                query = f'DELETE FROM dev_domestico.credito_2024_excel where 1=1'
                limpeza_dados(query)

                import time
                wait_time = 10
                time.sleep(wait_time)

                #CREDITO
                credito(None)

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
            debito(sys.argv[2], sys.argv[4])


    elif sys.argv[1]=='geral':
        lista = ['credito_2024_excel'] #['recebido_2023_excel','credito_2023_excel','custo_2023_excel','saldo_2023_excel']
        for tab in lista:

            print(f'Primeira etapa gerar dados controle do cartão de credito: Limpeza de dados da tabela:{tab}!!!','\n')

            query = f'DROP TABLE IF EXISTS dev_domestico.{tab}' #where dt_mes_base = {periodo}
            limpeza_dados(query)

            import time
            wait_time = 10
            time.sleep(wait_time)
            
        #CREDITO
        credito(None)

        lista = ['recebido_2024_excel','custo_2024_excel','saldo_2024_excel']
        for tab in lista:

            print(f'Primeira etapa gerar dados controle da conta debito: Limpeza de dados da tabela:{tab}!!!','\n')

            query = f'DROP TABLE IF EXISTS dev_domestico.{tab}' #where dt_mes_base = {periodo}
            limpeza_dados(query)
        
            import time
            wait_time = 10
            time.sleep(wait_time)

        #DEBITO
        debito(None)

        #fake
        #debitofake('2023-06','2023-08')