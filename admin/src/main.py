def debitofake(mesde, mesfake):
    import os

    from myextractdatafake import my_extract_excel_deb
    my_fin = my_extract_excel_deb
    path = os.getcwd()
    #print(path)

    my_fin(path, f'custo_{mesde}.xls', mesfake, 'pulling')
    my_fin(path, f'custo_{mesde}.xls', mesfake, 'pushout')
    my_fin(path, f'custo_{mesde}.xls', mesfake, 'stop')

def debito():
    import os

    from myextract import my_extract_excel_deb
    my_fin = my_extract_excel_deb
 
    from myextract import my_type_modal_finance
    my_type_modal_fin = my_type_modal_finance 

    path = os.getcwd()
    #print(path)
    #file = 'custo_2023_07.xls'
    #pushout
    #pulling
    #saldo

    #total de arquivos para leitura


    for i in range(1,13):
        if i <= 9:
            if os.path.exists(f'{path}/excel/custo_2023_0{i}.xls'):
                print('\n')
                df = my_fin(path, f'custo_2023_0{i}.xls')
                
                print(f"TAMANHO TOTAL DF: {len(df)}")

                print('\n')
                my_type_modal_fin(df, path, f'custo_2023_0{i}.xls', 'pulling')
                print('\n')
                my_type_modal_fin(df, path, f'custo_2023_0{i}.xls', 'pushout')
                print('\n')
                my_type_modal_fin(df, path, f'custo_2023_0{i}.xls', 'stop')
            else:
                print('\n')
                print(f'arquivo {path}/excel/custo_2023_0{i}.xls não existe')
        else:
            if os.path.exists(f'{path}/excel/custo_2023_{i}.xls'):

                print('\n')
                df = my_fin(path, f'custo_2023_{i}.xls')
                
                print(f"TAMANHO TOTAL DF: {len(df)}")

                print('\n')
                my_type_modal_fin(df, path, f'custo_2023_{i}.xls', 'pulling')
                print('\n')
                my_type_modal_fin(df, path, f'custo_2023_{i}.xls', 'pushout')
                print('\n')
                my_type_modal_fin(df, path, f'custo_2023_{i}.xls', 'stop')
            else:
                print('\n')
                print(f'arquivo {path}/excel/custo_2023_{i}.xls não existe')
                
def credito():
    import os

    from myextractcred import my_extract_excel_cred
    my_fin = my_extract_excel_cred

    from myextractcred import my_credits
    my_cred = my_credits
    
    path = os.getcwd()
    print(path)
    #file = 'custo_2023_07.xls'
    #pushout
    #pulling
    #saldo


    for i in range(1,13):
        if i <= 9:
            if os.path.exists(f'{path}/excel/credito_2023_0{i}.xls'):
                print('\n')
                df = my_fin(path, f'credito_2023_0{i}.xls')
                
                print(f"TAMANHO TOTAL DF: {len(df)}")

                print('\n')

                my_cred(df, path, f'credito_2023_0{i}.xls')
            else:
                print('\n')
                print(f'arquivo {path}/excel/credito_2023_0{i}.xls não existe')                
        else:

            if os.path.exists(f'{path}/excel/credito_2023_{i}.xls'):
                df = my_fin(path, f'credito_2023_{i}.xls')

                print(f"TAMANHO TOTAL DF: {len(df)}")

                print('\n')

                my_cred(df, path, f'credito_2023_{i}.xls')
            else:
                print('\n')
                print(f'arquivo {path}/excel/credito_2023_{i}.xls não existe')

def limpeza_dados(sql):
    from mygcpjobfinfam import my_execute_job
    my_job = my_execute_job

    my_job(sql)


if __name__=='__main__':

    lista = ['credito_2023_excel'] #['recebido_2023_excel','credito_2023_excel','custo_2023_excel','saldo_2023_excel']
    for tab in lista:

        print(f'Primeira etapa gerar dados controle do cartão de credito: Limpeza de dados da tabela:{tab}!!!','\n')

        query = f'DROP TABLE IF EXISTS dev_domestico.{tab}' #where dt_mes_base = {periodo}
        limpeza_dados(query)

    #CREDITO
    credito()

    lista = ['recebido_2023_excel','custo_2023_excel','saldo_2023_excel']
    for tab in lista:

        print(f'Primeira etapa gerar dados controle da conta debito: Limpeza de dados da tabela:{tab}!!!','\n')

        query = f'DROP TABLE IF EXISTS dev_domestico.{tab}' #where dt_mes_base = {periodo}
        limpeza_dados(query)
    
    #DEBITO
    debito()

    #fake
    #debitofake('2023-06','2023-08')