from diacono_categ.agent import classificar_com_ia
import json

if __name__ == "__main__":
    print("Classificando transação...")
    lista=['ELECTRON MINI EXTRA-51502','ELECTRON FRIGOMUNDO C1502','ELECTRON FDI FERNAO D1502','ELECTRON NSAI POSTO D1602','ELECTRON DROGARIA SAO1602','ELECTRON RP SERVICOS 1702','ELECTRON RP SERVICOS 1702','ELECTRON IvoneteMaria1702']
    categoria = [classificar_com_ia(l) for l in lista]
    print(categoria)

    # with open('saida.json', 'r', encoding='utf-8') as f:
    # dados = json.load(categoria)
    lista_dict = [[ item['original'], item['categoria']] for item in categoria]
    print(lista_dict)