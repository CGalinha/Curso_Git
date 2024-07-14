"""
Ja tem interface grafica com varias funçoes a trabalhar

 # instalar webdriver na pasta -> c:\windows\system

telefones de teste
919311037
+351918121450
+351918408860
+351925406711

pip install openpyxl
pip install mysql-connector-python
sudo apt-get install python3-tk

Falta fazer 
- Saldo final da divida
- Acreto nas colunas de formataçao de moeda
- Envio por email (urgente)


18/09/2021 - Criacao de novo Ecxel

11-10-2021 Fazer a limpeza dos Nif

25/01/2022 - acertos graficos
10/03/2022 - acertos

"""

import pymysql.cursors
import mysql.connector as conn_mysql
import pandas as pd
import numpy as np
from numpy import  float64
import pymssql as sql
from contextlib import contextmanager
from datetime import datetime
############################
#   Import Envio Emails #
############################
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
############################
#   Import para o Whatsapp #
############################
import time
import urllib
#from selenium import webdriver

#from selenium.webdriver.common.keys import Keys

#######################################
# Inferface Grafica
######################################

from tkinter import *

@contextmanager

def conecta_MsSQL():
    con_MsSql = sql.connect(host='192.168.0.201\SQLEXPRESS',
                      user='sa',
                      password='cnj2015+',
                      database='sage2018')
    try:
        yield con_MsSql
    finally:
        con_MsSql.close()

def conecta_MySQL():
    con_MySQL = pymysql.connect(
        host='192.168.0.201',
        port=3309,
        user='root',
        password='Galinha123#',
        db='cnj_xd2016',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    try:
        yield con_MySQL
    finally:
        con_MySQL.close()
        print("MySQL connection is closed")

def ligacao_SQL(sql):

    with conecta_MsSQL() as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(sql)

def ligacao_MySQL(sql):

    with conecta_MySQL() as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(sql)

#*****************************************
#Envia Email com Anexo
#*****************************************
def envia_smtp(folha):
    print(folha)
    dt_string = now.strftime("%d%m%Y-%H%M")
# Configurações do servidor SMTP
    smtp_server = 'mail.cnj-web.com'
    smtp_port = 587
    smtp_username = 'apoio@cnj-web.com'
    smtp_password = 'Galinha123#'
    cc = 'apoio@cnj-web.com'

# Cria o objeto MIMEMultipart para representar o e-mail
    msg = MIMEMultipart()

# Adiciona o corpo do e-mail
    body = "Olá,\n\nSegue o arquivo em anexo."
    msg.attach(MIMEText(body))

# Adiciona o anexo
    filepath = './' + folha
    print(filepath)
    with open(filepath, 'rb') as f:
        attach = MIMEApplication(f.read(), _subtype='xlsx')
        attach.add_header('Content-Disposition', 'attachment', filename=folha)
        msg.attach(attach)

# Lista de destinatários
    destinatarios = ['cnj@net.vodafone.pt', 'catarina.galinha@cnj-web.com']

# Configura as informações do remetente e destinatário

    msg['From'] = smtp_username
    msg['To'] = ", ".join(destinatarios)
    msg['Subject'] = f"Dividas CNJ a {dt_string}!!"
    msg['Cc'] = 'cnj@net.vodafone.pt'

# Conecta no servidor SMTP e envia o e-mail
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, msg['To'], msg.as_string())
        print(f'Email enviado com sucesso. Anexo: {folha}')

def saldo_sage():

#,MobileTelephone1 , LastUpdated

    query = """select  CustomerLedgerAccount.PartyID,Customer.OrganizationName,Customer.KeyFederalTaxID,Customer.EmailAddress, Customer.Telephone1, Customer.MobileTelephone1,Customer.LastUpdated, sum(TotalPendingAmount) as SALDO 
       from CustomerLedgerAccount  JOIN Customer on  CustomerLedgerAccount.partyid=Customer.partyid 
       where CustomerLedgerAccount.ReconciledFlag = 0 group by CustomerLedgerAccount.PartyID,Customer.OrganizationName,Customer.KeyFederalTaxID,Customer.EmailAddress,Customer.Telephone1,Customer.MobileTelephone1,Customer.LastUpdated 
       order by SALDO DESC

       """
    with conecta_MsSQL() as conexao:
        dataSQL = pd.read_sql_query(query, conexao)
    df = pd.DataFrame(dataSQL)


    format1 = "%Y/%m/%d %H:%M:%S"
    format2 = "%Y-%m-%d"
    #df["LastUpdated"] = df["LastUpdated"].apply(lambda x: datetime.strptime(x, format1).strftime(format2))
    df['LastUpdated'] = df['LastUpdated'].apply(lambda x: x.strftime('%Y-%m-%d'))

    Tratamento_Dados_sage(df)
    df.info()
    print("Tabela Sage importada!!")

    #print(df)

def saldo_XD():

    query1 = "select  vat, name, MobilePhone1,ContactEmail, Email1, balance, LastPurchase from entities where balance != 0"
    mydb = conn_mysql.connect(host='192.168.0.201',
                          port=3309,
                          user='root',
                          password='Galinha123#',
                          db='cnj_xd2016')

    df = pd.read_sql(query1, mydb)
    #df.to_excel('Saldo_XD.xlsx', index=False)

    df['LastPurchase'] = df['LastPurchase'].apply(lambda x: x.strftime('%Y-%m-%d'))

    Tratamento_Dados_xd(df)
    #print(df)
    mydb.close()  # close the connection
    df.info()
    print(df['balance'])
    print("Tabela XD importada!!!")
   # query1 = """ select * from items """

def Tratamento_Dados_sage(df):
    df = df.rename(columns={'PartyID': 'Id'})
    df = df.rename(columns={'OrganizationName': 'Empresa'})
    df = df.rename(columns={'KeyFederalTaxID': 'Nif'})
    df = df.rename(columns={'EmailAddress': 'Email_sage'})
    df = df.rename(columns={'MobileTelephone1': 'Telm_sage'})
    df = df.rename(columns={'Telephone1': 'Tele1_sage'})
    df = df.rename(columns={'LastUpdated': 'Movim_Sage'})
    df = df.rename(columns={'SALDO': 'Saldo_Sage'})
    df = df.drop(df[df.Saldo_Sage <= 0].index)
    #print('No tratamento')
    #print(df)
    df.to_excel('Saldo_Sage.xlsx', index=False)

    txt_informacao['text'] = "Ficheiro SAGE criado - OK"

    #print(resul)

def Tratamento_Dados_xd(df):
    df = df.rename(columns={'vat': 'Nif'})
    df = df.rename(columns={'name': 'Empresa'})
    df = df.rename(columns={'MobilePhone1': 'Telm_xd'})
    df = df.rename(columns={'balance': 'Saldo_XD'})
    df = df.rename(columns={'LastPurchase': 'Movim_XD'})
    df = df.sort_values('Saldo_XD')
    df = df.drop(df[df.Saldo_XD > 0].index)

    df1 = df['Saldo_XD'].abs() #converte em valores positivos
    df = df.drop(columns=['Saldo_XD'])
    df['Saldo_XD'] = df1
   # (df['Currency'].replace('[\$,)]', '', regex=True)
   #  .replace('[(]', '-', regex=True).astype(float))

    df.to_excel('Saldo_XD.xlsx', index=False)
    txt_informacao['text'] = "Ficheiro XD criado - OK"

    #print(resul)


def Tratamento_Final():

    sage = pd.read_excel('Saldo_Sage.xlsx')
    xd = pd.read_excel('Saldo_XD.xlsx')
    xd.info()
    sage.info()
    frames = [sage, xd]

    df1 = pd.concat(frames).reset_index()
    var1 = 0.0
    df1["Saldo_CNJ"] = var1
    df1['Saldo_XD'] = df1['Saldo_XD'].fillna(value=0)
    df1['Saldo_Sage'] = df1['Saldo_Sage'].fillna(value=0)
    ###########################################################
    # Ficheiro para analise
    ###########################################################
    #df1.to_excel('Saldo_Temp.xlsx', index=False)

   # df1.set_index('Nif', inplace=True)
    for a, row in df1.iterrows():
        soma = 0,0
        cont = 0
        #print(df1.loc[i])
        nif = (df1.loc[a]['Nif'])
        for i, row in df1.iterrows():
            #print(nif, ' == ',(df1.loc[i]['Nif']) )
            if nif == (df1.loc[i]['Nif']):
                cont = cont + 1
                #print('encontei um')

                soma_1 = (df1.loc[i]['Saldo_XD'])
                soma1 = soma_1

                #print('type da Soma1 XD',type(soma1))
                print(f'soma1 saldo cnj ->{soma1}')
                soma2 = (df1.loc[i]['Saldo_Sage'])
                soma = soma2 + soma1 + soma
                #print(soma, '= ', soma1, '+', soma2, 'vezes = ', cont)

        ########################################################
        # a soma tem o type numpy - teve de ser convertido
        ########################################################
        soma_2 = soma.item(0)
        #print(f'Soma_2 =',type(soma_2))
        #print(f'Soma_2 =', soma_2)
        df1.at[a, 'Saldo_CNJ'] = soma_2
        #print(type(soma))
        #print(soma)
        

        #print(' nif  a alterar->',df1.loc[a]['Nif']) # nif encontrado
        #print('linha que foi mexida ->>>>>>>')
        #print(df1.loc[a])


    #df_result = df1.groupby(["Nif"]).sum().reset_index() - >resulta mas ...

    df1 = df1.drop(columns=['Saldo_XD'])
    df1 = df1.drop(columns=['Saldo_Sage'])
    df1 = df1.drop(columns=['index'])
    df1 = df1.drop(columns=['Id'])
    print(f"Saldo CNJ -> {df1['Saldo_CNJ']}")
    ####################################################
    # Passagem de dados para linhas vazias
    #####################################################
    uniao = df1

    for a, row in df1.iterrows():
        soma = 0
        cont = 0
        # print(df1.loc[i])
        nif = (uniao.loc[a]['Nif'])
        for i, row in uniao.iterrows():
            # print(nif, ' == ',(df1.loc[i]['Nif']) )
            if nif == (uniao.loc[i]['Nif']):
                telm = (uniao.loc[i]['Telm_xd'])
                email1 = (uniao.loc[i]['ContactEmail'])
                email2 = (uniao.loc[i]['Email1'])
                data_xd = (uniao.loc[i]['Movim_XD'])

        df1.at[a, 'Telm_xd'] = telm
        df1.at[a, 'ContactEmail'] = email1
        df1.at[a, 'Email1'] = email2
        df1.at[a, 'Movim_XD'] = data_xd

    df1.to_excel('Novo1_CNJ.xlsx', index=False)

    #################################################
    # Fazer a limpeza e por os numeros com +351
    #################################################
    df1 = df1.drop_duplicates(subset='Nif', keep='first')
    df1 = df1.sort_values('Saldo_CNJ', ascending=False)
    dt_string = now.strftime("%d%m%Y-%H%M")
    fich = 'Saldo_CNJ_final_'+dt_string+'.xlsx'
    df1.to_excel('Saldo_CNJ.xlsx', index=False)
    df1 = df1 [['Empresa','Nif','Saldo_CNJ','Movim_Sage','Movim_XD','Tele1_sage','Telm_sage','Telm_xd','Email_sage','ContactEmail','Email1']]
    df1.to_excel(fich, index=False)
    envia_smtp(fich)
    #print(sage)
    #print(xd)
    #return resul_final
    print("Tabela Tratamento Final")
    print("Tabela EXCEL criado com sucesso!!!")

    txt_informacao['text'] = "Tabela EXCEL criado com sucesso!!!"


def Limpa_Nif():

    lista_nif={504872192, 509602886, 507957016, 508708435, 509251544, 235560588}
    cnjnif = pd.read_excel('Saldo_CNJ.xlsx')

    df_saida = cnjnif.loc[~cnjnif['Nif'].isin(lista_nif)]
    df_saida.to_excel('Saldo_CNJ_nifs.xlsx', index=False)
    print(df_saida)

    print("Nif limpos!!!")
    txt_informacao['text'] = "Nif's limpos - OK"
    #txt_orientacao = Label(janela, text="Nif's limpos - OK                       ")
    #txt_orientacao.grid(column=0, row=9, padx=5, pady=5)

def WhatsUp():

    contatos_df = pd.read_excel("Saldo_CNJ.xlsx")
    print(contatos_df)
    # instalar webdriver na pasta -> c:\windows\system
    navegador = webdriver.Chrome()
    navegador.get("https://web.whatsapp.com/")

    while len(navegador.find_elements_by_id("side")) < 1:
        time.sleep(2)

    for i, mensagem in enumerate(contatos_df['Nif']):
        saldo = contatos_df.loc[i, "Saldo_CNJ"]
        pessoa = contatos_df.loc[i, "Empresa"]
        numero = contatos_df.loc[i, "Tele1"]
        texto = urllib.parse.quote(
            f"Ola, {pessoa}, nif: {mensagem}, informo que tem um valor em divida de {saldo}€, Obrigado CNJ")
        # print(texto)
        link = f"https://web.whatsapp.com/send?phone={numero}&text={texto}"
        navegador.get(link)
        while len(navegador.find_elements_by_id("side")) < 1:
            time.sleep(1)
        # aquo faz o enter -> usar o xpath

        inp_xpath = '//*[@id="main"]/footer/div[1]/div[2]/div/div[1]/div/div[2]'

        time.sleep(10)

        element = navegador.find_element_by_xpath(inp_xpath)
        element.send_keys(Keys.ENTER)
        time.sleep(10)


if __name__ == "__main__":

    # datetime object containing current date and time
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y - %H:%M %S")

    #print("now =", now)


    janela = Tk()
    janela.title("Pedidos de Euros CNJ - Ver 2022.01.25")
    janela.geometry("800x500")
    janela.resizable(True, True)
    #janela.iconbitmap("imagens/icon.icon")
    # Texto na tela
    txt_orientacao = Label(janela, text="opçao de recolha dados")
    txt_orientacao.grid(column=0, row=0, padx=5, pady=5)

    # Botao na tela para chamar a funçao
    botao_xd = Button(janela, text="Recolha de dados no XD", command= saldo_XD)
    botao_xd.grid(column=0, row=1, padx=5, pady=5)

    botao_sage = Button(janela, text="Recolha de dados no Sage", command=saldo_sage)
    botao_sage.grid(column=0, row=2, padx=5, pady=5)

    botao_tratamento = Button(janela, text="Criaçao de fich divida", command=Tratamento_Final)
    botao_tratamento.grid(column=0, row=3, padx=5, pady=5)

    botao_tratamento = Button(janela, text="Limpeza de Nif's", command=Limpa_Nif)
    botao_tratamento.grid(column=1, row=4, padx=5, pady=5)

    botao_Whatsupp = Button(janela, text="Envio por WhatsUpp", command=WhatsUp)
    botao_Whatsupp.grid(column=0, row=5, padx=5, pady=5)

    botao_Email = Button(janela, text="Envio por Email", command='')
    botao_Email.grid(column=0, row=6, padx=5, pady=5)

    # Texto na tela Coluna 2
    txt_orientacao1 = Label(janela, text="Trat. Dados")
    txt_orientacao1.grid(column=1, row=0, padx=5, pady=5)

    botao_1 = Button(janela, text="Telf +351", command='')
    botao_1.grid(column=1, row=1, padx=5, pady=5)

    txt_info = Label(janela, text=dt_string)
    txt_info.grid(column=0, row=7, padx=5, pady=5)
    txt_informacao = Label(janela, text="                                         ",
                           bg="yellow",
                           fg="blue",
                           font="Arial 8 bold",
                           width=100,
                           height=2,
                           bd=1,
                           relief="solid")
    txt_informacao.grid(column=0, row=9, columnspan=2, padx=5, pady=5)
    janela.mainloop()



    #saldo_sage()
    #saldo_XD()
    #Tratamento_Final()
    #WhatsUp()





