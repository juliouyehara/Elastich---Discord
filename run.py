import time
from catalog_config import CatalogConfig
from utils.elastic import Elastic
from utils.csv import convert_to_csv
from utils.buckets import Buckets
from utils.discord import *
import uuid
import datetime
from sort_subjects import sort_by_subject
from get_sac_mails import get_sac_mails
import psycopg2 as pg
import pandas as pd
from secrets import *


config = CatalogConfig()
config.read()

data = []
df = ['id','name','email','cnpj','telefone']

conection = pg.connect(config['POSTGRE']['URI'])
sql = "SELECT  seller_id, seller_nm, email_ds, cnpj_nu, phone_nu from vw_nodis_seller "
curs = conection.cursor()
curs.execute(sql)
for i in curs.fetchall():
    data.append(dict(zip(df,i)))

df1 = pd.json_normalize(data)
conection.close()
curs.close()




date = datetime.date.today() - datetime.timedelta(days=1)

id = str(uuid.uuid4().fields[-1])[:8]

el = Elastic(config['ELASTIC']['URI'])

##############
# Listar assuntos e quantidade do último dia
##############

sort_by_subject(elastic=el, index='magalu_sellers_gmail*', key=config['DISCORD']['URI'])
sort_by_subject(elastic=el, index='b2w_sellers_gmail*', key=config['DISCORD']['URI'])

##############
# Converter e-mails para CSV
##############


# Adicionar novos assuntos de e-mail aqui:
subjects = [
    "Atualize seus dados bancários para receber o seu repasse",
    "NOTIFICAÇÃO DE RESTRIÇÃO FINANCEIRA",
    "Cobrança Saldo Devedor Marketplace",
    "Tem mensagem nova para você",
    "Aviso Importante: Sua loja possui notas fiscais inválidas",
    "MKT PLACE MAGALU - VALOR EM ABERTO",
    "Atualize os dados bancários!",
    "Regularize sua situação para liberar o seu repasse"
]


email_list = []

for subject in subjects:
    print('Filtrando assunto no elastic')
    elastich_control = True
    while elastich_control is True:
        try:
            data = el.get_data(query=subject, index="b2w_sellers_gmail*")
            elastich_control = False
        except Exception as e:
            print(e)
    for d in data:
        email_list.append(d['_source'])
    elastich_control = True
    while elastich_control is True:
        try:
            data = el.get_data(query=subject, index="magalu_sellers_gmail*")
            elastich_control = False
        except Exception as e:
            print(e)
    for d in data:
        email_list.append(d['_source'])

    elastich_control = True

if len(email_list) >= 1:
    csv_file = convert_to_csv(email_list)
    webhook = DiscordWebhook(url=config['DISCORD']['URI'], username="Webhook with files")
    webhook.add_file(file=csv_file, filename=f'seller_emails_{id}_{date}.csv')
    webhook.execute()
    time.sleep(1)
else:
    print('Sem emails')


##############
# Exportar lista com os e-mails de SAC p/ csv
##############

get_sac_mails(elastic=el,date=date,df=df1, url=config['DISCORD']['URI'])



