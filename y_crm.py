#!/usr/bin/env python
# coding: utf-8




#!pip install fake-useragent



print('импорт пакетов')
import pandas as pd
import json
import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
from datetime import datetime
from datetime import date, timedelta
import time
import numpy as np
from datetime import datetime as dt
import pandas.io.formats.format as pf
import contextlib
import os
import csv
import calendar

from pandas import Series,DataFrame
import datetime
from datetime import date, timedelta
from pandas import concat 

from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import pandas as pd
import time
from datetime import datetime as dt


#Функции вывода Датафрейма
pd.set_option('display.max_columns',None)
pd.set_option('display.expand_frame_repr',False)
pd.set_option('max_colwidth',-1)


print('скачивание xml')

import xml.etree.ElementTree as ET
f=open(r'c:\Users\asus2\OneDrive\pt\dataset\yves-rosher\new_offer\feed.yml',"wb")
ufr = requests.get("https://transport.productsup.io/50c60dc625d0324522d4/channel/125869/pdsfeed.yml")
f.write(ufr.content)
f.close()
tree = ET.parse("feed.yml")
root = tree.getroot()


print('парсинг xml')

start_time = dt.now()
root = tree.getroot()
df = pd.DataFrame()
data=[]
i=0
while i <= len(root[0][6].findall("offer")):
    try:
        data=[root[0][6][i][0].text, 
              root[0][6][i][1].text]
        df = df.append(pd.DataFrame(data).T, ignore_index=True)
        print(i)
        i = i+1
    except:
        i = i+1
df.columns = ['url_utm','price']
df_new = df.url_utm.str.split('?', 0, expand=True)
df['url']= df_new[0]
print(dt.now() - start_time)


print('сохранение временного датасета из xml')


df.to_csv('offer_temp.csv', index=False, header=True, sep=';', encoding='utf-8')


print('чтение и сравнение версий')

start_time = dt.now()
x = pd.read_csv('offer.csv', sep=';', encoding='utf8', header=0)
y = pd.read_csv('offer_temp.csv', sep=';', encoding='utf8', header=0)
y['file'] = 'y'
print(dt.now() - start_time)


start_time = dt.now()
z = x.merge(y, on=['url'], how='outer', indicator=True)
z = z[z._merge.str.contains(r'right', case=False)==True]
z = z.drop(['_merge', 'file', 'url_utm_x', 'price_x'], axis=1)
z.columns = ['url','url_utm','price']
print(dt.now() - start_time)
z.to_csv('z.csv', index=False, header=True, sep=';', encoding='utf-8')
z


itog = z.append(x, ignore_index=False)
itog.to_csv('offer.csv', index=False, header=True, sep=';', encoding='utf-8')


z = z.reset_index(drop=True)
z


print('парсинг информации с сайта')
i = 0
name = []
while i <= len(z)-1:
    UserAgent().chrome
    url = z['url'][i]
    response = requests.get(url, headers={'User-Agent': UserAgent().chrome})
    response
    html = response.text
    html
    soup = BeautifulSoup(html) 
    offer_name = soup.findAll('h1')
    offer_name
    name.append({'url': url, 'название': offer_name})
    i = i+1
df_name = pd.DataFrame(name)
df_name


print('формирование в датасет')

z = z.merge(df_name, on=['url'] , how='outer')
z.to_csv(r'new_offer.csv', index=False, header=True, sep=';', encoding='cp1251')
z.head()


print('рассылка')
import smtplib

from email.mime.text import MIMEText
from email.header import Header

# Настройки
mail_sender = 'xxx@gmail.com'
mail_receiver = ['xxx@yandex.ru']

username = 'x@gmail.com'
password = 'xxx'
server = smtplib.SMTP('smtp.gmail.com:587')

# Формируем тело письма
subject = u'xxx'

forday = """ <html> <head></head><p> новые товары ( https://1drv.ms/xxx ): <body> {0} </body> </html> """.format(z.to_html())
msg = MIMEText(forday, 'html') 
msg['Subject'] = Header(subject, 'utf-8')

# Отпавляем письмо
server.starttls()
server.ehlo()
server.login(username, password)
server.sendmail(mail_sender, mail_receiver, msg.as_string())
server.quit()





