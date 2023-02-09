#БИБЛИОТЕКИ# 
from requests.structures import CaseInsensitiveDict
from datetime import datetime
import pandas as pd
import requests

start = datetime.now()

#ПОЛУЧЕНИЕ ТОКЕНА#
url = 'https://gp.mos.ru/services/EhdIntegration/token'
username = '###########'
password = '############'
response = ''
token = ''
data = {
        'host': url,
        'Conten-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'grant_type': 'password',
        'username': username,
        'password': password
       }

response = requests.post(url, data=data)
token = response.json()['access_token']

#ИЗЪЯТИЕ ДАННЫХ#
tables = [] #агрегатор таблиц
headers = CaseInsensitiveDict() #формирование формы запроса на получение данных
headers["Accept"] = "application/json"
headers["Authorization"] = f"Bearer {token}"
errors = pd.DataFrame(columns = ['name']) #Датафрейм ошибок запросов

#########################################################################################################################################

#Количество действующих индивидуальных предпринимателей, сведения о которых содержатся в ЕГРИП:
numpages = "https://gp.mos.ru/EhdIntegration/api/DataPages/?id_src=306&id_ind=50" #получение общего числа страниц в таблице
numpages = requests.get(numpages, headers=headers).json()[0]
url = "https://gp.mos.ru/EhdIntegration/api/DimensionTree/?id_src=306&id_dim=DIM_1295" #получение всех категорий из ЕХД
resp = pd.DataFrame(requests.get(url, headers=headers).json())
resp['name'] = resp['name'].str.lower()
inds = {}
inds = dict(zip(pd.DataFrame(resp).id, pd.DataFrame(resp).name))
url = "https://gp.mos.ru/EhdIntegration/api/DimensionTree/?id_src=306&id_dim=DIM_369" #получение всех разрезов из ЕХД
resp = pd.DataFrame(requests.get(url, headers=headers).json())
resp['name'] = resp['name'].str.lower()
dim = {}
dim = dict(zip(pd.DataFrame(resp).id, pd.DataFrame(resp).name))
pred_dei = pd.DataFrame()

print('................................................................................................')
print('..Начало загрузки показателей количества действующих ИП, сведения о которых содержатся в ЕГРИП..')
print('................................................................................................')
ex = 0

for page in range(numpages): #постраничное считывание данных
    try:
        npage = page + 1
        url = f"https://gp.mos.ru/EhdIntegration/api/Data/?id_src=306&id_ind=50&page={npage}"
        resp = pd.DataFrame(requests.get(url, headers=headers).json())
        resp = resp[(resp['DIM_1293'] == 2.0) & #отбор нужных измерений и преобразование полей
                    (resp['DIM_1743'] == 50.0) & (resp['DIM_1294'] == 1.0) &
                    (resp['DIM_1297'] == 22.0)][['DIM_369', 'DIM_1743','DIM_1295', 'DT', 'VL']]
        pred_dei = pd.concat([pred_dei, resp], axis = 0).dropna(axis = 1, how = 'all')
        if (npage % 10 == 1) & (npage != 11):
            print(f"Показатели количества действующих ИП, сведения о которых содержатся в ЕГРИП: загружена {npage} страница")
        elif ((npage % 10) in [2, 3, 4]) & (npage not in [12, 13, 14]):
            print(f"Показатели количества действующих ИП, сведения о которых содержатся в ЕГРИП: загружены {npage} страницы")
        else:
            print(f"Показатели количества действующих ИП, сведения о которых содержатся в ЕГРИП: загружено {npage} страниц")
    except:
        ex += 1

error_row = f'Число невыгруженных страниц в показателях количества действующих ИП, сведения о которых содержатся в ЕГРИП: {ex}'
errors = pd.concat([errors, pd.DataFrame.from_records({'name': [error_row]})], axis = 0, ignore_index = True)

pred_dei['DIM_1743'] = pred_dei['DIM_1743'].replace(50.0, 'количество действующих ИП, сведения о которых содержатся в ЕГРИП')
pred_dei['DIM_1295'] = pred_dei['DIM_1295'].replace(inds)
pred_dei['DIM_369'] = pred_dei['DIM_369'].replace(dim)
pred_dei['VL'][pred_dei['VL'] == '-'] = float('0.0')
pred_dei['VL']= pred_dei['VL'].str.replace(' ', '').str.replace(',', '.').str.replace('%', '').astype('float64')
pred_dei = pred_dei.groupby(by=['DIM_369', 'DIM_1743', 'DIM_1295', 'DT']).min().reset_index()
pred_dei.columns = ['DIM_5', 'DIM_1', 'DIM_2', 'date', 'val']
pred_dei['DIM_4'] = 'ед.'
pred_dei['DIM_3'] = 'null'
pred_dei = pred_dei[['DIM_1', 'DIM_2', 'DIM_3', 'DIM_4', 'DIM_5', 'date', 'val']]

tables.append(pred_dei) #запись таблицы в агрегатор

print('..........................................................................................')
print('..Показатели количества действующих ИП, сведения о которых содержатся в ЕГРИП, загружены..')
print('..........................................................................................')
print()

###########################################################################################################################################

#Количество зарегистрированных индивидуальных предпринимателей в ЕГРИП:
numpages = "https://gp.mos.ru/EhdIntegration/api/DataPages/?id_src=306&id_ind=9950" #получение общего числа страниц в таблице
numpages = requests.get(numpages, headers=headers).json()[0]
url = "https://gp.mos.ru/EhdIntegration/api/DimensionTree/?id_src=306&id_dim=DIM_1295" #получение всех категорий из ЕХД
resp = pd.DataFrame(requests.get(url, headers=headers).json())
resp['name'] = resp['name'].str.lower()
inds = {}
inds = dict(zip(pd.DataFrame(resp).id, pd.DataFrame(resp).name))
url = "https://gp.mos.ru/EhdIntegration/api/DimensionTree/?id_src=306&id_dim=DIM_369" #получение всех разрезов из ЕХД
resp = pd.DataFrame(requests.get(url, headers=headers).json())
resp['name'] = resp['name'].str.lower()
dim = {}
dim = dict(zip(pd.DataFrame(resp).id, pd.DataFrame(resp).name))
pred_reg = pd.DataFrame()

print('........................................................................')
print('..Начало загрузки показателей количества зарегистрированных ИП в ЕГРИП..')
print('........................................................................')
ex = 0

for page in range(numpages): #постраничное считывание данных
    try:
        npage = page + 1
        url = f"https://gp.mos.ru/EhdIntegration/api/Data/?id_src=306&id_ind=9950&page={npage}"
        resp = pd.DataFrame(requests.get(url, headers=headers).json())
        resp = resp[(resp['DIM_1293'] == 2.0) & #отбор нужных измерений и преобразование полей
                    (resp['DIM_1743'] == 9950.0) & (resp['DIM_1294'] == 1.0) &
                    (resp['DIM_1297'] == 22.0)][['DIM_369', 'DIM_1743','DIM_1295', 'DT', 'VL']]
        pred_reg = pd.concat([pred_reg, resp], axis = 0).dropna(axis = 1, how = 'all')
        if (npage % 10 == 1) & (npage != 11):
            print(f"Показатели количества зарегистрированных ИП в ЕГРИП: загружена {npage} страница")
       elif ((npage % 10) in [2, 3, 4]) & (npage not in [12, 13, 14]):
            print(f"Показатели количества зарегистрированных ИП в ЕГРИП: загружены {npage} страницы")
        else:
            print(f"Показатели количества зарегистрированных ИП в ЕГРИП: загружено {npage} страниц")
    except:
        ex += 1

error_row = f'Число невыгруженных страниц в показателях количества зарегистрированных ИП в ЕГРИП: {ex}'
errors = pd.concat([errors, pd.DataFrame.from_records({'name': [error_row]})], axis = 0, ignore_index = True)

pred_reg['DIM_1743'] = pred_reg['DIM_1743'].replace(9950.0, 'количество зарегистрированных ИП в ЕГРИП')
pred_reg['DIM_1295'] = pred_reg['DIM_1295'].replace(inds)
pred_reg['DIM_369'] = pred_reg['DIM_369'].replace(dim)
pred_reg['VL'][pred_reg['VL'] == '-'] = float('0.0')
pred_reg['VL'] = pred_reg['VL'].str.replace(' ', '').str.replace(',', '.').str.replace('%', '').astype('float64')
pred_reg = pred_reg.groupby(by=['DIM_369', 'DIM_1743', 'DIM_1295', 'DT']).min().reset_index()
pred_reg.columns = ['DIM_5', 'DIM_1', 'DIM_2', 'date', 'val']
pred_reg['DIM_4'] = 'ед.'
pred_reg['DIM_3'] = 'null'
pred_reg = pred_reg[['DIM_1', 'DIM_2', 'DIM_3', 'DIM_4', 'DIM_5', 'date', 'val']]

tables.append(pred_reg) #запись таблицы в агрегатор

print('..................................................................')
print('..Показатели количества зарегистрированных ИП в ЕГРИП, загружены..')
print('..................................................................')
print()

###########################################################################################################################################

#Количество прекративших деятельность индивидуальных предпринимателей, сведения о которых внесены в ЕГРИП:
numpages = "https://gp.mos.ru/EhdIntegration/api/DataPages/?id_src=306&id_ind=53" #получение общего числа страниц в таблице
numpages = requests.get(numpages, headers=headers).json()[0]
url = "https://gp.mos.ru/EhdIntegration/api/DimensionTree/?id_src=306&id_dim=DIM_1295" #получение всех категорий из ЕХД
resp = pd.DataFrame(requests.get(url, headers=headers).json())
resp['name'] = resp['name'].str.lower()
inds = {}
inds = dict(zip(pd.DataFrame(resp).id, pd.DataFrame(resp).name))
url = "https://gp.mos.ru/EhdIntegration/api/DimensionTree/?id_src=306&id_dim=DIM_369" #получение всех разрезов из ЕХД
resp = pd.DataFrame(requests.get(url, headers=headers).json())
resp['name'] = resp['name'].str.lower()
dim = {}
dim = dict(zip(pd.DataFrame(resp).id, pd.DataFrame(resp).name))
pred_prek = pd.DataFrame()

print('...........................................................................................................')
print('..Начало загрузки показателей количества прекративших деятельность ИП, сведения о которых внесены в ЕГРИП..')
print('...........................................................................................................')
ex = 0

for page in range(numpages): #постраничное считывание данных
    try:
        npage = page + 1
        url = f"https://gp.mos.ru/EhdIntegration/api/Data/?id_src=306&id_ind=53&page={npage}"
        resp = pd.DataFrame(requests.get(url, headers=headers).json())
        resp = resp[(resp['DIM_1293'] == 2.0) & #отбор нужных измерений и преобразование полей
                    (resp['DIM_1743'] == 53.0) & (resp['DIM_1296'] == 1.0) &
                    (resp['DIM_1297'] == 22.0)][['DIM_369', 'DIM_1743','DIM_1295', 'DT', 'VL']]
        pred_prek = pd.concat([pred_prek, resp], axis = 0).dropna(axis = 1, how = 'all')
        if (npage % 10 == 1) & (npage != 11):
            print(f"Показатели количества прекративших деятельность ИП, сведения о которых внесены в ЕГРИП: загружена {npage} страница")
        elif ((npage % 10) in [2, 3, 4]) & (npage not in [12, 13, 14]):
            print(f"Показатели количества прекративших деятельность ИП, сведения о которых внесены в ЕГРИП: загружены {npage} страницы")
        else:
            print(f"Показатели количества прекративших деятельность ИП, сведения о которых внесены в ЕГРИП: загружено {npage} страниц")
    except:
        ex += 1

error_row = f'Число невыгруженных страниц в показателях количества прекративших деятельность ИП, сведения о которых внесены в ЕГРИП: {ex}'
errors = pd.concat([errors, pd.DataFrame.from_records({'name': [error_row]})], axis = 0, ignore_index = True)

pred_prek['DIM_1743'] = pred_prek['DIM_1743'].replace(53.0, 'количество прекративших деятельность ИП, сведения о которых внесены в ЕГРИП')
pred_prek['DIM_1295'] = pred_prek['DIM_1295'].replace(inds)
pred_prek['DIM_369'] = pred_prek['DIM_369'].replace(dim)
pred_prek['VL'][pred_prek['VL'] == '-'] = float('0.0')
pred_prek['VL']= pred_prek['VL'].str.replace(' ', '').str.replace(',', '.').str.replace('%', '').astype('float64')
pred_prek = pred_prek.groupby(by=['DIM_369', 'DIM_1743', 'DIM_1295', 'DT']).min().reset_index()
pred_prek.columns = ['DIM_5', 'DIM_1', 'DIM_2', 'date', 'val']
pred_prek['DIM_4'] = 'ед.'
pred_prek['DIM_3'] = 'null'
pred_prek = pred_prek[['DIM_1', 'DIM_2', 'DIM_3', 'DIM_4', 'DIM_5', 'date', 'val']]

tables.append(pred_prek) #запись таблицы в агрегатор

print('.....................................................................................................')
print('..Показатели количества прекративших деятельность ИП, сведения о которых внесены в ЕГРИП, загружены..')
print('.....................................................................................................')
print()

###########################################################################################################################################

#Количество прекративших деятельность индивидуальных предпринимателей, сведения о которых содержатся в ЕГРИП:
numpages = "https://gp.mos.ru/EhdIntegration/api/DataPages/?id_src=306&id_ind=51" #получение общего числа страниц в таблице
numpages = requests.get(numpages, headers=headers).json()[0]
url = "https://gp.mos.ru/EhdIntegration/api/DimensionTree/?id_src=306&id_dim=DIM_1295" #получение всех категорий из ЕХД
resp = pd.DataFrame(requests.get(url, headers=headers).json())
resp['name'] = resp['name'].str.lower()
inds = {}
inds = dict(zip(pd.DataFrame(resp).id, pd.DataFrame(resp).name))
url = "https://gp.mos.ru/EhdIntegration/api/DimensionTree/?id_src=306&id_dim=DIM_369" #получение всех разрезов из ЕХД
resp = pd.DataFrame(requests.get(url, headers=headers).json())
resp['name'] = resp['name'].str.lower()
dim = {}
dim = dict(zip(pd.DataFrame(resp).id, pd.DataFrame(resp).name))
pred_preks = pd.DataFrame()

print('..............................................................................................................')
print('..Начало загрузки показателей количества прекративших деятельность ИП, сведения о которых содержатся в ЕГРИП..')
print('..............................................................................................................')
ex = 0

for page in range(numpages): #постраничное считывание данных
    try:
        npage = page + 1
        url = f"https://gp.mos.ru/EhdIntegration/api/Data/?id_src=306&id_ind=51&page={npage}"
        resp = pd.DataFrame(requests.get(url, headers=headers).json())
        resp = resp[(resp['DIM_1293'] == 2.0) & #отбор нужных измерений и преобразование полей
                    (resp['DIM_1743'] == 51.0) & (resp['DIM_1296'] == 1.0) &
                    (resp['DIM_1297'] == 22.0)][['DIM_369', 'DIM_1743','DIM_1295', 'DT', 'VL']]
        pred_preks = pd.concat([pred_preks, resp], axis = 0).dropna(axis = 1, how = 'all')
        if (npage % 10 == 1) & (npage != 11):
            print(f"Показатели количества прекративших деятельность ИП, сведения о которых содержатся в ЕГРИП: загружена {npage} страница")
        elif ((npage % 10) in [2, 3, 4]) & (npage not in [12, 13, 14]):
            print(f"Показатели количества прекративших деятельность ИП, сведения о которых содержатся в ЕГРИП: загружены {npage} страницы")
        else:
            print(f"Показатели количества прекративших деятельность ИП, сведения о которых содержатся в ЕГРИП: загружено {npage} страниц")
    except:
        ex += 1

error_row = f'Число невыгруженных страниц в показателях количества прекративших деятельность ИП, сведения о которых содержатся в ЕГРИП: {ex}'
errors = pd.concat([errors, pd.DataFrame.from_records({'name': [error_row]})], axis = 0, ignore_index = True)

pred_preks['DIM_1743'] = pred_preks['DIM_1743'].replace(51.0, 'количество прекративших деятельность ИП, сведения о которых содержатся в ЕГРИП')
pred_preks['DIM_1295'] = pred_preks['DIM_1295'].replace(inds)
pred_preks['DIM_369'] = pred_preks['DIM_369'].replace(dim)
pred_preks['VL'][pred_preks['VL'] == '-'] = float('0.0')
pred_preks['VL'][pred_preks['VL'].str.contains('%') == False] = pred_preks['VL'][pred_preks['VL'].str.contains('%') == False].str.replace(' ', '').str.replace(',', '.').astype('float64')/1000
pred_preks['VL'][pred_preks['VL'].str.contains('%') == True] = pred_preks['VL'][pred_preks['VL'].str.contains('%') == True].str.replace(' ', '').str.replace(',', '.').str.replace('%', '').astype('float64')
pred_preks = pred_preks.groupby(by=['DIM_369', 'DIM_1743', 'DIM_1295', 'DT']).min().reset_index()
pred_preks.columns = ['DIM_5', 'DIM_1', 'DIM_2', 'date', 'val']
pred_preks['DIM_4'] = 'ед.'
pred_preks['DIM_3'] = 'null'
pred_preks = pred_preks[['DIM_1', 'DIM_2', 'DIM_3', 'DIM_4', 'DIM_5', 'date', 'val']]

tables.append(pred_preks) #запись таблицы в агрегатор

print('........................................................................................................')
print('..Показатели количества прекративших деятельность ИП, сведения о которых содержатся в ЕГРИП, загружены..')
print('........................................................................................................')
print()

#СОСТЫКОВКА ТАБЛИЦ#
print('.....................................')
print('..Начало процесса состыковки таблиц..')
print('.....................................')
print()
print('...')

k = 0 #счётчик записи датафреймов
for file in tables:
    k += 1
    if k == 1:
        file['val'] = file['val'].astype('str').str.replace('.',',', regex = True)
        file['date'] = pd.to_datetime(file['date'], errors='coerce').dt.date
        file = file.sort_values(by=['DIM_2', 'DIM_3', 'DIM_4', 'DIM_5', 'date']).reset_index(drop = True)
        file['date'] = file['date'].apply(lambda x: x.strftime("%Y-%d-%m"))
        file['date'] = file['date'].str[8:10] + '.' + file['date'].str[5:7] + '.' + file['date'].str[0:4]
        file.to_csv('./result/EHD_EGRIP ' + datetime.today().strftime('%Y-%m-%d').replace('-', "") + ".csv",
                    header = True, index = False, encoding = 'cp1251', mode = 'a')
    else:
        file['val'] = file['val'].astype('str').str.replace('.',',', regex = True)
        file['date'] = pd.to_datetime(file['date'], errors='coerce').dt.date
        file = file.sort_values(by=['DIM_2', 'DIM_3', 'DIM_4', 'DIM_5', 'date']).reset_index(drop = True)
        file['date'] = file['date'].apply(lambda x: x.strftime("%Y-%d-%m"))
        file['date'] = file['date'].str[8:10] + '.' + file['date'].str[5:7] + '.' + file['date'].str[0:4]
        file.to_csv('./result/EHD_EGRIP ' + datetime.today().strftime('%Y-%m-%d').replace('-', "") + ".csv",
                    header = False, index = False, encoding = 'cp1251', mode = 'a')

print()
print('........................................')
print('..Окончание процесса состыковки таблиц..')
print('........................................')
print()

#ПОДСЧЁТ НЕВЫГРУЖЕННЫХ СТРАНИЦ ПО ПОКАЗАТЕЛЯМ#
print('.......................................................')
print('..Начало подсчёта невыгруженных страниц в показателях..')
print('.......................................................')

for row in range(errors.shape[0]):
    print(errors['name'][row])

print('..........................................................')
print('..Окончание подсчёта невыгруженных страниц в показателях..')
print('..........................................................')
print()

#ПОДЧЁТ ВРЕМЕНИ РАБОТЫ ПАРСЕРА#
print('...................................................')
print(f'..Полная продолжительность работы парсера:{str(datetime.now() - start)[:7]}..')
print('...................................................')
print()
