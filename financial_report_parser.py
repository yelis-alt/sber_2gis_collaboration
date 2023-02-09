#ЗАГРУЗКА БИБЛИОТЕК# 
from xml.etree import ElementTree as ET
from datetime import datetime
import pandas as pd
import os
import lib

#ФОРМИРОВАНИЕ ПУТИ К КАТАЛОГАМ#
inFolder = "./in/"
outFilePath_full = "./out/girbo_full " + datetime.today().strftime('%Y-%m-%d').replace('-', "") + ".csv"
dictionaryFilePath = "./dictionary.txt"
progress = 0.0
inFiles = os.listdir(inFolder)
progressFile = 100.0 / len(inFiles)
dictionary = lib.read_dictionary(dictionaryFilePath)

#СОРТИРОВКА БУХГАЛТЕРСКИЙ СТАТЕЙ ПО ВОЗРАСТАНИЮ#
dictionary = pd.DataFrame(dictionary.items())
dictionary[0] = dictionary[0].astype('int64')
dictionary = dictionary.replace({1600:1099, 1700:1299, 13001:1301, 13002:1302}, regex=True)
dictionary = dictionary.sort_values(by = 0)
dictionary[0] = dictionary[0].astype('str')
dictionary = dict(zip(dictionary[0], dictionary[1]))

#СОЗДАНИЕ СЛОВАРЯ ДЛЯ ПОЛНЫХ ОТЧЁТНОСТЕЙ#
dictionary_full = dictionary.copy()
for del_full in dictionary.keys():
    if del_full in ['1151', '1171', '1231', '1301', '1302',
                    '1411', '1451', '1511', '1551', '2121', '2416']:
        dictionary_full.pop(del_full)

#ФОРМИРОВАНИЕ ЗАГОЛОВКА ТАБЛИЦЫ#
writer = lib.ResultsWriter(outFilePath_full)
writer.clear()
writer.open()
line = "ИдФайл" + ";" + "КНД" + ";" + "КНД" + ";" + "ДатаДок" + ";" + "СвНП" + ";" + "НПЮЛ" + ";" + "НПЮЛ" + ";"

#ФУНКЦИЯ ОТБОРА КЛЮЧЕЙ СЛОВАРЯ ПО ЗНАЧЕНИЯМ#
def get_key(val):
    for key, value in dictionary.items():
         if val == value:
             return key

#ФОРМИРОВАНИЕ ЗАГОЛОВКА ТАБЛИЦЫ#
for ln in dictionary_full.values():
    if int(get_key(ln)) < 2000:
        line += (ln + ";")*3
    else:
        line += (ln + ";")*2
line += "\n"
writer.write(line)
line = "xml;Тип;ОтчетГод;ГГГГММДД;ОКВЭД2;ИННЮЛ;НаимОрг"
for val in dictionary_full.values():
    if int(get_key(val)) < 2000:
        line += ";" + "СумОтч"
        line += ";" + "СумПрдщ"
        line += ";" + "СумПрдшв"
    else:
        line += ";" + "СумОтч"
        line += ";" + "СумПред"
line += "\n"
writer.write(line)

#ВЫДЕЛЕНИЕ СЛОВАРЯ ДЛЯ ВНЕОБОРОТНЫХ АКТИВОВ#
pos_vneob = -2
add_vneob = -1
vneob_sec = []
vneob_list = ['НематАкт', 'РезИсслед', 'НеМатПоискАкт', 'МатПоискАкт', 'ОснСр', 'ВлМатЦен', 'ФинВлож', 'ОтлНалАкт', 'ПрочВнеОбАкт']
for i in vneob_list:
    if i in list(dictionary_full.values()):
        vneob_sec.append(i)
if 'ФинВлож' in vneob_sec and '1170' not in list(dictionary_full.keys()):
    vneob_sec.remove('ФинВлож')

#ВЫДЕЛЕНИЕ СЛОВАРЯ ДЛЯ ОБОРОТНЫХ АКТИВОВ#
pos_ob = -2
add_ob = -1
ob_sec = []
ob_list = ['Запасы', 'НДСПриобрЦен', 'ДебЗад', 'ФинВлож', 'ДенежнСр', 'ПрочОба']
for i in ob_list:
    if i in list(dictionary_full.values()):
        ob_sec.append(i)
if 'ФинВлож' in ob_sec and '1240' not in list(dictionary_full.keys()):
    ob_sec.remove('ФинВлож')

#ВЫДЕЛЕНИЕ СЛОВАРЯ ДЛЯ КАПИТАЛА И РЕЗЕРВОВ#
pos_kap = -2
add_kap = -1
kap_sec = []
kap_list = ['УставКапитал', 'СобствАкции', 'ПереоцВнеОбА', 'ДобКапитал', 'РезКапитал', 'НераспПриб']
for i in kap_list:
    if i in list(dictionary_full.values()):
        kap_sec.append(i)

#ВЫДЕЛЕНИЕ СЛОВАРЯ ДЛЯ ДОЛГОСРОЧНЫХ ОБЯЗАТЕЛЬСТВ#
pos_dolob = -2
add_dolob = -1
dolob_sec = []
dolob_list = ['ЗаемСредств', 'ОтложНалОбяз', 'ОценОбяз', 'ПрочОбяз']
for i in dolob_list:
    if i in list(dictionary_full.values()):
        dolob_sec.append(i)
if 'ЗаемСредств' in dolob_sec and '1410' not in list(dictionary_full.keys()):
    dolob_sec.remove('ЗаемСредств')
if 'ОценОбяз' in dolob_sec and '1430' not in list(dictionary_full.keys()):
    dolob_sec.remove('ОценОбяз')
if 'ПрочОбяз' in dolob_sec and '1450' not in list(dictionary_full.keys()):
    dolob_sec.remove('ПрочОбяз')

#ВЫДЕЛЕНИЕ СЛОВАРЯ ДЛЯ КРАТКОСРОЧНЫХ ОБЯЗАТЕЛЬСТВ#
pos_krob = -2
add_krob = -1
krob_sec = []
krob_list = ['ЗаемСредств', 'КредитЗадолж', 'ДоходБудущ', 'ОценОбяз', 'ПрочОбяз']
for i in krob_list:
    if i in list(dictionary_full.values()):
        krob_sec.append(i)
if 'ЗаемСредств' in krob_sec and '1510' not in list(dictionary_full.keys()):
    krob_sec.remove('ЗаемСредств')
if 'ОценОбяз' in krob_sec and '1540' not in list(dictionary_full.keys()):
    krob_sec.remove('ОценОбяз')
if 'ПрочОбяз' in krob_sec and '1550' not in list(dictionary_full.keys()):
    krob_sec.remove('ПрочОбяз')

#ВЫДЕЛЕНИЕ СЛОВАРЯ ДЛЯ ФИНАНСОВЫХ РЕЗУЛЬТАТОВ#
pos_fin = -2
add_fin = -1
fin_sec = []
fin_list = ['Выруч', 'СебестПрод', 'ВаловаяПрибыль', 'КомРасход', 'УпрРасход', 'ПрибПрод']
fin_list += ['ДоходОтУчаст', 'ПроцПолуч', 'ПроцУпл', 'ПрочДоход', 'ПрочРасход', 'ПрибУбДоНал']
fin_list += ['НалПриб', 'ТекНалПриб', 'ОтложНалПриб', 'ПостНалОбяз', 'ИзмНалОбяз', 'ИзмНалАктив']
fin_list += ['Прочее', 'ЧистПрибУб', 'РезПрцВОАНеЧист', 'РезПрОпНеЧис', 'НалПрибОпНеЧист', 'СовФинРез']
fin_list += ['БезПрибылАкц', 'РазводПрибылАкц']
for i in fin_list:
    if i in list(dictionary_full.values()):
        fin_sec.append(i)

#ЗАПОЛНЕНИЕ ТЕКСТОВЫХ ПОЛЕЙ#
for file in inFiles: #УРОВЕНЬ ФАЙЛА
    xml = ET.parse(inFolder + file)
    xmlRoot = xml.getroot()
    idFile = xmlRoot.get("ИдФайл")

    if idFile[slice(0,8)] == "NO_BUHOT": #УСЛОВИЕ ПОЛНОЙ БУХГАЛТЕРСКОЙ ОТЧЁТНОСТИ
        line = idFile + ";"

        for doc in xmlRoot.findall("Документ"): #УРОВЕНЬ ДОКУМЕНТА
            docType = doc.attrib.get("КНД").__str__().replace("0710099", "Полная")
            docYear = doc.attrib.get("ОтчетГод").__str__()
            docDate = doc.attrib.get("ДатаДок").__str__().replace(".", "")
            line += docType + ";" + docYear + ";" + docDate + ";"

            for org in doc.findall("СвНП"): #УРОВЕНЬ СвНП
                docOrg = org.attrib.get("ОКВЭД2").__str__().replace(".", "")
                line += docOrg + ";"

                for cod in org.findall("НПЮЛ"): #УРОВЕНЬ НПЮЛ
                    docInn = cod.attrib.get("ИННЮЛ").__str__()
                    docName = cod.attrib.get("НаимОрг").__str__().replace(";", ",")
                    line += docInn + ";" + docName + ";"

        #ЗАПОЛНЕНИЕ ПОКАЗАТЕЛЕЙ#
        for bal in doc.findall("Баланс"): #УРОВЕНЬ БАЛАНСА

            for acti in bal.findall('Актив'): # УРОВЕНЬ АКТИВА
                if '1099' in dictionary.keys():
                    acti_cur = acti.attrib.get("СумОтч").__str__().replace("None", "")
                    acti_past = acti.attrib.get("СумПрдщ").__str__().replace("None", "")
                    acti_prepast = acti.attrib.get("СумПрдшв").__str__().replace("None", "")
                    line += acti_cur + ";" + acti_past + ";" + acti_prepast + ";"

                n_vneob = 0 #Проверка существования внеоборотных активов
                for vneob in acti.findall('ВнеОбА'): #УРОВЕНЬ ВНЕОБОРОТНЫХ АКТИВОВ
                    n_vneob += 1
                    if '1100' in dictionary.keys():
                        vneob_cur = vneob.attrib.get("СумОтч").__str__().replace("None", "")
                        vneob_past = vneob.attrib.get("СумПрдщ").__str__().replace("None", "")
                        vneob_prepast = vneob.attrib.get("СумПрдшв").__str__().replace("None", "")
                        line += vneob_cur + ";" + vneob_past + ";" + vneob_prepast + ";"
                        add_vneob = 0
                    else:
                        add_vneob = -1

                    k_vneob = -1 #Счётчик внеоборотных активов
                    for ind in vneob_sec:
                        for vneob_con in vneob.findall(ind): #УРОВЕНЬ СОДЕРЖИМОГО ВНЕОБОРОТНЫХ АКТИВОВ
                            k_vneob += 1
                            pos_vneob = vneob_sec.index(vneob_con.__str__().split('at')[0][10:-2])
                            if pos_vneob == 0:
                                vneob_con_cur = vneob_con.attrib.get("СумОтч").__str__().replace("None", "")
                                vneob_con_past = vneob_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                vneob_con_prepast = vneob_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += vneob_con_cur + ";" + vneob_con_past + ";" + vneob_con_prepast + ";"
                            else:
                                for i in range(pos_vneob - k_vneob):
                                    line += "" + ";" + "" + ";" + "" + ";"
                                    k_vneob += 1
                                vneob_con_cur = vneob_con.attrib.get("СумОтч").__str__().replace("None", "")
                                vneob_con_past = vneob_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                vneob_con_prepast = vneob_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += vneob_con_cur + ";" + vneob_con_past + ";" + vneob_con_prepast + ";"
                                k_vneob += 1

                    if k_vneob <= 0:
                        for i in range(len(vneob_sec) - k_vneob - 1):
                            line += "" + ";" + "" + ";" + "" + ";"
                    else:
                        for i in range(len(vneob_sec) - k_vneob):
                            line += "" + ";" + "" + ";" + "" + ";"
                    if (len(vneob_sec) == k_vneob) & (pos_vneob != len(vneob_sec) - 1):
                        line += "" + ";" + "" + ";" + "" + ";"
                if n_vneob == 0:
                    for i in range(len(vneob_sec) + 1 + add_vneob):
                        line += "" + ";" + "" + ";" + "" + ";"

                n_ob = 0 #Проверка существования оборотных активов
                for ob in acti.findall('ОбА'): #УРОВЕНЬ ОБОРОТНЫХ АКТИВОВ
                    n_ob += 1
                    if '1200' in dictionary.keys():
                        ob_cur = ob.attrib.get("СумОтч").__str__().replace("None", "")
                        ob_past = ob.attrib.get("СумПрдщ").__str__().replace("None", "")
                        ob_prepast = ob.attrib.get("СумПрдшв").__str__().replace("None", "")
                        line += ob_cur + ";" + ob_past + ";" + ob_prepast + ";"
                        add_ob = 0
                    else:
                        add_ob = -1

                    k_ob = -1 #Счётчик оборотных активов
                    for ind in ob_sec:
                        for ob_con in ob.findall(ind): #УРОВЕНЬ СОДЕРЖИМОГО ОБОРОТНЫХ АКТИВОВ
                            k_ob += 1
                            pos_ob = ob_sec.index(ob_con.__str__().split('at')[0][10:-2])
                            if pos_ob == 0:
                                ob_con_cur = ob_con.attrib.get("СумОтч").__str__().replace("None", "")
                                ob_con_past = ob_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                ob_con_prepast = ob_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += ob_con_cur + ";" + ob_con_past + ";" + ob_con_prepast + ";"
                            else:
                                for i in range(pos_ob - k_ob):
                                    line += "" + ";" + "" + ";" + "" + ";"
                                    k_ob += 1
                                ob_con_cur = ob_con.attrib.get("СумОтч").__str__().replace("None", "")
                                ob_con_past = ob_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                ob_con_prepast = ob_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += ob_con_cur + ";" + ob_con_past + ";" + ob_con_prepast + ";"
                                k_ob += 1

                    if k_ob <= 0:
                        for i in range(len(ob_sec) - k_ob - 1):
                            line += "" + ";" + "" + ";" + "" + ";"
                    else:
                        for i in range(len(ob_sec) - k_ob):
                            line += "" + ";" + "" + ";" + "" + ";"
                    if (len(ob_sec) == k_ob) & (pos_ob != len(ob_sec) - 1):
                        line += "" + ";" + "" + ";" + "" + ";"
                if n_ob == 0:
                    for i in range(len(ob_sec) + 1 + add_ob):
                        line += "" + ";" + "" + ";" + "" + ";"

            for pasi in bal.findall('Пассив'): # УРОВЕНЬ ПАССИВА
                if '1299' in dictionary.keys():
                    pasi_cur = pasi.attrib.get("СумОтч").__str__().replace("None", "")
                    pasi_past = pasi.attrib.get("СумПрдщ").__str__().replace("None", "")
                    pasi_prepast = pasi.attrib.get("СумПрдшв").__str__().replace("None", "")
                    line += pasi_cur + ";" + pasi_past + ";" + pasi_prepast + ";"

                n_kap = 0 #Проверка существования капитала и резервов
                for kap in pasi.findall('КапРез'): #УРОВЕНЬ КАПИТАЛА И РЕЗЕРОВ
                    n_kap += 1
                    if '1300' in dictionary.keys():
                        kap_cur = kap.attrib.get("СумОтч").__str__().replace("None", "")
                        kap_past = kap.attrib.get("СумПрдщ").__str__().replace("None", "")
                        kap_prepast = kap.attrib.get("СумПрдшв").__str__().replace("None", "")
                        line += kap_cur + ";" + kap_past + ";" + kap_prepast + ";"
                        add_kap = 0
                    else:
                        add_kap = -1

                    k_kap = -1 #Счётчик капитала и резервов
                    for ind in kap_sec:
                        for kap_con in kap.findall(ind): #УРОВЕНЬ СОДЕРЖИМОГО КАПИТАЛА И РЕЗЕРВОВ
                            k_kap += 1
                            pos_kap = kap_sec.index(kap_con.__str__().split('at')[0][10:-2])
                            if pos_kap == 0:
                                kap_con_cur = kap_con.attrib.get("СумОтч").__str__().replace("None", "")
                                kap_con_past = kap_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                kap_con_prepast = kap_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += kap_con_cur + ";" + kap_con_past + ";" + kap_con_prepast + ";"
                            else:
                                for i in range(pos_kap - k_kap):
                                    line += "" + ";" + "" + ";" + "" + ";"
                                    k_kap += 1
                                kap_con_cur = kap_con.attrib.get("СумОтч").__str__().replace("None", "")
                                kap_con_past = kap_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                kap_con_prepast = kap_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += kap_con_cur + ";" + kap_con_past + ";" + kap_con_prepast + ";"
                                k_kap += 1

                    if k_kap <= 0:
                        for i in range(len(kap_sec) - k_kap - 1):
                            line += "" + ";" + "" + ";" + "" + ";"
                    else:
                        for i in range(len(kap_sec) - k_kap):
                            line += "" + ";" + "" + ";" + "" + ";"
                    if (len(kap_sec) == k_kap) & (pos_kap != len(kap_sec) - 1):
                        line += "" + ";" + "" + ";" + "" + ";"
                if n_kap == 0:
                    for i in range(len(kap_sec) + 1 + add_kap):
                        line += "" + ";" + "" + ";" + "" + ";"

                n_dolob = 0 #Проверка существования долгосрочных обязательств
                for dolob in pasi.findall('ДолгосрОбяз'): #УРОВЕНЬ ДОЛГОСРОЧНЫХ ОБЯЗАТЕЛЬСТВ
                    n_dolob += 1
                    if '1400' in dictionary.keys():
                        dolob_cur = dolob.attrib.get("СумОтч").__str__().replace("None", "")
                        dolob_past = dolob.attrib.get("СумПрдщ").__str__().replace("None", "")
                        dolob_prepast = dolob.attrib.get("СумПрдшв").__str__().replace("None", "")
                        line += dolob_cur + ";" + dolob_past + ";" + dolob_prepast + ";"
                        add_dolob = 0
                    else:
                        add_dolob = -1

                    k_dolob = -1 #Cчётчик долгосрочных обязательств
                    for ind in dolob_sec:
                        for dolob_con in dolob.findall(ind): #УРОВЕНЬ СОДЕРЖИМОГО ДОЛГОСРОЧНЫХ ОБЯЗАТЕЛЬСТВ
                            k_dolob += 1
                            pos_dolob = dolob_sec.index(dolob_con.__str__().split('at')[0][10:-2])
                            if pos_dolob == 0:
                                dolob_con_cur = dolob_con.attrib.get("СумОтч").__str__().replace("None", "")
                                dolob_con_past = dolob_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                dolob_con_prepast = dolob_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += dolob_con_cur + ";" + dolob_con_past + ";" + dolob_con_prepast + ";"
                            else:
                                for i in range(pos_dolob - k_dolob):
                                    line += "" + ";" + "" + ";" + "" + ";"
                                    k_dolob += 1
                                dolob_con_cur = dolob_con.attrib.get("СумОтч").__str__().replace("None", "")
                                dolob_con_past = dolob_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                dolob_con_prepast = dolob_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += dolob_con_cur + ";" + dolob_con_past + ";" + dolob_con_prepast + ";"
                                k_dolob += 1

                    if k_dolob <= 0:
                        for i in range(len(dolob_sec) - k_dolob - 1):
                            line += "" + ";" + "" + ";" + "" + ";"
                    else:
                        for i in range(len(dolob_sec) - k_dolob):
                            line += "" + ";" + "" + ";" + "" + ";"
                    if (len(dolob_sec) == k_dolob) & (pos_dolob != len(dolob_sec) - 1):
                        line += "" + ";" + "" + ";" + "" + ";"
                if n_dolob == 0:
                    for i in range(len(dolob_sec) + 1 + add_dolob):
                        line += "" + ";" + "" + ";" + "" + ";"

                n_krob = 0 #Проверка существования краткосрочных обязательств
                for krob in pasi.findall('КраткосрОбяз'): #УРОВЕНЬ КРАТКОСРОЧНЫХ ОБЯЗАТЕЛЬСТВ
                    print('df')
                    n_krob += 1
                    if '1500' in dictionary.keys():
                        krob_cur = krob.attrib.get("СумОтч").__str__().replace("None", "")
                        krob_past = krob.attrib.get("СумПрдщ").__str__().replace("None", "")
                        krob_prepast = krob.attrib.get("СумПрдшв").__str__().replace("None", "")
                        line += krob_cur + ";" + krob_past + ";" + krob_prepast + ";"
                        add_krob = 0
                    else:
                        add_krob = -1

                    k_krob = -1 #Cчётчик краткосрочных обязательств
                    for ind in krob_sec:
                        for krob_con in krob.findall(ind): #УРОВЕНЬ СОДЕРЖИМОГО КРАТКОСРОЧНЫХ ОБЯЗАТЕЛЬСТВ
                            k_krob += 1
                            pos_krob = krob_sec.index(krob_con.__str__().split('at')[0][10:-2])
                            if pos_krob == 0:
                                krob_con_cur = krob_con.attrib.get("СумОтч").__str__().replace("None", "")
                                krob_con_past = krob_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                krob_con_prepast = krob_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += krob_con_cur + ";" + krob_con_past + ";" + krob_con_prepast + ";"
                            else:
                                for i in range(pos_krob - k_krob):
                                    line += "" + ";" + "" + ";" + "" + ";"
                                    k_krob += 1
                                krob_con_cur = krob_con.attrib.get("СумОтч").__str__().replace("None", "")
                                krob_con_past = krob_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                                krob_con_prepast = krob_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                                line += krob_con_cur + ";" + krob_con_past + ";" + krob_con_prepast + ";"
                                k_krob += 1

                    if k_krob <= 0:
                        for i in range(len(krob_sec) - k_krob - 1):
                            line += "" + ";" + "" + ";" + "" + ";"
                    else:
                        for i in range(len(krob_sec) - k_krob):
                            line += "" + ";" + "" + ";" + "" + ";"
                    if (len(krob_sec) == k_krob) & (pos_krob != len(krob_sec) - 1):
                        line += "" + ";" + "" + ";" + "" + ";"
                if n_krob == 0:
                    for i in range(len(krob_sec) + 1 + add_krob):
                        line += "" + ";" + "" + ";" + "" + ";"

        for fin in doc.findall('ФинРез'): # УРОВЕНЬ ФИНАНСОВЫХ РЕЗУЛЬТАТОВ
            k_fin = -1 #Cчётчик краткосрочных обязательств
            for ind in fin_sec:
                for fin_con in fin.findall(ind): #УРОВЕНЬ СОДЕРЖИМОГО ФИНАНСОВЫХ РЕЗУЛЬТАТОВ
                    k_fin += 1
                    pos_fin = fin_sec.index(fin_con.__str__().split('at')[0][10:-2])
                    if pos_fin == 0:
                        fin_con_cur = fin_con.attrib.get("СумОтч").__str__().replace("None", "")
                        fin_con_past = fin_con.attrib.get("СумПред").__str__().replace("None", "")
                        line += fin_con_cur + ";" + fin_con_past + ";"
                    else:
                        for i in range(pos_fin - k_fin):
                            line += "" + ";" + "" + ";"
                            k_fin += 1
                        fin_con_cur = fin_con.attrib.get("СумОтч").__str__().replace("None", "")
                        fin_con_past = fin_con.attrib.get("СумПред").__str__().replace("None", "")
                        line += fin_con_cur + ";" + fin_con_past + ";"

            if k_fin <= 0:
                for i in range(len(fin_sec) - k_fin - 1):
                    line += "" + ";" + "" + ";"
            else:
                for i in range(len(fin_sec) - k_fin - 1):
                    line += "" + ";" + "" + ";"
            if (len(fin_sec) == k_fin) & (pos_fin != len(fin_sec) - 1):
                line += "" + ";" + "" + ";"

        #ПЕРЕМЕЩЕНЕИЕ ПО НОВЫМ СТРОКАМ#
        line += "\n"
        writer.write(line)
    progress += progressFile
    print("Progress (full_reports): " + f'{progress:.4f}%')
writer.close()

###############################################################################################################################################################

#ФОРМИРОВАНИЕ ПУТИ К КАТАЛОГАМ#
outFilePath_simple = "./out/girbo_simplified " + datetime.today().strftime('%Y-%m-%d').replace('-', "") + ".csv"
progress = 0.0

#СОЗДАНИЕ СЛОВАРЯ ДЛЯ ПОЛНЫХ ОТЧЁТНОСТЕЙ#
dictionary_simple = dictionary.copy()
for del_simple in dictionary.keys():
    if del_simple in ['1110', '1120', '1130', '1140', '1150', '1160', '1170',
                      '1180', '1190', '1100', '1220', '1230', '1240', '1260',
                      '1200', '1310', '1320', '1340', '1350', '1360', '1370',
                      '1300', '1410', '1420', '1430', '1450', '1400', '1510',
                      '1530', '1540', '1550', '1500', '2120', '2130', '2210',
                      '2220', '2230', '2310', '2320', '2360', '2410', '2411',
                      '2412', '2460', '2510', '2520', '2530', '2540', '2900', '2910']:
        dictionary_simple.pop(del_simple)

#ФОРМИРОВАНИЕ ЗАГОЛОВКА ТАБЛИЦЫ#
writer = lib.ResultsWriter(outFilePath_simple)
writer.clear()
writer.open()
line = "ИдФайл" + ";" + "КНД" + ";" + "КНД" + ";" + "ДатаДок" + ";" + "СвНП" + ";" + "НПЮЛ" + ";" + "НПЮЛ" + ";"

#ФОРМИРОВАНИЕ ЗАГОЛОВКА ТАБЛИЦЫ#
for ln in dictionary_simple.values():
    if int(get_key(ln)) < 2000:
        line += (ln + ";")*3
    else:
        line += (ln + ";")*2
line += "\n"
writer.write(line)
line = "xml;Тип;ОтчетГод;ГГГГММДД;ОКВЭД2;ИННЮЛ;НаимОрг"
for val in dictionary_simple.values():
    if int(get_key(val)) < 2000:
        line += ";" + "СумОтч"
        line += ";" + "СумПрдщ"
        line += ";" + "СумПрдшв"
    else:
        line += ";" + "СумОтч"
        line += ";" + "СумПред"
line += "\n"
writer.write(line)

#ВЫДЕЛЕНИЕ СЛОВАРЯ ДЛЯ АКТИВОВ#
pos_acti = -2
acti_sec = []
acti_list = ['МатВнеАкт', 'НеМатФинАкт', 'Запасы', 'ФинВлож', 'ДенежнСр']
for i in acti_list:
    if i in list(dictionary_simple.values()):
        acti_sec.append(i)

#ВЫДЕЛЕНИЕ СЛОВАРЯ ДЛЯ ПАССИВОВ#
pos_pasi = -2
pasi_sec = []
pasi_list = ['КапРез', 'ЦелевСредства', 'ФондИмущИнЦФ', 'ДлгЗаемСредств', 'ДрДолгосрОбяз', 'КртЗаемСредств', 'КредитЗадолж', 'ДрКраткосрОбяз']
for i in pasi_list:
    if i in list(dictionary_simple.values()):
        pasi_sec.append(i)

#ВЫДЕЛЕНИЕ СЛОВАРЯ ДЛЯ ФИНАНСОВЫХ РЕЗУЛЬТАТОВ#
pos_fin = -2
fin_sec = []
fin_list = ['Выруч', 'РасхОбДеят', 'ПроцУпл', 'ПрочДоход', 'ПрочРасход', 'НалПрибДох', 'ЧистПрибУб']
for i in fin_list:
    if i in list(dictionary_simple.values()):
        fin_sec.append(i)

#ЗАПОЛНЕНИЕ ТЕКСТОВЫХ ПОЛЕЙ#
for file in inFiles: #УРОВЕНЬ ФАЙЛА
    xml = ET.parse(inFolder + file)
    xmlRoot = xml.getroot()
    idFile = xmlRoot.get("ИдФайл")
    if idFile[slice(0,8)] == "NO_BOUPR": #УСЛОВИЕ УПРОЩЁННОЙ БУХГАЛТЕРСКОЙ ОТЧЁТНОСТИ
        line = idFile + ";"
        for doc in xmlRoot.findall("Документ"): #УРОВЕНЬ ДОКУМЕНТА
            docType = doc.attrib.get("КНД").__str__().replace("0710096", "Упрощённая")
            docYear = doc.attrib.get("ОтчетГод").__str__()
            docDate = doc.attrib.get("ДатаДок").__str__().replace(".", "")
            line += docType + ";" + docYear + ";" + docDate + ";"
            for org in doc.findall("СвНП"): #УРОВЕНЬ СвНП
                docOrg = org.attrib.get("ОКВЭД2").__str__().replace(".", "")
                line += docOrg + ";"
                for cod in org.findall("НПЮЛ"): #УРОВЕНЬ НПЮЛ
                    docInn = cod.attrib.get("ИННЮЛ").__str__()
                    docName = cod.attrib.get("НаимОрг").__str__().replace(";", ",")
                    line += docInn + ";" + docName + ";"

        #ЗАПОЛНЕНИЕ ПОКАЗАТЕЛЕЙ#
        for bal in doc.findall("Баланс"): #УРОВЕНЬ БАЛАНСА
            for acti in bal.findall('Актив'): # УРОВЕНЬ АКТИВА
                if '1099' in dictionary_simple.keys():
                    acti_cur = acti.attrib.get("СумОтч").__str__().replace("None", "")
                    acti_past = acti.attrib.get("СумПрдщ").__str__().replace("None", "")
                    acti_prepast = acti.attrib.get("СумПрдшв").__str__().replace("None", "")
                    line += acti_cur + ";" + acti_past + ";" + acti_prepast + ";"

                k_acti = -1
                for ind in acti_sec:
                    for acti_con in acti.findall(ind): #УРОВЕНЬ СОДЕРЖИМОГО АКТИВОВ
                        k_acti += 1
                        pos_acti = acti_sec.index(acti_con.__str__().split('at')[0][10:-2])
                        if pos_acti == 0:
                            acti_con_cur = acti_con.attrib.get("СумОтч").__str__().replace("None", "")
                            acti_con_past = acti_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                            acti_con_prepast = acti_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                            line += acti_con_cur + ";" + acti_con_past + ";" + acti_con_prepast + ";"
                        else:
                            for i in range(pos_acti - k_acti):
                                line += "" + ";" + "" + ";" + "" + ";"
                                k_acti += 1
                            acti_con_cur = acti_con.attrib.get("СумОтч").__str__().replace("None", "")
                            acti_con_past = acti_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                            acti_con_prepast = acti_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                            line += acti_con_cur + ";" + acti_con_past + ";" + acti_con_prepast + ";"

                if k_acti <= 0:
                    for i in range(len(acti_sec) - k_acti - 1):
                        line += "" + ";" + "" + ";" + "" + ";"
                else:
                    for i in range(len(acti_sec) - k_acti - 1):
                        line += "" + ";" + "" + ";" + "" + ";"
                if (len(acti_sec) == k_acti) & (pos_acti != len(acti_sec) - 1):
                    line += "" + ";" + "" + ";" + "" + ";"

            for pasi in bal.findall('Пассив'): # УРОВЕНЬ ПАССИВА
                if '1299' in dictionary_simple.keys():
                    pasi_cur = pasi.attrib.get("СумОтч").__str__().replace("None", "")
                    pasi_past = pasi.attrib.get("СумПрдщ").__str__().replace("None", "")
                    pasi_prepast = pasi.attrib.get("СумПрдшв").__str__().replace("None", "")
                    line += pasi_cur + ";" + pasi_past + ";" + pasi_prepast + ";"

                k_pasi = -1
                for ind in pasi_sec:
                    for pasi_con in pasi.findall(ind): #УРОВЕНЬ СОДЕРЖИМОГО ПАССИВОВ
                        k_pasi += 1
                        pos_pasi = pasi_sec.index(pasi_con.__str__().split('at')[0][10:-2])
                        if pos_pasi == 0:
                            pasi_con_cur = pasi_con.attrib.get("СумОтч").__str__().replace("None", "")
                            pasi_con_past = pasi_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                            pasi_con_prepast = pasi_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                            line += pasi_con_cur + ";" + pasi_con_past + ";" + pasi_con_prepast + ";"
                        else:
                            for i in range(pos_pasi - k_pasi):
                                line += "" + ";" + "" + ";" + "" + ";"
                                k_pasi += 1
                            pasi_con_cur = pasi_con.attrib.get("СумОтч").__str__().replace("None", "")
                            pasi_con_past = pasi_con.attrib.get("СумПрдщ").__str__().replace("None", "")
                            pasi_con_prepast = pasi_con.attrib.get("СумПрдшв").__str__().replace("None", "")
                            line += pasi_con_cur + ";" + pasi_con_past + ";" + pasi_con_prepast + ";"

                if k_pasi <= 0:
                    for i in range(len(pasi_sec) - k_pasi - 1):
                        line += "" + ";" + "" + ";" + "" + ";"
                else:
                    for i in range(len(pasi_sec) - k_pasi - 1):
                        line += "" + ";" + "" + ";" + "" + ";"
                if (len(pasi_sec) == k_pasi) & (pos_pasi != len(pasi_sec) - 1):
                    line += "" + ";" + "" + ";" + "" + ";"

        for fin in doc.findall('ФинРез'): # УРОВЕНЬ ФИНАНСОВЫХ РЕЗУЛЬТАТОВ
            k_fin = -1 #Cчётчик краткосрочных обязательств
            for ind in fin_sec:
                for fin_con in fin.findall(ind): #УРОВЕНЬ СОДЕРЖИМОГО ФИНАНСОВЫХ РЕЗУЛЬТАТОВ
                    k_fin += 1
                    pos_fin = fin_sec.index(fin_con.__str__().split('at')[0][10:-2])
                    if pos_fin == 0:
                        fin_con_cur = fin_con.attrib.get("СумОтч").__str__().replace("None", "")
                        fin_con_past = fin_con.attrib.get("СумПред").__str__().replace("None", "")
                        line += fin_con_cur + ";" + fin_con_past + ";"
                    else:
                        for i in range(pos_fin - k_fin):
                            line += "" + ";" + "" + ";"
                            k_fin += 1
                        fin_con_cur = fin_con.attrib.get("СумОтч").__str__().replace("None", "")
                        fin_con_past = fin_con.attrib.get("СумПред").__str__().replace("None", "")
                        line += fin_con_cur + ";" + fin_con_past + ";"

            if k_fin <= 0:
                for i in range(len(fin_sec) - k_fin - 1):
                    line += "" + ";" + "" + ";"
            else:
                for i in range(len(fin_sec) - k_fin - 1):
                    line += "" + ";" + "" + ";"
            if (len(fin_sec) == k_fin) & (pos_fin != len(fin_sec) - 1):
                line += "" + ";" + "" + ";"

        #ПЕРЕМЕЩЕНИЕ ПО НОВЫМ СТРОКАМ#
        line += "\n"
        writer.write(line)
    progress += progressFile
    print("Progress (simplified reports): " + f'{progress:.4f}%')
writer.close()

#ОБЪЕДИНЕНИЕ ФАЙЛОВ ПОЛНОЙ И УПРОЩЁННОЙ ОТЧЁТНОСТИ#
print()
print("Fusion of files in progress...")
print()

full = pd.read_csv(outFilePath_full, sep = ';', header = None).iloc[:,:-1]
full_dp = [a if not (s:=sum(j == a for j in list(full.iloc[0,:])[:i]))
           else f'{a}_{s}' for i, a in enumerate(list(full.iloc[0,:]))]
full.iloc[0,:] = full_dp.copy()
full.columns = [full.iloc[0].values, full.iloc[1].values]
full = full.iloc[2:].reset_index(drop = True)

simple = pd.read_csv(outFilePath_simple, sep = ';', header = None).iloc[:,:-1]
simple_dp = [a if not (s:=sum(j == a for j in list(simple.iloc[0,:])[:i]))
             else f'{a}_{s}' for i, a in enumerate(list(simple.iloc[0,:]))]
simple.iloc[0,:] = simple_dp.copy()
simple.columns = [simple.iloc[0].values, simple.iloc[1].values]
simple = simple.iloc[2:].reset_index(drop = True)

all = pd.concat([full, simple], axis = 0)
outFilePath = "./out/girbo " + datetime.today().strftime('%Y-%m-%d').replace('-', "") + ".csv"
all.to_csv(outFilePath, sep=';', index = False, encoding = 'utf-8')

print("Fusion of files completed.")
