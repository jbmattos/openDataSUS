# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 11:36:48 2021

@author: jubma
"""

from clint.textui import progress
import requests
from bs4 import BeautifulSoup

def __get_data_url(db_ref):
    r = requests.get(__DB_URL[db_ref])
    soup = BeautifulSoup(r.text)
    return soup.find_all('li', class_='resource-item', attrs={'data-id':__DB_ID[db_ref]})[0].find(class_="resource-url-analytics")['href']

__DB_URL = {'srag20': "https://opendatasus.saude.gov.br/dataset/bd-srag-2020",
            'srag21': "https://opendatasus.saude.gov.br/dataset/bd-srag-2021"}

__DB_ID = {'srag20': "d89ea107-4a2b-4bd5-8b8b-fa1caaa96550",
            'srag21': "42bd5e0e-d61a-4359-942e-ebc83391a137"}

url_csv = __get_data_url('srag20')
r = requests.get(url_csv, stream=True)
path = 'srag_test.csv'
with open(path, 'wb') as f:
    total_length = int(r.headers.get('content-length'))
    for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1): 
        if chunk:
            f.write(chunk)
            f.flush()