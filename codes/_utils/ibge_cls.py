# -*- coding: utf-8 -*-
"""
Created on Mon Sep 13 18:29:55 2021

@author: jubma
"""

import json
import numpy as np
import os
import pandas as pd

from datetime import date, datetime
from repository_cls import Repository


class SUSurv(Repository):
    
    def __init__(self):
        super().__init__()
        self.__this_path = self._extDB_ibge_path
        
        # /data/__external_databases/ibge/
        self.__file_pop_estimation_2020 = "estimativa_dou_2020.xls"
        self.__file_pop_estimation_2021 = "estimativa_dou_2021.xls"
        self.__file_pop_projection = "PROJECOES_2013_POPULACAO.xls"
        self.__file_pnad = "Indicadores_harmonizados_PNAD_1992_2015_Trabalho_e_Rendimento_15_anos_ou_mais.xls"
        self.__file_ind_prepandemic = "Indicadores_moradia_saneamento_2019_20210624.xlsx"
        
        