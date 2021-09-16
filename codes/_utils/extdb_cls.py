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


class ExtDB(Repository):
    
    def __init__(self):
        super().__init__()
        self.__this_path = self._extDB_path
        
        # /data/__external_databases/ibge/
		
		# /data/__external_databases/atlasbrasil-org/
		
		# /data/__external_databases/dados-gov/
		
		# /data/__external_databases/opendata-sus/
        
        