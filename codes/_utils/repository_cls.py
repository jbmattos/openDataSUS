# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 19:46:52 2021

@author: jubma

Class containing the repository structure
"""

from datetime import date, datetime

class Repository():
    
    def __init__(self):
        self.__datestamp = date.today().strftime('%Y%m%d')
        # paths
        self.__this_path = None # read pathlib (directory or file?) (replace \ > /)
        self._root = "openDataSUS"
        self._root_path = self.__this_path.split(self.__root)[0] + self.__root + "/"
        self._data_path = self.root_path + "data/"
        self._code_path = self.root_path + "code/"
        self._proc_data_path = self._root_path + "process_data_{}/".format(self.__datestamp)
        # paths: base dictionary of types
        self._baseDic_path = self.root_path + "data/__data_dictionary/"
        self._baseDic_file = self._dataDic_path + "_srag_orig_dicionario.json"
        
        # data sets identifiers
        self._db20_name = "srag20"
        self._db21_name = "srag21"
        self._db_file_mask = "{}.??"                # define the extension
        self._db_tempfile_mask = "temp_{}.??"       # define the extension
        
        # processing (input) files
        self._db20dic = self._db20_name + "_dictionary.json"
        self._db21dic = self._db21_name + "_dictionary.json"
        self._column_selec = "_srag_colSelection.json"
        self._clinical_feat = "_srag_clinicalFeat.json"
        self._feat_replace = "_srag_clinicalFeatReplace.json"
        self._feat_regex = "_srag_featRegex.json"
        self._feat_unification = "_srag_featUnification.json"
        
        # (saving) log files
    
    