# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 19:46:52 2021

@author: jubma

Class containing the repository structure
"""

import json
import os

from datetime import date, datetime

class Repository():
    
    def __init__(self, check_dic=True):
        self.__datestamp = date.today().strftime('%Y%m%d')
        # path config: root
        self.__this_file_path = str(os.path.dirname(__file__)).replace('\\','/') + '/' + str(os.path.basename(__file__))
        self._root = "openDataSUS"
        self._root_path = self.__this_file_path.split(self._root)[0] + self._root + "/"
        self._data_path = self._root_path + "data/"
        self._code_path = self._root_path + "code/"
        self._proc_data_path = self._root_path + "process_data_{}/".format(self.__datestamp)
        # path config: data
        self._db20_name = "INFLUD-28-06-2021"
        self._db21_name = "INFLUD21-28-06-2021"
        self._db_file_mask = self._data_path + "{}.csv"             # define the extension
        self._db_tempfile_mask = self._data_path + "temp_{}.??"     # define the extension
        self.__use_temp = False
        self.__compress = None                                      # define extension for compressing
        # paths: data/ dictionary of types
        self.__dataDic_path = self._root_path + "data/__data_dictionary/"
        self.__logfile_dictionaries = self._data_path + '/__data_dictionary/_log.txt'
        self.__baseDic_file = self.__dataDic_path + "_srag_orig_dicionario.json"
        self.__procDic_remove = self.__dataDic_path + "feat2remove_basedic.json"
        self.__procDic_add20 = self.__dataDic_path + "add_2020.json"
        self.__procDic_add21 = self.__dataDic_path + "add_2021.json"
        
        # processing: openDataSUS files
        self._db20dic = self._data_path + self._db20_name + "_dictionary.json"
        self._db21dic = self._data_path + self._db21_name + "_dictionary.json"
        self._column_selec = "_srag_colSelection.json"
        self._clinical_feat = "_srag_clinicalFeat.json"
        self._feat_replace = "_srag_clinicalFeatReplace.json"
        self._feat_regex = "_srag_featRegex.json"
        self._feat_unification = "_srag_featUnification.json"
        
        # initialise repository
        if check_dic:
            self.__check_data_dictionaries()
    
    @property
    def root(self):
        return self._root
    
    @property
    def db20_name(self):
        return self._db20_name
    
    @property
    def db21_name(self):
        return self._db21_name
    
    @property
    def db20_file(self):
        if not self.__use_temp:
            return self._db_file_mask.format(self._db20_name)
        else:
            return self._db_tempfile_mask(self._db20_name)
    
    @property
    def db21_file(self):
        if not self.__use_temp:
            return self._db_file_mask.format(self._db21_name)
        else:
            return self._db_tempfile_mask(self._db21_name)    
    
    def __check_data_dictionaries(self):
        '''
        This function checks whether is necessary to generate the 
        data-dictionary files
        '''
        
        exists20 = os.path.exists(self._db20dic)
        exists21 = os.path.exists(self._db21dic)
        if not exists20 or not exists21:
            self.__generate_dictionaries()
        else:
            print('\n>> verified files for data dictionaries')
        return
    
    def __generate_dictionaries(self):
        '''
        This function generates the data-dictionary files
        '''
        
        def _log(dic20_path,dic21_path):
            dic20_path = self._root + dic20_path.split(self._root)[1]
            dic21_path = self._root + dic21_path.split(self._root)[1]
            
            with open(self.__logfile_dictionaries, 'w') as f:
                f.write('>> "_dictionary.json" files generated in {}'.format(datetime.now().strftime("%Y-%m-%d %H:%m:%S")))
                f.write('\n.. path of execution: {}'.format(self.__this_file_path))
                f.write('\n.. input base file: {}'.format(self.__baseDic_file))
                f.write('\n.. generated file: {}'.format(dic20_path))
                f.write('\n.. generated file: {}'.format(dic21_path))
                f.write('\n(description) Dictionary of features for the data files with same names')
            return
        
        # SRAG ORIGINAL DICTIONARY
        with open(self.__baseDic_file,'r') as f:
            SRAG_DIC = json.load(f)
        
        # keys to remove from the original SRAG_DIC (constructed by Renato) >> those feat are not in the new DBS(20/21)
        with open(self.__procDic_remove,'r') as f:
            remove_from_dic = json.load(f)
            
        new_srag = SRAG_DIC.copy()
        for feat in remove_from_dic:
            del new_srag[feat]
        
        # new features to add to DB-2020 (and not in the original SRAG_DIC)
        # verify: category with only code [1]: can NaN be "ignorado"?
        with open(self.__procDic_add20,'r') as f:
            add_dic_2020 = json.load(f)
            
        # new features to add to DB-2021 (and not in the original SRAG_DIC nor in DB-2020)
        with open(self.__procDic_add21,'r') as f:
            add_dic_2021 = json.load(f)
        
        ########
        # Save new Dictionary of Features 
        # for srag2020 and srag2021
        ########
        
        dic_srag20 = {**new_srag, **add_dic_2020}
        dic_srag21 = {**dic_srag20, **add_dic_2021}
        
        with open(self._db20dic,'w') as f:
            json.dump(dic_srag20, f, indent=1)
        with open(self._db21dic,'w') as f:
            json.dump(dic_srag21,f, indent=1)
    
        _log(self._db20dic,self._db21dic)
        print('\n>> generated data dictionary files')
        return
    
    def __get_opendatasus(self):
        '''
        Downloads the data sets from the opendatasus
        (save downloaded data as temp)
        Returns
        -------
        None.

        '''
        
        self.__use_temp = True
        return


if __name__ == '__main__':
    repo = Repository()