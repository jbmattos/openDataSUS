# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 19:46:52 2021

@author: jubma

Class containing the repository structure
"""

import errno
import json
import os
import requests
import warnings

from bs4 import BeautifulSoup
from clint.textui import progress
from datetime import date, datetime

warnings.filterwarnings("ignore")

class Repository():
        
    # The data sets encompassed in the Repository
    _DB_REFS = ['srag20', 'srag21']
    
    # All defined dictionaries follow the _DB_REFS in the keys
    # --- Data file names
    __DB_NAME = {'srag20': "srag20", # "INFLUD-28-06-2021",
                 'srag21': "srag21"} # "INFLUD21-28-06-2021"}
    # --- openDataSUS webpages
    __DB_URL = {'srag20': "https://opendatasus.saude.gov.br/dataset/bd-srag-2020",
                'srag21': "https://opendatasus.saude.gov.br/dataset/bd-srag-2021"}
    # --- < data-id > to the SRAG data resource-item
    # soup.find_all('li', class_='resource-item')
    __DB_ID = {'srag20': "d89ea107-4a2b-4bd5-8b8b-fa1caaa96550",
               'srag21': "42bd5e0e-d61a-4359-942e-ebc83391a137"}
    
    def __init__(self, check_dic=True):
        self.__datestamp = date.today().strftime('%Y%m%d')
        # path config: root
        self.__this_file_path = str(os.path.dirname(__file__)).replace('\\','/') + '/' + str(os.path.basename(__file__))
        self.__root = "openDataSUS"
        self.__root_path = self.__this_file_path.split(self.__root)[0] + self.__root + "/"
        self.__data_path = self.__root_path + "data/"
        self.__code_path = self.__root_path + "code/"
        self.__proc_data_path = self.__root_path + "process_data_{}/".format(self.__datestamp)
        
        # path config: /data/
        self.__extension = '.csv'
        self.__db_file_mask = self.__data_path + "{}" + self.__extension             
        self.__db_tempfile_mask = self.__data_path + "temp_{}" + self.__extension
        self.__db_dic_mask = self.__data_path + "{}_dictionary.json"
        self.__logfile_download = self.__data_path + "_log_opendatasus.json"
        self.__log_download = {}
        self.__use_temp = False
        self.__compress = None
        ## data/subfolders
        ## data/__data_dictionary >> generation of dictionary of types
        self.__dataDic_path = self.__root_path + "data/__data_dictionary/"
        self.__logfile_dictionaries = self.__dataDic_path + '_log.txt'
        self.__baseDic_file = self.__dataDic_path + "_srag_orig_dicionario.json"
        self.__procDic_remove = self.__dataDic_path + "srag_2remove_orig.json"
        self.__procDic_add20 = self.__dataDic_path + "srag_2add_2020.json"
        self.__procDic_add21 = self.__dataDic_path + "srag_2add_2021.json"
        ## data/__proc_utils >> processing the openDataSUS files
        self.__procUtils_path = self.__root_path + "data/__proc_utils/"
        self._file_gen_feat = self.__procUtils_path + "_srag_featSelection_genFeat.json"
        self._file_clin_feat = self.__procUtils_path + "_srag_featSelection_clinFeat.json"
        self._file_dt_feat = self.__procUtils_path + "_srag_featSelection_dtFeat.json"
        self._file_demo_feat = self.__procUtils_path + "_srag_featSelection_demoFeat.json"
        self._file_lab_feat = self.__procUtils_path + "_srag_featSelection_labFeat.json"
        self._file_feat_clin2bool = self.__procUtils_path + "_srag_featProc_clinFeat2bool.json"
        self._file_feat_regex = self.__procUtils_path + "_srag_featProc_featRegex.json"
        self._file_feat_unification = self.__procUtils_path + "_srag_featProc_featUnification.json"
        ## data/__external_databases
        ### internal files to the subfolders are defined directly on the <external_database> implemented class 
        self._extDB_path = self.__root_path + "data/__external_databases/"
        
        
        # path config: /process_data_{}/
        self.__proc_file_mask = self.__proc_data_path + "{}_SUSurv" + self.__extension
        self.__proc_dtype_mask = self.__proc_data_path + "{}_SUSurv_dtypes.json"
        self.__logfile_proc = self.__proc_data_path + "_log.json"
        
        # initialise repository
        if check_dic: self.__check_data_dictionaries()
    
    @staticmethod
    def _verify_db(db_ref):
        if not db_ref in Repository._DB_REFS:
            raise ValueError('The data set to process is not defined: {}'.format(db_ref))
        
    @property
    def root(self):
        return self.__root
    
    @property
    def db20_name(self):
        return self._get_name('srag20')
    
    @property
    def db21_name(self):
        return self._get_name('srag21')
    
    @property
    def db20_file(self):
        return self._db_file('srag20')
    
    @property
    def db21_file(self):
        return self._db_file('srag21')
    
    def _get_path(self, _dir):
        
        if _dir == 'root':
            return self.__root_path
        elif _dir == 'data':
            return self.__data_path
        elif _dir == 'code':
            return self.__code_path
        elif _dir == 'proc':
            self.__proc_data_path
        else:
            raise ValueError('Directory not defined: {}'.format(_dir))
    
    def _get_db_datestamp(self, db_ref):
        # retrieve info from Repository log_download
        if db_ref in self.__log_download:
            return self.__log_download[db_ref]['datestamp']
        # retrieve info from Repository data/_log_opendatasus.json file
        elif os.path.exists(self.__logfile_download):
            with open(self.__logfile_download, 'r') as f:
                log = json.load(f)
            if db_ref in log:
                return log[db_ref]['datestamp']
        # handles absence of data datestamp info
        else:
            stamp = date.today().strftime('%Y-%m-%d')
            warnings.warn('Data set download datestamp not provided: used {} instead'.format(stamp))
            return stamp
    
    def _get_name(self, db_ref):
        self._verify_db(db_ref)
        return Repository.__DB_NAME[db_ref]
    
    def __db_file_temp(self, db_ref):
        return self.__db_tempfile_mask.format(Repository.__DB_NAME[db_ref])
    
    def __db_file_final(self, db_ref):
        return self.__db_file_mask.format(Repository.__DB_NAME[db_ref])
    
    def _db_file(self, db_ref):
        self._verify_db(db_ref)
        if not self.__use_temp:
            return self.__db_file_mask.format(Repository.__DB_NAME[db_ref])
        else:
            return self.__db_tempfile_mask.format(Repository.__DB_NAME[db_ref])
    
    def __proc_file(self, db_ref):
        if db_ref=='concat':
            return self.__proc_file_mask.format(Repository.__DB_NAME['srag20']+Repository.__DB_NAME['srag21'])
        else:
            return self.__proc_file_mask.format(Repository.__DB_NAME[db_ref])
    
    def __proc_file_dtypes(self, db_ref):
        if db_ref=='concat':
            return self.__proc_dtype_mask.format(Repository.__DB_NAME['srag20']+Repository.__DB_NAME['srag21'])
        else:
            return self.__proc_dtype_mask.format(Repository.__DB_NAME[db_ref])
    
    def _db_dic(self, db_ref):
        self._verify_db(db_ref)
        return self.__db_dic_mask.format(Repository.__DB_NAME[db_ref])
        
    def _has_db(self, db_ref):
        return os.path.exists(self._db_file(db_ref))
    
    def __check_data_dictionaries(self):
        '''
        This function checks whether is necessary to generate the 
        data-dictionary files
        '''
        
        exists20 = os.path.exists(self._db_dic('srag20'))
        exists21 = os.path.exists(self._db_dic('srag21'))
        if not exists20 or not exists21:
            self.__generate_dictionaries()
        return
    
    def __generate_dictionaries(self):
        '''
        This function generates the data-dictionary files
        '''
        
        def _log(dic20_path,dic21_path):
            dic20_path = self.root + dic20_path.split(self.root)[1]
            dic21_path = self.root + dic21_path.split(self.root)[1]
            
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
        
        # save dictionary json file for srag20
        file20 = self._db_dic('srag20')
        with open(file20,'w') as f:
            json.dump(dic_srag20, f, indent=1)
        # save dictionary json file for srag21
        file21 = self._db_dic('srag21')
        with open(file21,'w') as f:
            json.dump(dic_srag21,f, indent=1)
    
        _log(file20, file21)
        print('\n>> generated data dictionary files')
        return
        
    def _download_opendatasus(self, db_ref):
        '''
        Downloads the data sets from the opendatasus
        (save downloaded data as temp)
        Returns
        -------
        None.

        '''
        
        def get_data_url(db_ref):
            db_url = Repository.__DB_URL[db_ref]
            r = requests.get(db_url)
            soup = BeautifulSoup(r.text, features="lxml")
            
            data_url = soup.find_all('li', class_='resource-item', attrs={'data-id':Repository.__DB_ID[db_ref]})[0].find(class_="resource-url-analytics")['href']
            log = {'file': self._db_file(db_ref),
                   'url': db_url,
                   'head.title': soup.head.title.string,
                   'body.data-site-root': soup.body['data-site-root'],
                   'file-source': data_url,
                   'datestamp': date.today().strftime('%Y-%m-%d')
                  }
            return data_url, log
        
        
        self._verify_db(db_ref)
        print('\n>> downloading {}\n.'.format(db_ref))
        
        url, log = get_data_url(db_ref)
        print('.. generating file: {}'.format(log['file']))
        print('.. url: {}'.format(log['url']))
        print('.. data source: {}'.format(log['file-source']))
        
        self.__log_download[db_ref] = log
        self.__use_temp = True
        file_name = self._db_file(db_ref)
        
        # with no progress bar
        #req = requests.get(url)
        #with open(file_name, 'wb') as f:
        #    f.write(req.content)
        
        # download data with progress bar
        r = requests.get(url, stream=True)
        with open(file_name, 'wb') as f:
            total_length = int(r.headers.get('content-length'))
            for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1): 
                if chunk:
                    f.write(chunk)
                    f.flush()
        return
    
    def _create_proc_folder(self):
        # creates directory for saving proessed data sets and logs 
        if not os.path.exists(os.path.dirname(self.__proc_data_path)):
            try:
                os.makedirs(os.path.dirname(self.__proc_data_path))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        return
    
    def _save_proc_file(self, db, db_ref):
        file_name = self.__proc_file(db_ref)
        dtypes_file = self.__proc_file_dtypes(db_ref)
        
        # save dtypes json file
        with open(dtypes_file, 'w') as f:
            json.dump(db.dtypes.apply(lambda x: x.name).to_dict(),f)
        
        db.to_csv(file_name, index=False, compression=self.__compress, date_format='%Y-%m-%d') 
        print('..saved: {}'.format(file_name))
        return
    
    def _close_repo(self, save_orig=True, proc_log=None):
        
        def rename_files(db_ref):
            old = self.__db_file_temp(db_ref)
            new = self.__db_file_final(db_ref)
            os.rename(old, new)
            print('\n>> saved final original data files')
            return
        
        def remove_files(db_ref):
            file = self.__db_file_temp(db_ref)
            os.remove(file)
            print('\n>> removed temp_ original data files')
            return
        
        if self.__log_download:
            if save_orig:
                # rename temp data files to final files
                for db_ref in self.__log_download:
                    rename_files(db_ref)
                self.__use_temp = False
                # save log from download
                with open(self.__logfile_download, 'w') as f:
                    json.dump(self.__log_download, f)
            else:
                # remove temp files
                for db_ref in self.__log_download:
                    remove_files(db_ref)
        
        if proc_log:
            # add download info to proc_log
            proc_log['crawler'] = self.__log_download
            with open(self.__logfile_proc, 'w') as f:
                json.dump(proc_log, f)
        
        return


if __name__ == '__main__':
    repo = Repository()