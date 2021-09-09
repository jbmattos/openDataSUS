# -*- coding: utf-8 -*-
'''
CODE FOR PROCESSING THE SRAG DATABASES FROM OPENDATASUS
[2021-07-19]
    
    THIS CODE AUTOMATICALLY RETRIEVES THE SRAG20 AND SRAG21 DATABASES FROM 
    THE OPENDATASUS SOURCE AND PERFORMS PROCESSING TO ADJUST THE DATA SET 
    INTO SURVIVAL DATA FORMAT
    
    (this script was designed for the databases retrieved in 2021-07-07;
    any changes in the data structure after this data 
    may not be addressed in this pipeline)
    
    EXECUTION: THE SCRIPT SHOULD RUN FROM ANYWERE INSIDE 'openDataSUS' FOLDER
    
    INPUT FILES:  #ISSUES OBS: EXPLAIN FILES
        openDataSUS\\data\\INFLUD-28-06-2021_dictionary.json
        openDataSUS\\data\\INFLUD21-28-06-2021_dictionary.json #ISSUE1: adjust global variable with this name
        openDataSUS\\data\\_srag_colSelection.json
        openDataSUS\\data\\_srag_clinicalFeat.json
        openDataSUS\\data\\_srag_clinicalFeatReplace.json
        openDataSUS\\data\\_srag_featRegex.json
        openDataSUS\\data\\_srag_featUnification.json
                                      
    OUTPUT FILES: [out-folder] < openDataSUS\\process_data_{DATESTAMP} >
        - processed datasets: SRAG20 and SRAG21, or SRAG20-21
        - a '_log.txt' file (include path of this script)
        - ?
    
ADJUSTMENT:
    GENERATE 'PATH' VARIABLE READING FROM SYSTEM THE PATH (UNTIL 'openDataSUS' node)
    SAVE A '_log.txt' FILE INSIDE OUTPUT FOLDER <PATH+\\openDataSUS\\process_data_DATESTAMP> (include path of this script)
'''

import argparse
import json
import numpy as np
import os
import pandas as pd
import warnings

from datetime import date, datetime
from _utils.repository_cls import Repository


class SUSurv(Repository):
    
    def __init__(self):
        super().__init__()
        # DataFrames
        self.__dfs_proc = {}
        self.__df_concat = None
        self.__df_inproc = None
        
        # Data Processing 
        self.surv_status_feat_ = 'survival_status'
        self.surv_time_feat = 'survival_time'
        self.__dt_surv_begin = datetime.strptime('2019-12-01', '%Y-%m-%d') # date considered for the beggining of the Survival Study
        self.__db_datestamp = None
        ## [modifief in] processing in open/close_db_processing methods
        self.__open = False
        self.__db_ref_inproc = None
        self.__surv_event_def = None
        self.__input_cens = None
        ## logs
        self.__log_proc = {'datastamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           'code-path': str(os.path.dirname(__file__)).replace('\\','/') + '/' + str(os.path.basename(__file__)),
                           'concat': None,
                           'save_srag': None
                           }
        self.__log_proc_db = {'event': None,
                              'db_datestamp': None,
                              'case_selection': False,
                              'feat_selection': False,
                              'generate_clinical_feat': False,
                              'survival_features': False,
                             }
    
    def open_db_processing(self, db_ref, event='obitoUTI', input_cens=True):
        self._verify_db(db_ref)
        # private class attributes definition
        self.__db_ref_inproc = db_ref
        self.__surv_event_def = event
        self.__input_cens = input_cens
        self.__set_log_db(db_ref, event) # create log for db_ref    
        
        if not self._has_db(db_ref):
            self.download_opendatasus(db_ref)
        
        self.__df_inproc = self.load_original_data(db_ref)
        self.__open = True
        return
    
    def close_db_processing(self):
        self.__save_processed_data()
        self.__db_ref_inproc = None
        self.__surv_event_def = None
        self.__input_cens = None
        self.__df_inproc = None
        self.__open = False
        return    
    
    @__db_ref_inproc.setter
    def __db_ref_inproc(self, value):
        if self__open:
            self.close_db_processing(self)
        self.__db_ref_inproc = value
    
    @property
    def event_(self):
        return self.__surv_event_def
    
    @property
    def survival_status_(self):
        if self.__open:
            if not self.__log_proc[self.__db_ref_inproc]['survival_features']:
                self.survival_features()
            return self.__df_inproc[self.surv_status_feat_]
        else:
            return None
                
    @property
    def inproc_data_(self):
        if self.__df_inproc is None:
            self.__df_inproc = self.load_original_data(db_ref)
        return self.__df_inproc
    
    def __set_log_db(self, db_ref, event):
        self.__log_proc[db_ref] = self.__log_proc_db.copy()
        self.__log_proc[db_ref]['event'] = event
        return
    
    def __save_processed_data(self):
        self.__dfs_proc[self.__db_ref_inproc] = self.__df_inproc.copy()
        print('> {} processing done'.format(db_ref))
        return
    
    def __set_concat(self):
        self.__df_concat = pd.concat([df for df in self.__dfs_proc.values()], axis='index', ignore_index=True)
        return
    
    def __default_surv_status(self, df):
        '''
        Returns the default Survival Status definition for SRAG data set

        Parameters
        ----------
        df : pd.DataFrame
            Srag data with at least default feature selection.

        Returns
        -------
        pd.Series
            Survival status for the data population.

        '''
        if self.event_=='obitoUTI':
            return (df['UTI'] == 'sim') | (df['EVOLUCAO'] == 'obito')
        elif self.event_=='obito':
            return (df['EVOLUCAO'] == 'obito')
        elif self.event_=='casosGraves':
            return (df['HOSPITAL'] == 'sim') | (df['UTI'] == 'sim') | (df['EVOLUCAO'] == 'obito')
    
    def __load_db(self, db_ref):
        '''
        This function loads the SRAG .csv file and format the features 
        using external json file comprising a feature dictionary of types.
        
        input-file:
            openDataSUS\data\{db-name}_dictionary.json
            
        ISSUES:
            #ISSUE1: CS_GESTANT feature contain value 0 that is not defined by the openDataSUS Dictionary >> mapped to np.nan
            #ISSUE2: OBES_IMC   feature uses ',' for decimal and pandas reads as string insteada of float >>

        Returns
        -------
        df : pd.DataFrame
            SRAG data.

        '''
        print('.. loading {}'.format(self._db_file(db_ref)))
        
        # dictionary of types
        with open(self._db_dic(db_ref)) as f: 
            dic_srag = json.load(f)    
    
        fbool = lambda x: bool(x)
        conv_bool = {k:fbool for k,v in dic_srag.items() if v['type']=='bool'}
    
        df = pd.read_csv(self._db_file(db_ref),
                         sep=';',
                         encoding='ISO-8859-1',
                         usecols=lambda x: x in dic_srag.keys(),
                         converters=conv_bool,
                         low_memory=False,
                         error_bad_lines=False)
        for k,v in dic_srag.items():
            if v['type'] == 'category':
                if k=="CS_GESTANT": #ISSUE1
                    df[k].mask(df[k] == 0, np.nan, inplace=True)
                df[k] = df[k].astype(v['type'])
                df[k].cat.rename_categories(dict(zip(v['codes'],v['categories'])),inplace=True)
            if k=="OBES_IMC":      #ISSUE2
                df[k] = df[k].str.replace(pat=',', repl='.').astype(float)
        df.fillna(value=np.nan, inplace=True)
        df.insert(0,'id_DB', [self._get_name(db_ref)]*df.shape[0])
        
        self.__db_datestamp = self._get_db_datestamp(db_ref)
        self.__log_proc[self.__db_ref_inproc]['db_datestamp'] = self.__db_datestamp
        return df
    
    def processing_status_(self):
        print(self.__log_proc)
        
    def get_proc_status_(self):
        return self.__log_proc
    
    def load_original_data(self, db_ref):
        return self.__load_db(db_ref)
    
    def case_selection(self, covid_pos=True):
        '''
        This function is responsible for filtering the data set transactions.

        Parameters
        ----------
        covid_pos : TYPE, optional
            Selects the transactions that are COVID positive. The selection is a disjunction of the following conditions:
            (CLASSI_FIN=='covid19'):                 final diagnosis of the case
            (PCR_SARS2==True):                       RT-PCR for SARS-CoV-2 result
            (POS_AN_OUT=='nao' & POS_AN_FLU=='nao'): results of testing other respiratory disease (OUT) and influenza (FLU)
            
            The default is True.

        Returns
        -------
        None.
        (Modifies the private attribute of the dataframe under processing)

        '''
        
        self.__log_proc[self.__db_ref_inproc]['case_selection'] = {'covid_pos': covid_pos} # add here future input args in **kwargs format
        print('.. case selection: {}'.format(self.__log_proc[self.__db_ref_inproc]['case_selection']))
        
        db = self.inproc_data_
        
        if covid_pos:
            db = self.__covid_selection(db)
        # add here future input args selection
        
        self.__df_inproc = db
        return
    
    def __covid_selection(self, db):
        '''
        This function selects the transactions that are COVID positive. 
        The selection is a disjunction of the following conditions:
        (CLASSI_FIN=='covid19'):                 final diagnosis of the case
        (PCR_SARS2==True):                       RT-PCR for SARS-CoV-2 result
        (POS_AN_OUT=='nao' & POS_AN_FLU=='nao'): results of testing other respiratory disease (OUT) and influenza (FLU)
        '''        
        return db[(db.CLASSI_FIN=='covid19') | (db.AN_SARS2==True) | (db.PCR_SARS2==True) | (db.RES_IGM == SIM ) | (db.RES_IGA == SIM )].copy()
    
    def feat_selection(self, clin_feat_gen=False, demo_feat=False, lab_feat=False):
        '''
        This function is responsible for feature selection. By default, the data includes 
        general info, date features and the SRAG original clinical features.
        The selected features are predefined in the external .json files:
        
            openDataSUS\data\_srag_featSelection_genFeat.json  (general info feats)
            openDataSUS\data\_srag_featSelection_dtFeat.json   (date feats)
            openDataSUS\data\_srag_featSelection_clinFeat.json (clinical feats)
        
        Parameters
        ----------
        clin_feat_gen : bool, optional
            Whether to generate clinical features by processing text features using predefined regex patterns.
            If True, executes SUSurv.generate_clinical_feat()
            The default is False.
        demo_feat : bool, optional
            Whether to include demographic features from SRAG data set
            The default is False.
        lab_feat : bool, optional (NOT IMPLEMENTED YET)
            Whether to include laboratorial features from SRAG data set
            containing test results for other viral coinfections
            The default is False.
            
        Returns
        -------
        None.
        (Modifies the private attribute of the dataframe under processing)

        '''
        
        self.__log_proc[self.__db_ref_inproc]['feat_selection'] = {'clin_feat_gen': clin_feat_gen, 
                                                                   'demo_feat': demo_feat, 
                                                                   'lab_feat': lab_feat}
        print('.. feature selection: {}'.format(self.__log_proc[self.__db_ref_inproc]['feat_selection']))
        
        # default info
        default_feat = ["id_DB"]
        with open(self._file_gen_feat, 'r') as f: 
            default_feat += json.load(f)
        with open(self._file_dt_feat, 'r') as f: 
            default_feat += json.load(f)
        with open(self._file_clin_feat, 'r') as f: 
            default_feat += json.load(f)
        
        if demo_feat:
            with open(self._file_demo_feat, 'r') as f: 
                default_feat += json.load(f)
        if lab_feat:
            with open(self._file_lab_feat, 'r') as f: 
                default_feat += json.load(f)        
        
        df = self.inproc_data_
        df = df[default_feat]
        
        # generate new clinical features from text search and feature unification
        if clin_feat_gen:
            df = self.generate_clinical_feat(df)
        
        self.__df_inproc = df
        return
    
    def generate_clinical_feat(self, df, combine=True):
        '''
        This function generates the boolean clinical features by:
            - processing the < 'OUTRO_DES','MORB_DESC' > text features using preseted regex patterns 
            - unifying the identified (regex) patterns with the openDataSUS original clinical features
            - unifying some strategic clinical features into single one 
        
        Parameters
        ----------
        df : pd.DataFrame
            Dataframe containing the original clinical features to be processed into new ones.
        combine : bool, optional
            Wheter to combine some (pre-defined) strategic clinical features into single one. 
            The default is True.

        Returns
        -------
        pd.DataFrame
            Data containing the final generated clinical features.

        '''
        
        self.__log_proc[self.__db_ref_inproc]['generate_clinical_feat'] = {'combine': True}
        print('.. clinical features generation from text processing ({})'.format(self.__log_proc[self.__db_ref_inproc]['generate_clinical_feat']))
        
        # existent clinical features >> adjustments
        with open(self._file_clin_feat, 'r') as f: 
            clin_feat = json.load(f)
        with open(self._file_feat_clin2bool, 'r') as f: 
            dic_replace = json.load(f)
        with open(self._file_feat_regex, mode='rt') as f:
            feat_regex = json.load(f)
        with open(self._file_feat_unification, mode='rt') as f:
            feat_unif = json.load(f)
        
        df = df.copy()
        
        # values adjustments
        df.CS_GESTANT = df.CS_GESTANT.astype(str)
        df.replace(dic_replace, inplace=True)
        
        # new clinical features >> text search
        text_feat = ['OUTRO_DES','MORB_DESC']
        textSearch = df[text_feat].fillna('').agg(' '.join, axis=1).replace(r'^\s*$', np.nan, regex=True)
        textSearch = textSearch.str.lower()
        df.drop(columns=text_feat, inplace=True)
        
        # add newly generated feats from regex search
        for feature in feat_regex.keys(): 
            df[feature] = textSearch.str.contains(feat_regex[feature], case=False, regex=True)
        # unifying generated features with existent srag features
        for feat_srag, feat in feat_unif:
            df[feat] = df[[feat,feat_srag]].fillna('').astype(str).agg(''.join, axis=1).replace(r'^\s*$', np.nan, regex=True).str.contains('True', case=False)
        # add original features for which no regex was implemented
        df['doenca_hematologica'] = df['HEMATOLOGI']
        df['sindrome_down'] = df['SIND_DOWN']
        df.drop(columns=clin_feat, inplace=True)
        
        # set clinical features to boolean dtype
        df = df.astype(dict.fromkeys([feat for feat in df.columns if feat.islower()],'boolean'))   
        
        # generate combined clinical features
        if combine:
            df['alteracao_olfato_paladar'] = (df['alteracao_olfato']==True) | (df['alteracao_paladar']==True)
            df['alteracao_respiratoria'] = (df['disturbios_respiratorios']==True) | (df['cianose']==True) | (df['saturacao_menor_95']==True)
            df['sintoma_gripal'] = (df['congestao_nasal']==True) | (df['coriza']==True) | (df['espirro']==True) | (df['cefaleia']==True) | (df['dor_de_garganta']==True) | (df['dores_corpo']==True) | (df['adinamia']==True) | (df['mal_estar']==True) | (df['dor_olhos']==True)
            df['nausea_vomito'] = (df['nausea']==True) | (df['vomito']==True)
        
        upper_cases = {col: col.upper() for col in df.columns}
        df.rename(columns=upper_cases, inplace=True)
        return df
    
    def add_ibge_info(self):
        pass
    
    def survival_features(self):
        '''
        Generate the Time and Status Survival features.
        Pipeline:
            - process date features to datetype 
            - set the begin/end study dates
            - set default survival status
            - input censoring date to missing end-study dates (optional)
            - set survival times dropping missing values

        Parameters
        ----------

        Returns
        -------
        None.
        (Modifies the private attribute of the dataframe under processing)

        '''
        
        self.__log_proc[self.__db_ref_inproc]['survival_features'] = True
        print('.. survival features generation')
        
        dt_end = datetime.strptime(self.__db_datestamp, '%Y-%m-%d')
        df = self.inproc_data_
        df = self.__date_process(df) # transform dt_features into date type
        df = self.__set_study_dates(df, dt_end)
        
        # Feature <survival_status>
        df[self.surv_status_feat_] = self.__default_surv_status(df)
        if self.__input_cens:
            df = self.__handle_censoring(df)
        
        # Feature <survival_time>
        df[self.surv_time_feat] = (df.DT_END - df.DT_BEGIN).dt.days
        df.dropna(axis='index', subset=[self.surv_time_feat], inplace=True)     # drop <survival_time=NaN> (cases: <DT_END=NaN & event=True & dt_censor=NaN> OR <DT_BEGIN=NaN>)
        
        self.__df_inproc = df
        return
    
    def __date_process(self, db):
        '''
        This function process the Date Type features, where different data formats are mapped to %Y-%m-%d.
        The date features are selected from a prefix 'DT_'
        Dates registered with year superior to the current year are mapped to np.nan
        
        FORMATS:
        d/m/yyyy
        d/mm/yyyy
        dd/m/yyyy
        dd/mm/yyyy
        
        ADJUSTMENTS
            1. Insert parameter for additional date features 
        '''
        print('.. date-type formatting')
        
        df = db.copy()
        with open(self._file_dt_feat, 'r') as f: 
            dtFeat = json.load(f)
        dtRegex = {r'^0*$':np.nan,                                # 1) dates with only 0 >> NaN
                   r'^(\d\d)\/(\d\d)\/(\d\d\d\d)': r'\3-\2-\1',   # 2) format dd/mm/yyyy >> Y-m-d
                   r'^(\d)\/(\d)\/(\d\d\d\d)': r'\3-0\2-0\1',     # 3) format d/m/yyyy >> Y-m-d
                   r'^(\d\d)\/(\d)\/(\d\d\d\d)': r'\3-0\2-\1',    # 4) format dd/m/yyyy >> Y-m-d
                   r'^(\d)\/(\d\d)\/(\d\d\d\d)': r'\3-\2-0\1',    # 5) format d/mm/yyyy >> Y-m-d
                   r'^(\d\d\d\d\-\d\d\-\d\d)\s*.*': r'\1'}        # 6) format Y-m-d hh:mm:ss >> Y-m-d
        df.replace(dict.fromkeys(dtFeat,dtRegex), regex=True, inplace=True)
    
        this_year = date.today().year
        for col in dtFeat:
            # year mask
            yearMask = df[col].fillna('').astype(str).apply(lambda value: 
            # (year > this_year) >> NaN
            df[col].mask(yearMask > this_year, np.nan, inplace=True) 
            # type: datetime
            df[col] = pd.to_datetime(df[col], errors='coerce') # columns to datetime: dates yielding overflow are set to NaN ['coerce'] 
        return df
    
    def __set_study_dates(self, df, dt_end):
        '''
        This function generates the beggining and final survival study dates.

        Parameters
        ----------
        df : pd.DataFrame
            Processing dataframe.
        dt_end : datetime
            Final date to consider (data set datestamp) [format: '%Y-%m-%d']

        Returns
        -------
        pd.DataFrame

        '''
        # event definition 
        if self.event_=='casosGraves':
            colsEnd = ['DT_INTERNA', 'DT_ENTUTI', 'DT_EVOLUCA', 'DT_ENCERRA', 'DT_DIGITA']
        elif self.event_=='obitoUTI':
            colsEnd = ['DT_ENTUTI', 'DT_EVOLUCA', 'DT_ENCERRA', 'DT_DIGITA']
        elif self.event_=='obito':
            colsEnd = ['DT_EVOLUCA', 'DT_ENCERRA', 'DT_DIGITA']

        # Features <DT_BEGIN, DT_END>: survival date ref for begin/end of study
        df['DT_BEGIN'] = df[['DT_NOTIFIC', 'DT_SIN_PRI']].apply(lambda row: row[(row >= self.__dt_surv_begin) & (row <= dt_end)].min(), axis='columns')
        df['DT_END'] = df[colsEnd].apply(lambda row: row[(row >= df['DT_BEGIN'].loc[row.name]) & (row <= dt_end)].min(), axis='columns')
        return df

    def __handle_censoring(self, df):
        '''
        

        Parameters
        ----------
        df : pd.DataFrame
            Data.

        Returns
        -------
        pd.DataFrame

        '''
        
        df = df.copy()
        # date of censoring
        if self.event_=='obitoUTI':
            dt_censor = df['DT_INTERNA'].where((df['DT_INTERNA'] >= df['DT_BEGIN']) & (df['DT_INTERNA'] <= dt_end))
        elif self.event_=='obito':
            dt_censor = df[['DT_INTERNA', 'DT_ENTUTI']].apply(lambda row: row[(row >= df['DT_BEGIN'].loc[row.name]) & (row <= dt_end)].min(), axis='columns')
        else:
            dt_censor = pd.Series([np.nan]*df.shape[0])
            
        # handling <DT_END = NaT> for EVENT=True
        cond_evTrue = (df['DT_END'].isna()) & (dt_censor.notnull()) & (df[self.surv_status_feat_]==True)    # condition for replacing NaN DT_END
        df['DT_END'].mask(cond_evTrue, dt_censor, inplace=True)                                             # change <DT_END=NaN & event=True & dt_censor> with dt_censor
        df[self.surv_status_feat] = df[self.surv_status_feat].mask(cond_evTrue, False, inplace=True)        # status=False for replaced <DT_END=NaN & event=True & dt_censor>

        # handling <DT_END = NaT> for EVENT=False
        cond_evFalse = (df['DT_END'].isna()) & (df[self.surv_status_feat]==False)
        df['DT_END'].mask(cond_evFalse, dt_end, inplace=True)                                           # change <DT_END=NaN & event=False> with dt_end
        return df

    def download_opendatasus(self, db_ref):
        self._download_opendatasus(db_ref)
        return
    
    def data_processing(self, db_ref, clin_feat_gen=True, demo_feat=True, lab_feat=False, ibge_data=False, **kwargs):
        '''
        

        Parameters
        ----------
        db_ref : string
            ['srag20', 'srag21']
            Reference to the data set to be processed.
        clin_feat_gen : bool, optional
            Whether to generate clinical features by processing text features using predefined regex patterns.
            If True, executes SUSurv.generate_clinical_feat()
            The default is True.
        demo_feat : bool, optional
            Whether to include demographic features from SRAG data set
            The default is True.
        lab_feat : bool, optional (NOT IMPLEMENTED YET)
            Whether to include laboratorial features from SRAG data set
            containing test results for other viral coinfections
            The default is False.
        ibge_data : bool, optional (NOT IMPLEMENTED YET)
            Add demographic information from IBGE database.
            The default is False.
        **kwargs : dictionary
            Additional arguments for the processing procedure initialisation < proc_init() >
            - event: ["obito","obitoUTI","graves"]
                Definition of the survival event.

        Returns
        -------
        None.

        '''
        
        print('\n>> started {} data processing pipeline\n.'.format(db_ref))
        
        self.open_db_processing(db_ref, **kwargs)
        
        # CASE SELECTION
        self.case_selection()
        
        # FEAT SELECTION
        self.feat_selection(clin_feat_gen=clin_feat_gen, demo_feat=demo_feat, lab_feat=lab_feat)
        if ibge_data:
            self.add_ibge_info()
        
        # SURVIVAL DATA SET 
        df = self.survival_features()
        
        return
    
    def save(self, concat=False, save_srag=True, **kwargs): # true or false?
    '''
    ADJUST FOR self.__dfs_proc = {db_ref : pandas.dataframe}
    '''    
        print('\n>> started SUSurv saving process\n.')
        self.__log_proc['concat'] = concat
        self.__log_proc['save_srag'] = save_srag
        
        if concat:
            if isinstance(self.__df20, pd.DataFrame) and isinstance(self.__df21, pd.Dataframe):
                self.__set_concat()
            else:
                warnings.warn('Missing processed data set to concatenate. Single dataframes will be saved instead')
            
        # generate processed folder and save dbs (sep or concat)
        self._create_proc_folder()
        if isinstance(self.__df_concat, pd.DataFrame):
            self._save_proc_file(self.__df_concat, 'concat')
        else:
            if isinstance(self.__df20, pd.DataFrame):
                self._save_proc_file(self.__df20, 'srag20')
            if isinstance(self.__df21, pd.DataFrame):
                self._save_proc_file(self.__df21, 'srag21')
            
        self._close_repo(save_orig=save_srag, proc_log=self.__log_proc)
        return


###################
# SCRIPT FUNCTIONS
###################

def processing_pipeline(dbs, download, **kwargs):
    '''
    Pipeline for processing SRAG database into survival data.
    '''
    
    # create class of SUSurv
    surv_proc = SUSurv()
    
    for db in dbs:
        if download: surv_proc.download_opendatasus(db)
        surv_proc.data_processing(db, **kwargs)
    
    surv_proc.save(**kwargs)
        
    return


if __name__ == '__main__':

    # ARG PARSE SETTINGS
    parser = argparse.ArgumentParser(description="Script to automatically retrieve SRAG databases from openDataSUS and process into survival data")
    
    parser.add_argument("--db", 
                        choices=["srag20", "srag21", "both"],
                        default="both", type=str, 
                        help="Srag databases to process")
    parser.add_argument("--concat", action='store_true', 
                        help="If --db=both, saves srag20 and srag21 in a single processed data file")
    parser.add_argument("--nocrawler", action='store_true', 
                        help="Do not download the updated databases from openDataSUS: uses the dbs in the repository")
    parser.add_argument("--nosave", action='store_true', 
                        help="Do not save the original (downloaded) openDataSUS databases. Else, overrides the dbs in the repository")
    parser.add_argument("--event", 
                        choices=["obito","obitoUTI","graves"],
                        default="obitoUTI", type=str,
                        help="The survival event")
    
    args = parser.parse_args()
    kwargs = vars(args)
    
    # PARAMS OF FUNCTION: processing_pipeline
    if args.db=='both':
        dbs = ["srag20", "srag21"]
    else:
        dbs = [args.db]
    del kwargs['db']
    
    if args.nocrawler:
        download = False
    else:
        download = True
    del kwargs['nocrawler']
    
    if args.nosave:
        kwargs['save_srag'] = False
    del kwargs['nosave']
    
    processing_pipeline(dbs, download, **kwargs)