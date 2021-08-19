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
        # dataframes
        self.__df20 = None
        self.__df21 = None
        self.__df_concat = None
        
        # processing 
        self.__dt_surv_begin = datetime.strptime('2019-12-01', '%Y-%m-%d') # date considered for the beggining of the Survival Study
        self.__db_datestamp = None
        
        # processing infos
        self.__db_in_proc = None
        self.__log_proc = {'datastamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           'code-path': str(os.path.dirname(__file__)).replace('\\','/') + '/' + str(os.path.basename(__file__)),
                           'concat': None,
                           'save_srag': None
                           }
        self.__log_proc_db = {'event': None,
                              'cases': None,
                              'survival_dt_end': None,
                              '__feat_selection': False,
                              '__covid_selection': False,
                              '__date_process': False,
                              '__processClinicalInfo': False,
                              '__processSurvivalData': False,
                             }
        
    def __set_log_db(self, db_ref, event, cases):
        self.__log_proc[db_ref] = self.__log_proc_db.copy()
        self.__log_proc[db_ref]['event'] = event
        self.__log_proc[db_ref]['cases'] = cases
        return
    
    def __set_df(self, db_ref, df):
        if db_ref=='srag20':
            self.__df20 = df.copy()
        elif db_ref=='srag21':
            self.__df21 = df.copy()
        else:
            raise ValueError('The processed data set < {} > is not defined in SUSurv()'.format(db_ref))
        
        print('> {} processing done'.format(db_ref))
        return
    
    def __set_concat(self):
        self.__df_concat = pd.concat([self.__df20, self.__df21], axis='index', ignore_index=True)
        return
    
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
        df : TYPE
            DESCRIPTION.

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
        self.__log_proc[self.__db_in_proc]['survival_dt_end'] = self.__db_datestamp
        return df
    
    def __feat_selection(self, db):
        '''
        This function performs a feature selection that uses 
        external .json file containing the features to keep in the data set.
        
        input-file: 
            openDataSUS\data\_srag_colSelection.json
        '''
        self.__log_proc[self.__db_in_proc]['__feat_selection'] = True
        print('.. feature selection')
        
        with open(self._column_selec, 'r') as f: 
            colSelec = json.load(f)
        cols2drop = list(set(db.columns) - set(colSelec))
        return db.drop(columns=cols2drop)
    
    def __covid_selection(self, db):
        '''
        This function selects the transactions that are COVID positive. 
        The selection is a disjunction of the following conditions:
        
        (CLASSI_FIN=='covid19'):                 final diagnosis of the case
        (PCR_SARS2==True):                       RT-PCR for SARS-CoV-2 result
        (POS_AN_OUT=='nao' & POS_AN_FLU=='nao'): results of testing other respiratory disease (OUT) and influenza (FLU)
        '''
        self.__log_proc[self.__db_in_proc]['__covid_selection'] = True
        print('.. covid cases selection')
        
        return db[(db.CLASSI_FIN=='covid19') | (db.PCR_SARS2==True) | ((db.POS_AN_OUT=='nao') & (db.POS_AN_FLU=='nao'))].copy()
    
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
        self.__log_proc[self.__db_in_proc]['__date_process'] = True
        print('.. date-type formatting')
        
        df = db.copy()
        dtFeat = [item for item in df.columns if 'DT_' in item]     # date features
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
                                                            value.split('-')[0]).replace(r'^\s*$', np.nan, regex=True).astype(float)
            # (year > this_year) >> NaN
            df[col].mask(yearMask > this_year, np.nan, inplace=True) 
            # type: datetime
            df[col] = pd.to_datetime(df[col], errors='coerce') # columns to datetime: dates yielding overflow are set to NaN ['coerce'] 
        return df

    def __processClinicalInfo(self, df, combine=True):
        '''
        This function generates the boolean clinical features by:
            - processing the < 'OUTRO_DES','MORB_DESC' > text features using preseted regex patterns 
            - unifying the identified (regex) patterns with the openDataSUS original clinical features
            - unifying some strategic clinical features into single one 
        '''
        self.__log_proc[self.__db_in_proc]['__processClinicalInfo'] = True
        print('.. clinical features extraction')
        
        # existent clinical features >> adjustments
        with open(self._clinical_feat, 'r') as f: 
            var = json.load(f)
        with open(self._feat_replace, 'r') as f: 
            dic_replace = json.load(f)
        
        dfClinical = df[var].copy()
        dfClinical.CS_GESTANT = dfClinical.CS_GESTANT.astype(str)
        dfClinical.replace(dic_replace, inplace=True)
        
        # new clinical features >> text search
        textSearch = df[['OUTRO_DES','MORB_DESC']].fillna('').agg(' '.join, axis=1).replace(r'^\s*$', np.nan, regex=True)
        textSearch = textSearch.str.lower()
        with open(self._feat_regex, mode='rt') as f:
            feature_regex = json.load(f)
        for feature in feature_regex.keys():
            dfClinical[feature] = textSearch.str.contains(feature_regex[feature], case=False, regex=True)
        
        # unifying generated features with existent srag features
        with open(self._feat_unification, mode='rt') as f:
            unif = json.load(f)
        for feat_srag, feat in unif:
            dfClinical[feat] = dfClinical[[feat,feat_srag]].fillna('').astype(str).agg(''.join, axis=1).replace(r'^\s*$', np.nan, regex=True).str.contains('True', case=False)
        
        # processed clinical database >> with relevant original information
        clinicalFeat = list(feature_regex.keys()) + ['HEMATOLOGI','SIND_DOWN']
        dfFeat = ['id_DB','DT_NOTIFIC','DT_SIN_PRI','HOSPITAL','DT_INTERNA','UTI','DT_ENTUTI','EVOLUCAO','DT_EVOLUCA','DT_ENCERRA']
        
        dfProcess = pd.concat([df[dfFeat],dfClinical[clinicalFeat]], axis='columns')
        dfProcess.rename(columns={'HEMATOLOGI':'doenca_hematologica','SIND_DOWN':'sindrome_down'}, inplace=True)
        dfProcess = dfProcess.astype(dict.fromkeys([feat for feat in dfProcess.columns if feat.islower()],'boolean'))   # set clinical features to boolean dtype
        
        if combine:
            # generate combined clinical features
            dfProcess['alteracao_olfato_paladar'] = (dfProcess['alteracao_olfato']==True) | (dfProcess['alteracao_paladar']==True)
            dfProcess['alteracao_respiratoria'] = (dfProcess['disturbios_respiratorios']==True) | (dfProcess['cianose']==True) | (dfProcess['saturacao_menor_95']==True)
            dfProcess['sintoma_gripal'] = (dfProcess['congestao_nasal']==True) | (dfProcess['coriza']==True) | (dfProcess['espirro']==True) | (dfProcess['cefaleia']==True) | (dfProcess['dor_de_garganta']==True) | (dfProcess['dores_corpo']==True) | (dfProcess['adinamia']==True) | (dfProcess['mal_estar']==True) | (dfProcess['dor_olhos']==True)
            dfProcess['nausea_vomito'] = (dfProcess['nausea']==True) | (dfProcess['vomito']==True)
        
        return dfProcess    

    #FUNCTION: GENERATE SURVIVAL FEATURES
    def __processSurvivalData(self, _df, event='obitoUTI', cases='all'):
        
        self.__log_proc[self.__db_in_proc]['__processSurvivalData'] = True
        print('.. survival features generation')
        
        #FUNCTION: GENERATE BEGIN/END SURVIVAL DATES
        def setSurvivalDates(_df, dt_end):
            df = _df.copy()
            # event definition
            if event=='obitoUTI':
                colsEnd = ['DT_ENTUTI', 'DT_EVOLUCA', 'DT_ENCERRA']
            elif event=='casosGraves':
                colsEnd = ['DT_INTERNA', 'DT_ENTUTI', 'DT_EVOLUCA', 'DT_ENCERRA']
            elif event=='obito':
                colsEnd = ['DT_EVOLUCA', 'DT_ENCERRA']
    
            # Features <DT_BEGIN, DT_END>: survival date ref for begin/end of study
            df['DT_BEGIN'] = df[['DT_NOTIFIC', 'DT_SIN_PRI']].apply(lambda row: row[(row >= self.__dt_surv_begin) & (row <= dt_end)].min(), axis='columns')
            df['DT_END'] = df[colsEnd].apply(lambda row: row[(row >= df['DT_BEGIN'].loc[row.name]) & (row <= dt_end)].min(), axis='columns')
            return df
        
        dt_end = datetime.strptime(self.__db_datestamp, '%Y-%m-%d')
        df = setSurvivalDates(_df, dt_end)
    
        # cases selection
        if cases == 'all':
            df = df.copy()
        elif cases == 'hospital':
            df = df[df.HOSPITAL == 'sim'].copy()
    
        # Feature <survival_status>
        if event=='obitoUTI':
            df['survival_status'] = (df['UTI'] == 'sim') | (df['EVOLUCAO'] == 'obito')
            dt_censor = df['DT_INTERNA'].where((df['DT_INTERNA'] >= df['DT_BEGIN']) & (df['DT_INTERNA'] <= dt_end))
        elif event=='obito':
            df['survival_status'] = (df['EVOLUCAO'] == 'obito')
            dt_censor = df[['DT_INTERNA', 'DT_ENTUTI']].apply(lambda row: row[(row >= df['DT_BEGIN'].loc[row.name]) & (row <= dt_end)].min(), axis='columns')
        elif event=='casosGraves':
            df['survival_status'] = (df['HOSPITAL'] == 'sim') | (df['UTI'] == 'sim') | (df['EVOLUCAO'] == 'obito')
        
        # handling <DT_END = NaT> for EVENT=True
        if event=='obitoUTI' or event=='obito':
            cond_evTrue = (df['DT_END'].isna()) & (dt_censor.notnull()) & (df['survival_status']==True) # condition for replacing DT_END
            df['DT_END'].mask(cond_evTrue, dt_censor, inplace=True)                                     # change <DT_END=NaN & event=True & dt_censor> with dt_censor
            df['survival_status'].mask(cond_evTrue, False, inplace=True)                                # status=False for <DT_END=NaN & event=True & dt_censor>
    
        # handling <DT_END = NaT> for EVENT=False
        cond_evFalse = (df['DT_END'].isna()) & (df['survival_status']==False)
        df['DT_END'].mask(cond_evFalse, dt_end, inplace=True)                                           # change <DT_END=NaN & event=False> with dt_end
        df['survival_status'].mask(cond_evFalse, False, inplace=True)                                   # status=False for <DT_END=NaN & event=False>
        
        # Feature <survival_time>
        df['survival_time'] = (df.DT_END - df.DT_BEGIN).dt.days
        df.dropna(axis='index', subset=['survival_time'], inplace=True)     # drop <survival_time=NaN> (cases: <DT_END=NaN & event=True & dt_censor=NaN> OR <DT_BEGIN=NaN>)
        
        return df

    def download(self, db_ref):
        self._get_opendatasus(db_ref)
        return
    
    def process_data(self, db_ref, event='obitoUTI', cases='all', **kwargs):
        '''
        DESCRIBE

        Parameters
        ----------
        db_ref : string
            ['srag20', 'srag21']
            Reference to the data set to be processed
        **kwargs : 
            Additional arguments for the processing procedure
            - event: ["obito","obitoUTI","graves"]
                Definition of the survival event.
            - cases: ["all","hosp","uti"]
                Data set cases to be considered.

        Returns
        -------
        None.

        '''
        print('\n>> started {} data processing pipeline\n.'.format(db_ref))
        
        self._verify_db(db_ref)
        self.__set_log_db(db_ref, event, cases) # create log for db_ref
        self.__db_in_proc = db_ref        
        
        if not self._has_db(db_ref):
            self.download(db_ref)
                
        # LOAD DATASET
        df = self.__load_db(db_ref)
        # FEAT SELECTION
        df = self.__feat_selection(df)
        # COVID-CASES SELECTION
        df = self.__covid_selection(df)
        # DATE-TYPE PROCESSING
        df = self.__date_process(df)
        # CLINICAL INFO PROCESSING >> GENERATING SYMPTOMS/COMORBIDITIES FEATURES
        df = self.__processClinicalInfo(df)
        # SURVIVAL INFO
        df = self.__processSurvivalData(df, event=event, cases=cases)
        
        self.__set_df(db_ref, df)
        self.__db_in_proc = None
        return
    
    def save(self, concat=False, save_srag=True, **kwargs): # true or false?
        
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
        if download: surv_proc.download(db)
        surv_proc.process_data(db, **kwargs)
    
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
    parser.add_argument("--cases", 
                        choices=["all","hosp","uti"],
                        default="all", type=str, 
                        help="The covid cases to compose the data")
    
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