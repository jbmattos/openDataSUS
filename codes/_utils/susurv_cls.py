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

import json
import numpy as np
import os
import pandas as pd

from datetime import date, datetime
from repository_cls import Repository


class SUSurv(Repository):
    
    __EVENTS = ['icu', 'icu_death', 'death']
    __CLOSE_MSG = 'No data processing is open. Please, SUSurv.open_surv_processing(db_ref)'
    
    def __init__(self):
        super().__init__()
        # DataFrames
        self.__dfs_proc = {}
        self.__df_concat = None
        
        # Survival Definitions
        self.__surv_status_feat = 'survival_status'
        self.__surv_time_feat = 'survival_time'
        self.__dt_study_feat = ('DT_SURV_BEGIN','DT_SURV_EVENT')
        self.__dt_begin_study = ['DT_NOTIFIC', 'DT_SIN_PRI', 'DT_INTERNA']
        self.__dt_event_def = {'icu': ['DT_ENTUTI'],                        # the features must be in a list (even when only one)
                               'icu_death': ['DT_ENTUTI', 'DT_EVOLUCA'],
                               'death': ['DT_EVOLUCA']
                               }
        self.__dt_right_censor_def = {'icu': self.__dt_begin_study,
                                      'icu_death': self.__dt_begin_study,
                                      'death': self.__dt_begin_study + ['DT_ENTUTI']
                                      }
        self.__dt_begin_th = datetime.strptime('2019-12-01', '%Y-%m-%d') # date considered for the beggining of the Survival Study
        
        # Data Processing 
        ## [modifief in] processing in open/close_db_processing methods
        self.__db_ref_inproc = None
        self.__surv_event_def = None
        self.__input_cens = None
        self.__db_datestamp = None
        self.__df_inproc = None
        self.__open = False
        ## logs
        self.__generated_clin_feat = []
        self.__other_demo_feat = []
        self.__log_proc = {}
        self.__log_gen = {'datastamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                          'code-path': str(os.path.dirname(__file__)).replace('\\','/') + '/' + str(os.path.basename(__file__)),
                          'save_concat': None,
                          'save_orig': None
                          }
    


    ########################################################################
    ########################################################################
    #                  PRIVATE ATTRIBUTES AND METHODS                      #
    ########################################################################
    ########################################################################
    
    @staticmethod
    def __verify_event(event):
        if not event in SUSurv.__EVENTS:
            raise ValueError('The survival event to consider is not defined: {}. Please, set event={} while oppening the processing mode.'.format(event, SUSurv.__EVENTS))
    
    def __verify_opening(self):
        if not self.__open:
            raise ValueError(SUSurv.__CLOSE_MSG)        
    
    @property
    def __db_ref_proc(self):
        return list(self.__dfs_proc.keys())
    
    def __set_log_db_processing(self, db_ref, event, input_cens):
        self.__log_proc[db_ref] = {
            'event_': event,
            'input_cens_': input_cens,
            'db_datestamp_': None,
            'case_selection': False,
            'feat_selection': False,
            'generate_clinical_feat': False,
            'add_ibge_feats': False,
            'case_tree': {}
            }
        return
    
    def __save_processed_data(self):
        self.__dfs_proc[self.inproc_] = self.__df_inproc.copy()
        print('> closed {} processing'.format(self.inproc_))
        return
    
    def __set_concat(self):
        self.__df_concat = pd.concat([df for df in self.__dfs_proc.values()], axis='index', ignore_index=True)
        return

    def __open_db_processing(self, db_ref, event, input_cens, case_selection):
        # loads the parameters of open_surv_processing plus the processing DataFrame and __open flag
        self.__set_log_db_processing(db_ref, event, input_cens)
        self.__db_ref_inproc = db_ref
        self.__surv_event_def = event
        self.__input_cens = input_cens
        # self.__db_datestamp is initialised when the original data set is loaded
        
        # load < self.__df_inproc > for the first time
        if not self._has_db(db_ref):
            self.download_opendatasus(db_ref)
        self.__update_df_inproc(self.__load_db(db_ref, load_log=True), 'original_srag')
        self.__open = True
        
        if case_selection:
            self.case_selection()
        # set DT features to date type
        self.__update_df_inproc(self.__date_process(self.inproc_data_), 'date_process')
        # generates survival data
        self.__set_survival_data()
        print('{} new processing opened.'.format(self.inproc_))
        return
        
    def __reset_db_processing(self):
        # resets the parameters of open_surv_processing plus the processing DataFrame and __open flag
        self.__db_ref_inproc = None
        self.__surv_event_def = None
        self.__input_cens = None
        self.__db_datestamp = None
        self.__df_inproc = None
        self.__open = False
        
    def __reopen_db_processing(self, db_ref):
        # loads the parameters of open_surv_processing plus the processing DataFrame and __open flag
        self.__db_ref_inproc = db_ref
        self.__surv_event_def = self.__log_proc[db_ref]['event_']
        self.__input_cens = self.__log_proc[db_ref]['input_cens_']
        self.__db_datestamp = self.__log_proc[db_ref]['db_datestamp_']
        self.__df_inproc = self.__dfs_proc[db_ref].copy()
        self.__open = True
        print('Reopened {} data set processing.'.format(db_ref))
        self.__status_db(db_ref)
        
    def __status_db(self, db_ref):
        print('- event: {}'.format(self.__log_proc[db_ref]['event_']))
        print('- input right-censoring: {}'.format(self.__log_proc[db_ref]['input_cens_']))
        print('- data set datestamp: {}'.format(self.__log_proc[db_ref]['db_datestamp_']))
        print('- case selection: {}'.format(self.__log_proc[db_ref]['case_selection']))
        print('- feature selection: {}'.format(self.__log_proc[db_ref]['feat_selection']))
        print('- text-generated clinical features: {}'.format(self.__log_proc[db_ref]['generate_clinical_feat']))
        print('- ibge data: {}'.format(self.__log_proc[db_ref]['add_ibge_feats']))
    
    
    #==================================================
    #  SURVIVAL DATA GENERATOR
    # 
    #   - Method responsible for transformig the original 
    #   data set into survival data set
    #   - Inplace function (modify the in-processing data)
    #==================================================
    
    def __set_survival_data(self):
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
        (Modifies the private attribute of the dataframe under processing < __df_inproc >)

        '''
        print('.. survival features generation')
        
        df = self.inproc_data_
        
        ### SET STUDY DATES: study begin and event outcome
        dt_begin = self.__dt_study_feat[0]
        dt_event = self.__dt_study_feat[1]
        
        # BEGIN DATE: minimum date between self.__dt_begin_study given that it is between the begin/end threshold-dates
        df[dt_begin] = df[self.__dt_begin_study].apply(lambda row: row[(row >= self.__dt_begin_th) & (row < self.__dt_end_th)].min(), axis='columns')
        df.dropna(axis='index', subset=[dt_begin], inplace=True) # no registered date for the beggining of survival study
        
        # EVENT DATE:
        if len(self.__dt_event) > 1:
            # if more than one event date: selects the minimum between the possible event dates
            df[dt_event] = df[self.__dt_event].apply(lambda row: row[(row > df[dt_begin].loc[row.name]) & (row <= self.__dt_end_th)].min(), axis='columns')
        else:
            # only one possible event date
            df[dt_event] = df[self.__dt_event[0]]
        # Filters valid event dates: returns the event date if it is greater than beggining and less/equal to end_th
        df[dt_event] = df[dt_event].where(self.__validate_dt_feat(dt_event, df), np.nan)
        
        ### SURVIVAL STATUS Feature
        df[self.survival_status_feat_] = self.__get_surv_events(df)
        # Handles missing data for event date DT_SURV_EVENT: called after defining the event because of < Event=True > and <DT_Event=NaN> cases to imput right-censoring
        df = self.__handle_missing_event_dt(df)
        
        # SURVIVAL TIME Feature
        df[self.survival_time_feat_] = (df.DT_SURV_EVENT - df.DT_SURV_BEGIN).dt.days
        # drop <survival_time=NaN> (cases: <DT_SURV_EVENT=NaN & event=True & dt_censor=NaN>
        df.dropna(axis='index', subset=[self.survival_time_feat_], inplace=True)     
        
        self.__update_df_inproc(df, 'survival_data')
        return
    
    
    #==================================================
    #  SURVIVAL ANALYSIS DEFINITIONS AND METHODS
    # 
    #   - None of the private methods for survival data 
    #   manipulation is inplace (always return some info)
    #==================================================
    
    @property
    def __dt_event(self):
        return self.__dt_event_def[self.event_]
    
    @property
    def __dt_right_censor(self):
        return self.__dt_right_censor_def[self.event_]
        
    @property
    def __dt_end_th(self):
        if self.__open:
            return datetime.strptime(self.__db_datestamp, '%Y-%m-%d')
        else:
            return None
        
    def __validate_dt_feat(self, feat, df):
        return (df[feat] > df[self.__dt_study_feat[0]]) & (df[feat] <= self.__dt_end_th)
    
    def __get_surv_events(self, df):
        '''
        Returns the default Event occurrence for the defined event_.

        Parameters
        ----------
        df : pd.DataFrame
            Srag data with at least default feature selection.

        Returns
        -------
        pd.Series
            Event occurrence for the data population: the series is the 
            disjunction of feature conditions that define the event occurrence.

        '''
        
        if self.event_=='death':
            return (df['EVOLUCAO'] == 'obito')
        else:
            if self.event_=='icu':
                return (df['UTI'] == 'sim') | self.__validate_dt_feat(feat='DT_ENTUTI', df=df)
            elif self.event_=='icu_death':
                return (df['UTI'] == 'sim') | self.__validate_dt_feat(feat='DT_ENTUTI', df=df) | (df['EVOLUCAO'] == 'obito')
        
    def __handle_missing_event_dt(self, df):
        '''
        Handles missing values for DT_SURV_EVENT study date.
            
            - Default:
                Inputs the date of data retrival for censored cases missing DT_SURV_EVENT, 
                and removes remaining missing values.
            - input_cens=True:
                If processing method is open with input_cens=True, 
                handles event True cases with missing DT_SURV_EVENT by replacing 
                DT_SURV_EVENT with the latest known DT_ before DT_SURV_EVENT 
                (accordingly to defined in < __dt_right_censor_def >)

        Parameters
        ----------
        df : pd.DataFrame
            Data.

        Returns
        -------
        pd.DataFrame

        '''
        
        dt_event = self.__dt_study_feat[1]
        ## CENSORED CASES:
        # For already censored cases, missing DT_SURV_EVENT is inputted as the final study date __dt_end_th
        rpl_vals = ~df[self.survival_status_feat_] & df[dt_event].isna()
        df[dt_event] = df[dt_event].mask(rpl_vals, self.__dt_end_th)
        
        ## INPUT RIGHT CENSORING
        # For event True cases with no DT_SURV_EVENT, DT_SURV_EVENT is replaced with latest known DT_ before DT_SURV_EVENT >> defined by __dt_right_censor_def
        if self.__input_cens:
            df = self.__input_right_censoring(df)
        
        # Remove remaining missing DT_SURV_EVENT
        df.dropna(axis='index', subset=[dt_event], inplace=True)
        return df
    
    def __input_right_censoring(self, df):
        
        if len(self.__dt_right_censor) > 1:
            # if more than one possible censor date: selects the maximum between the possible dates
            dt_censor = df[self.__dt_right_censor].apply(lambda row: row[(row > df[self.__dt_study_feat[0]].loc[row.name]) & (row <= self.__dt_end_th)].max(), axis='columns')
        else:
            # only one possible event date
            dt_censor = df[self.__dt_right_censor[0]]
            
        # handling <DT_SURV_EVENT = NaT> for EVENT=True
        dt_event = self.__dt_study_feat[1]
        rpl_vals =  df[self.survival_status_feat_] & df[dt_event].isna() & dt_censor.notnull()   # condition for replacing NaN DT_SURV_EVENT
        df[dt_event].mask(rpl_vals, dt_censor, inplace=True)                                     # change <DT_SURV_EVENT=NaN & event=True & dt_censor> with dt_censor
        df[self.survival_status_feat_].mask(rpl_vals, False, inplace=True)                       # right cesor: status=False for cases replaced with censor date

        return df
    
    
    #==================================================
    #  FEATURE SELECTION METHODS
    # 
    # OBS:
    #   - The feature selection methods return a list
    #   of the features' names to be selected in the 
    #   required category
    #==================================================
    
    def __default_feats(self):
        # data set id, survival_time, survival_status, initial date for survival study, date of the event occurrence
        feats = ["id_DB", self.survival_time_feat_, self.survival_status_feat_, self.__dt_study_feat[0], self.__dt_study_feat[1]] 
        # general info
        with open(self._file_gen_feat, 'r') as f: 
            feats += json.load(f)
        # date features
        with open(self._file_dt_feat, 'r') as f: 
            feats += json.load(f)
        # clinical features
        if self.__log_proc[self.inproc_]['generate_clinical_feat']:
            feats += self.__generated_clin_feat
        else:
            with open(self._file_clin_feat, 'r') as f: 
                feats += json.load(f)
        return feats
    
    def __clin_feats(self):
        if self.__log_proc[self.inproc_]['generate_clinical_feat']:
            return self.__generated_clin_feat
        else:
            with open(self._file_clin_feat, 'r') as f: 
                return json.load(f)
    
    def __demo_feats(self):
        with open(self._file_demo_feat, 'r') as f: 
            feats = json.load(f)
        # self.__other_demo_feat: initialised as [] >> updated with < += somelist > inside the methods that generate demo feat from external sources (ex. ibge) 
        return feats + self.__other_demo_feat
        
    def __lab_feats(self):
        with open(self._file_lab_feat, 'r') as f: 
            return json.load(f)
    
    
    #==================================================
    #  CASE SELECTION METHODS
    # 
    # OBS:
    #   - All private case selection methods return
    #   a modified dataframe
    #==================================================
    
    def __covid_positive_cases(self, df):
        '''
        This function selects the transactions that are COVID positive. 
        The selection is a disjunction of the following conditions:
        (CLASSI_FIN=='covid19'):                 final diagnosis of the case
        (AN_SARS2==True):                        
        (PCR_SARS2==True):                       RT-PCR for SARS-CoV-2 result
        (RES_IGM==True):
        (RES_IGA==True): 
        '''        
        return df[(df.CLASSI_FIN=='covid19') | (df.AN_SARS2==True) | (df.PCR_SARS2==True) | (df.RES_IGM==True) | (df.RES_IGA==True)].copy()
    
    def __hospitalised_cases(self, df):
        '''
        This function selects the transactions that were hospitalised
        The selection is a disjunction of the following conditions:
        (HOSPITAL=='sim'):      Hospitalised patient
        (DT_INTERNA):           Valid date of hospitalisation
        (UTI=='sim'):           ICU hospitalisation                 
        (DT_ENTUTI):            Valid date of ICU hospitalisation
        '''        
        return df[(df.HOSPITAL=='sim') | self.__validate_dt_feat('DT_INTERNA', df) | (df.UTI == 'sim') | self.__validate_dt_feat(feat='DT_ENTUTI', df=df)].copy()
    
    def __full_clin_cases(self, df):
        '''
        This function selects the transactions with full information on 
        clinical features
        '''        
        return df.dropna(axis='index', how='any', subset=self.__clin_feats(), inplace=False)
    
    def __full_demo_cases(self, df):
        '''
        This function selects the transactions with full information on 
        clinical features
        '''        
        return df.dropna(axis='index', how='any', subset=self.__demo_feats(), inplace=False)
    
    #==================================================
    #  DATAFRAME MANIPULATION
    # 
    #   - All private data manipulation methods return
    #   a modified dataframe
    #==================================================
        
    def __load_db(self, db_ref, load_log=False):
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
        
        if load_log:
            self.__db_datestamp = self._get_db_datestamp(db_ref)
            self.__log_proc[db_ref]['db_datestamp_'] = self.__db_datestamp
        return df

    def __date_process(self, df):
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

        Parameters
        ----------
        df : pd.DataFrame
            Original data from SRAG data files.

        Returns
        -------
        df : pd.DataFrame
            Original data with date features as datetype.

        '''
        
        print('.. date-type formatting')
        
        df = df.copy()
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
                                                            value.split('-')[0]).replace(r'^\s*$', np.nan, regex=True).astype(float)
            # (year > this_year) >> NaN
            df[col].mask(yearMask > this_year, np.nan, inplace=True) 
            # type: datetime
            df[col] = pd.to_datetime(df[col], errors='coerce') # columns to datetime: dates yielding overflow are set to NaN ['coerce'] 

        return df
    
    def __update_df_inproc(self, df, _id):
       self.__df_inproc = df.copy()
       self.__log_proc[self.__db_ref_inproc]['case_tree'][_id] = df.shape[0]
       return
    
    

    #######################################################################
    #######################################################################
    #                  PUBLIC ATTRIBUTES AND METHODS                      #
    #######################################################################
    #######################################################################
    
    @property
    def open_(self):
        return self.__open
    
    @property
    def survival_status_feat_(self):
        return self.__surv_status_feat
    
    @property
    def survival_time_feat_(self):
        return self.__surv_time_feat
    
    @property
    def study_dt_feat_(self):
        return list(self.__dt_study_feat)
    
    @property
    def study_interval_(self):
        return (self.__dt_begin_study, self.__dt_event_def[self.event_])
    
    @property
    def clin_feat_(self):
        return self.__clin_feats()
    
    @property
    def demo_feat_(self):
        return self.__demo_feats()
    
    @property
    def lab_feat_(self):
        return self.__lab_feats()
    
    @property
    def inproc_(self):
        self.__verify_opening()
        return self.__db_ref_inproc
    
    @property
    def inproc_data_(self):
        self.__verify_opening()
        return self.__df_inproc.copy()
    
    @property
    def event_(self):
        self.__verify_opening()
        return self.__surv_event_def
    
    def proc_status(self):
        '''
        Print the processing status

        Returns
        -------
        None.

        '''
        print('\n=== DATA PROCESSING STATUS ===')
        print('>> running data processing: {}'.format(self.open_))
        if self.__open:
            print('>> data set in processing: {}'.format(self.inproc_))
        print('\n>> General Configs:')
        print('proc datastamp: {}'.format(self.__log_gen['datastamp']))
        print('proc code-path: {}'.format(self.__log_gen['code-path']))
        print('proc saving concatenation: {}'.format(self.__log_gen['save_concat']))
        print('proc saving original srag: {}'.format(self.__log_gen['save_orig']))
        print('\n>> Processing Configs:')
        for db in self.__log_proc.keys():
            print('\nData set: {}'.format(db))
            self.__status_db(db)
        return
    
    def case_selection_tree(self):
        '''
        Tree of case selection.

        Returns
        -------
        Dictionary
            The keys are identification of the method that modified the data set 
            and the values are the total number of transations that remained in
            the data set.

        '''
        return self.__log_proc[self.inproc_]['case_tree']
    
    def survival_status(self):
        # The survival status, with adjusted censoring
        self.__verify_opening()
        return self.__df_inproc[self.survival_status_feat_]
    
    def open_surv_processing(self, db_ref, event='death', input_cens=True, case_selection=True, reset=False):
        '''
        Open the processing mode for a data set 
            
        Parameters
        ----------
        db_ref : str
            [srag20, srag21]
            The identification of the data set to instantiate a new processing.
        event : str, The default is 'death'.
            ['icu', 'icu_death', 'death']
            Survival event to consider in the survival study.
        input_cens : bool, The default is True.
            Whether to input right-censoring. 
        case_selection: bool, The default is True.
            If True, performs the default case selection. 
        reset: bool, The default is False.
            If True, instantiate a entirely new processing for db_ref, even if 
            an ongoing process is already registered in the SUSurv object.
            If False, restores the ongoing process.

        Returns
        -------
        None.

        '''
        self._verify_db(db_ref)
        self.__verify_event(event)
        if self.__open:
            self.close_surv_processing()
        
        if db_ref in self.__db_ref_proc and not reset:
            self.__reopen_db_processing(db_ref)
        else:
            self.__open_db_processing(db_ref, event, input_cens, case_selection)
    
    def reset_surv_processing(self, case_selection=True):
        if not self.__open:
            raise ValueError('Reopen a data set processing before resetting. On-going processes: {}'.format(self.__log_proc.keys()))
        self.__open_db_processing(self.inproc_, self.event_, self.__input_cens, case_selection)
    
    def close_surv_processing(self):
        self.__save_processed_data()
        self.__reset_db_processing()
        return    
    
    def get_original_data(self, db_ref):
        return self.__load_db(db_ref)        
    
    def download_opendatasus(self):
        '''
        This function downloads the SRAG data set under processing (inproc_)
        directly from openDataSUS source

        Parameters
        ----------
        db_ref : str
            DESCRIPTION.

        Returns
        -------
        None.

        '''
        if self.__open:
            self._download_opendatasus(self.inproc_)
        else:
            raise ValueError('Data set processing is not open. Please, call SUSurv.open_surv_processing(db_ref)')
        return
    
    #==================================================
    #  DATA PROCESSING METHODS 
    # 
    # OBS:
    #   - All public processing methods are inplace
    #   (modify the in-processing data)
    #==================================================
    
    def case_selection(self, method='covid_pos'):
        '''
        This function is responsible for filtering the data set transactions.
        ! Attention: the filtering (selection) method removes transactions 
                from the data set in processing.
                To restore removed transaction, it is necessary to
                restart the data set processing with SUSurv.reset_surv_processing()
        
        Methods:
            
            - "covid_pos": Selects the transactions that are COVID positive. 
            The selection is a disjunction of the following conditions:
            (CLASSI_FIN=='covid19'):                 final diagnosis of the case
            (AN_SARS2==True):                        
            (PCR_SARS2==True):                       RT-PCR for SARS-CoV-2 result
            (RES_IGM==True):
            (RES_IGA==True): 
            
            - "hosp": Selects the transactions that were hospitalised.
            The selection is a disjunction of the following conditions:
                (HOSPITAL=='sim'):      Hospitalised patient
                (DT_INTERNA):           Valid date of hospitalisation
                (UTI=='sim'):           ICU hospitalisation                 
                (DT_ENTUTI):            Valid date of ICU hospitalisation
            
            - "full_clin": Selects the transactions with full clinical 
            information (no missing values on clinical features)
            
            - "full_demo": Selects the transactions with full demographic 
            information (no missing values on demographic features)
            
            [NOT IMPLEMENTED]
            - "age_adult": Selects the transactions with adult age
            (20 < age < 60)
            
            - "age_over_20": Selects the transactions with age > 20
        
        Parameters
        ----------
        method : str
            The method of case selection. The default is covid_pos.
        
        Returns
        -------
        None.
        (Modifies the private attribute of the dataframe under processing)

        '''
        if not self.__log_proc[self.inproc_]['case_selection']:
            self.__log_proc[self.inproc_]['case_selection'] = []
        if method in self.__log_proc[self.inproc_]['case_selection']:
            print('Case selection is updated')
            return
        
        self.__log_proc[self.inproc_]['case_selection'].append(method) 
        print('.. case selection: {}'.format(method))
        
        df = self.inproc_data_
        
        if method == 'covid_pos':
            df = self.__covid_positive_cases(df)
            self.__update_df_inproc(df, 'case_selection_{}'.format(method))
        if method == 'hosp':
            df = self.__hospitalised_cases(df)
            self.__update_df_inproc(df, 'case_selection_{}'.format(method))
        if method == 'full_clin':
            df = self.__full_clin_cases(df)
            self.__update_df_inproc(df, 'case_selection_{}'.format(method))
        if method == 'full_demo':
            df = self.__full_demo_cases(df)
            self.__update_df_inproc(df, 'case_selection_{}'.format(method))
        # add here future input args selection
        
        return
    
    def feat_selection(self, demo_feat=True, lab_feat=True, generate_clin_feat=False):
        '''
        This function is responsible for feature selection.
        ! Attention: this function works by removing the non-selected feature 
                    groups from the data set in processing.
                    To restore removed groups of features, it is necessary to
                    restart the data set processing with SUSurv.reset_surv_processing()
                    
        By default, the function selects:
            - general srag info, 
            - srag date features 
            - srag original clinical features
            - srag demographic features
            - srag laboratorial features [NOT IMPLEMENTED]
        The selected features are predefined in the external .json files:
            openDataSUS/data/_srag_featSelection_genFeat.json  (general info feats)
            openDataSUS/data/_srag_featSelection_dtFeat.json   (date feats)
            openDataSUS/data/_srag_featSelection_clinFeat.json (clinical feats)
            openDataSUS/data/_srag_featSelection_demoFeat.json (demographic feats)
            openDataSUS/data/_srag_featSelection_labFeat.json  (laboratorial feats)
        
        Parameters
        ----------
        demo_feat : bool, The default is True.
            Whether to include demographic features from SRAG data set
        lab_feat : bool, The default is True. (NOT IMPLEMENTED YET)
            Whether to include laboratorial features from SRAG data set
            containing test results for other viral coinfections
        generate_clin_feat : bool, The default is False.
            Whether to generate clinical features by processing text features using predefined regex patterns.
            If True, executes SUSurv.generate_clinical_feat().             
            
        Returns
        -------
        None.
        (Modifies the private attribute of the dataframe under processing)

        '''
        
        def __remove_feat_id(id_):
            if id_ in self.__log_proc[self.inproc_]['feat_selection']:
                self.__log_proc[self.inproc_]['feat_selection'].remove(id_)
        
        def __get_feats(id_):
            if id_ in self.__log_proc[self.inproc_]['feat_selection']:
                if id_=='demo_feat':
                    return self.__demo_feats()
                elif id_=='lab_feat':
                    return self.__lab_feats()
                else:
                    raise ValueError('{} feature identification not defined.'.format(id_))
            else: # requiring feat-id_=True that were aldeady removed from data
                raise ValueError('{} were already removed from selection. Please, reset_surv_processing() to reload removed features.'.format(id_))
            
        
        # first feat_selection execution >> self.__log_proc[self.inproc_]['feat_selection'] is initialised False
        if not self.__log_proc[self.inproc_]['feat_selection']: 
            self.__log_proc[self.inproc_]['feat_selection'] = ['gen_feat', 'dt_feat', 'clin_feat', 'demo_feat', 'lab_feat']
            
        print('.. feature selection: demo_feat={}, lab_feat={}, generate_clin_feat={}'.format(demo_feat, lab_feat, generate_clin_feat))
        
        # default info
        feats = self.__default_feats()
        
        if demo_feat:
            feats += __get_feats('demo_feat')
        else: # demo_feat=False >> remove feats from selection
            __remove_feat_id('demo_feat')
            
        if lab_feat:
            feats += __get_feats('lab_feat')
        else: # lab_feat=False >> remove feats from selection
            __remove_feat_id('lab_feat')
        
        df = self.inproc_data_
        df = df[feats]
        self.__update_df_inproc(df, 'feat_selection')
        
        # generate new clinical features from text search and feature unification
        if generate_clin_feat:
            self.generate_clinical_feat()
        return
    
    def generate_clinical_feat(self, combine=True):
        '''
        This function generates the boolean clinical features by:
            - processing the < 'OUTRO_DES','MORB_DESC' > text features using preseted regex patterns 
            - unifying the identified (regex) patterns with the openDataSUS original clinical features
            - unifying some strategic clinical features into single one 
        
        Parameters
        ----------
        combine : bool, The default is True.
            Wheter to combine some (pre-defined) strategic clinical features into single one. 

        Returns
        -------
        None.
        (Modifies the private attribute of the dataframe under processing)

        '''
        
        if self.__log_proc[self.inproc_]['generate_clinical_feat']:
            print('Clinical features generation is updated')
            return
        print('.. clinical features generation from text processing < OUTRO_DES','MORB_DESC > SRAG features.')
        
        # existent clinical features >> adjustments
        with open(self._file_clin_feat, 'r') as f: 
            clin_feat = json.load(f)
        with open(self._file_feat_clin2bool, 'r') as f: 
            dic_replace = json.load(f)
        with open(self._file_feat_regex, mode='rt') as f:
            feat_regex = json.load(f)
        with open(self._file_feat_unification, mode='rt') as f:
            feat_unif = json.load(f)
        
        df = self.inproc_data_
        
        # values adjustments
        df.CS_GESTANT = df.CS_GESTANT.astype(str)
        df.replace(dic_replace, inplace=True)
        
        # new clinical features >> text search
        text_feat = ['OUTRO_DES','MORB_DESC']
        textSearch = df[text_feat].fillna('').agg(' '.join, axis=1).replace(r'^\s*$', np.nan, regex=True)
        textSearch = textSearch.str.lower()
        #df.drop(columns=text_feat, inplace=True)
        
        # add newly generated feats from regex search
        for feature in feat_regex.keys(): 
            df[feature] = textSearch.str.contains(feat_regex[feature], case=False, regex=True)
        # unifying generated features with existent srag features
        for feat_srag, feat in feat_unif:
            df[feat] = df[[feat,feat_srag]].astype(str).fillna('').agg(''.join, axis=1).replace(r'^\s*$', np.nan, regex=True).str.contains('True', case=False)
        # add original features for which no regex was implemented
        df['doenca_hematologica'] = df['HEMATOLOGI']
        df['sindrome_down'] = df['SIND_DOWN']
        df.drop(columns=clin_feat, inplace=True) # drop columns of original srag clinical features
        
        # generate combined clinical features
        if combine:
            df['alteracao_olfato_paladar'] = (df['alteracao_olfato']==True) | (df['alteracao_paladar']==True)
            df['alteracao_respiratoria'] = (df['disturbios_respiratorios']==True) | (df['cianose']==True) | (df['saturacao_menor_95']==True)
            df['sintoma_gripal'] = (df['congestao_nasal']==True) | (df['coriza']==True) | (df['espirro']==True) | (df['cefaleia']==True) | (df['dor_de_garganta']==True) | (df['dores_corpo']==True) | (df['adinamia']==True) | (df['mal_estar']==True) | (df['dor_olhos']==True)
            df['nausea_vomito'] = (df['nausea']==True) | (df['vomito']==True)
        #new_clin_feat += ['alteracao_olfato_paladar', 'alteracao_respiratoria', 'sintoma_gripal', 'nausea_vomito']
        
        # new clinical features
        new_clin_feat = [feat for feat in df.columns if feat.islower()]
        if self.survival_status_feat_ in new_clin_feat: new_clin_feat.remove(self.survival_status_feat_)
        if self.survival_time_feat_ in new_clin_feat:  new_clin_feat.remove(self.survival_time_feat_)
        
        # set clinical features to boolean dtype
        df = df.astype(dict.fromkeys(new_clin_feat,'boolean'))   
        # adjust names to upper case
        upper_cases = {col: col.upper() for col in new_clin_feat}
        df.rename(columns=upper_cases, inplace=True)
        
        # save names of generated clinical features
        self.__generated_clin_feat = [feat.upper() for feat in new_clin_feat]
        
        self.__update_df_inproc(df, 'generate_clin_feat')
        self.__log_proc[self.inproc_]['generate_clinical_feat'] = {'combine': combine}
        return 
    
    def add_ibge_feats(self):
        '''
        !!! NOT IMPLEMENTED
        '''
        
        if self.__log_proc[self.inproc_]['add_ibge_feats']:
            print('IBGE features generation is updated')
            return
        self.__log_proc[self.inproc_]['add_ibge_feats'] = True # add future method arguments in dictionary type for **kwargs
        print('.. IBGE features extraction from data source ({})'.format(self.__log_proc[self.inproc_]['add_ibge_feats']))
        
        ibge_feats = []
        self.__other_demo_feat += ibge_feats
        
        # self.__update_df_inproc(df) # update with udpdated dataframe containing IBGE features
        return
    
    def save_surv_process(self, concat=False, save_srag=True): # true or false?
        '''
        This function saves all processed data open in SUSurv object.
        
        Parameters
        ----------
        concat : bool, optional
            Whether to concatenate all data sets processed and registered in the 
            proc_status(). The default is False (saves separete files for each data set).
            
        save_srag : bool, optional
            Whether to save the original SRAG data set retrieved from source 
            (in case of pending download). The default is True.
        **kwargs:
            
            
        Returns
        -------
        None.
        (Generates files in the local repository)

        '''
        if self.__open:
            self.close_surv_processing()
        
        # ADJUST FOR self.__dfs_proc = {db_ref : pandas.dataframe    
        print('\n>> started SUSurv saving process\n.')
        
        # generate processed folder and save dbs (sep or concat)
        self._create_proc_folder()
        
        # save files in Repository
        if concat:
            self.__set_concat()
            self._save_proc_file(self.__df_concat, 'concat')
        else:
            for db_ref, df in self.__dfs_proc.items():
                self._save_proc_file(df, db_ref)
        
        # update logs
        self.__log_gen['save_concat'] = concat
        self.__log_gen['save_orig'] = save_srag
        self.proc_status()
        
        self.__loc_proc['general_info'] = self.__log_gen
        self._close_repo(save_orig=save_srag, proc_log=self.__log_proc)
        self.__init__() # reset the class obj
        return
    
    #==================================================
    #  DATA PROCESSING PROCEADURES 
    # 
    # OBS:
    #
    #==================================================
    
    def data_processing(self, db_ref, generate_clin_feat=True, demo_feat=True, lab_feat=False, ibge_data=False):
        '''
        Data processing proceadure

        Parameters
        ----------
        db_ref : string
            ['srag20', 'srag21']
            Reference to the data set to be processed.
        generate_clin_feat : bool, optional
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
        
        self.open_surv_processing(db_ref)
        
        # CASE SELECTION
        self.case_selection()
        
        # FEAT SELECTION
        self.feat_selection(generate_clin_feat=generate_clin_feat, demo_feat=demo_feat, lab_feat=lab_feat)
        if ibge_data:
            self.add_ibge_feats()
            
        self.close_surv_processing()
        
        return