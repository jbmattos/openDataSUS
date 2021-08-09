# -*- coding: utf-8 -*-
'''
CODE FOR PROCESSING THE SRAG DATABASES FROM OPENDATASUS
[2021-07-19]
    
    THIS CODE AUTOMATICALLY RETRIEVES THE SRAG20 AND SRAG21 DATABASES FROM 
    THE OPENDATASUS SOURCE AND PERFORMS PROCESSING TO ADJUST THE DATA SET 
    INTO SURVIVAL DATA FORMAT
    
    (this script was created for the databases retrieved in 2021-07-07;
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
import pandas as pd
import os

from datetime import date, datetime

DATESTAMP = date.today().strftime('%Y%m%d')

# PATHS
ROOT = "openDataSUS"
SRAG20 = "INFLUD-28-06-2021"
SRAG21 = "INFLUD21-28-06-2021"

THIS_FILE = '{}{}\\{}'.format(ROOT,os.path.dirname(__file__).split(ROOT)[1],os.path.basename(__file__))
PATH = os.path.dirname(__file__).split(ROOT)[0]+ROOT # read system path up to ROOT
PATH_DATA = PATH + "\\data\\"
PATH_SAVE = PATH + "\\process_data_{}\\".format(DATESTAMP)

# INPUT FILES
SRAG20_DIC = SRAG20+"_dictionary.json"
SRAG21_DIC = SRAG21+"_dictionary.json"
COL_SELECTION = "_srag_colSelection.json"
CLINICAL_FEAT = "_srag_clinicalFeat.json"
FEAT_REPLACE = "_srag_clinicalFeatReplace.json"
FEAT_REGEX = "_srag_featRegex.json"
FEAT_UNIFICATION = "_srag_featUnification.json"

def _log():
    '''
    This function is executed inside option --save
    Use argparse parameter <--log=False> to deactivate the function
    '''

    with open(PATH_DATA+ '\\__data_dictionary\\_log.txt', 'w') as f:
        '''
        Log function to files generated inside out-folder
        '''
        f.write('>> "_dictionary.json" files generated in {}'.format(datetime.now().strftime("%Y-%m-%d %H:%m:%S")))
        #f.write('\n.. path of execution: {}'.format())
        #f.write('\n.. input base file: {}{}'.format())
        #f.write('\n.. generated file: {}'.format())
        #f.write('\n.. generated file: {}'.format())
        #f.write('\n(description)')
    return

def _save_dbs(no_save):
    '''
    --save=[True, False], default=True >> wheter to save (override) the original dataset files in \\openDataSUS\\data
                                          save as "temp_" file
                                        >> if True, 
                                        >> 
    '''
    if no_save:
        # remove "temp_" files in data folder
    else:
        # save a '_log_process.txt' with information on the saved datasets (include path of this script)
        # if process is successful, remove "temp_" to final name
    return

def save(no_save):
    '''
    
    '''
    # save output files inside out-file
    _log()
    _save_dbs(no_save)
    return

def processing_pipeline():
    '''
    Pipeline for processing SRAG database into survival data.
    '''
    
    return #df_proc


if __name__ == '__main__':

    # ARG PARSE SETTINGS
    parser = argparse.ArgumentParser(description="Script to automatically retrieve SRAG databases from openDataSUS and process into survival data")
    
    parser.add_argument("--db", 
                        choices=["srag20", "srag21", "both"],
                        default="both", type=str, 
                        help="Srag databases to process")
    parser.add_argument("--concat", action='store_true', 
                        help="If --db=both, saves srag20 and srag21 in a single processed data file")
    parser.add_argument("--sus", action='store_true', 
                        help="Download the updated databases from openDataSUS. Else, uses the dbs in the repository")
    parser.add_argument("--nosave", action='store_false', 
                        help="Do not save the downloaded openDataSUS databases. Else, overrides the dbs in the repository")
    parser.add_argument("--event", 
                        choices=["obito","obitoUTI","graves"],
                        default="obitoUTI", type=str,
                        help="The survival event")
    parser.add_argument("--cases", 
                        choices=["all","hosp","uti"],
                        default="all", type=str, 
                        help="The covid cases to compose the data")
    
    args = parser.parse_args()
    