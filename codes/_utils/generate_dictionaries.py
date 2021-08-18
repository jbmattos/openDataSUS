# -*- coding: utf-8 -*-
"""
CODE FOR GENERATING DICTIONARY OF FEATURES
[2021-07-19]
    THIS CODE USES A PREVIOUSLY CREATED (BY RENATO) DICTIONARY OF FEATURES FOR THE SRAG DATABASE 
    AND ADJUSTS TWO NEW DICTIONARIES FOR THE SRAG DATABASES OF 2020 AND 2021 RETRIEVED IN 2021-07-07
    (changes in the data structure after this data may not be addressed in this pipeline)

    EXECUTION: THE SCRIPT SHOULD RUN FROM ANYWERE INSIDE 'openDataSUS' FOLDER
    INPUT FILE: openDataSUS\\data\\__data_dictionary\\_srag_orig_dicionario.json
    
    ISSUES:
        #1 datestamp from log do not update after first execution.
        #2 add argparse option to save the json files (in case changes may be needed)
    
"""

import argparse
import json
import numpy as np
import os

from datetime import datetime
from repository_cls import Repository

if __name__ == '__main__':
    
    # ARG PARSE SETTINGS
    parser = argparse.ArgumentParser(description="Generates dictionary of types for features of the srag databases 2020/2021")
    
    parser.add_argument("--verb", action="store_true",
                        help="Print features analysis")
    args = parser.parse_args()
    
    REPO = Repository(check_dic=False)
    
    # PATHS
    ROOT = REPO.root
    SRAG20 = REPO.db20_name
    SRAG21 = REPO.db21_name
    
    THIS_FILE = '{}{}/{}'.format(ROOT,os.path.dirname(__file__).split(ROOT)[1],os.path.basename(__file__))
    PATH = os.path.dirname(__file__).split(ROOT)[0]+ROOT # read system path up to ROOT
    PATH_DATA = PATH + "/data/"
    INPUT_FILE = '/data/__data_dictionary/_srag_orig_dicionario.json'
    
    # SRAG ORIGINAL DICTIONARY
    with open(PATH+INPUT_FILE,'r') as f:
        SRAG_DIC = json.load(f)
    DIC_FEAT = list(SRAG_DIC.keys())
    
    #########################################################################
    ## FILE: /data/__data_dictionary/feat2remove_basedic.json (in 20210811)
    #########################################################################
    # keys to remove from the original SRAG_DIC (constructed by Renato) 
    # >> those feat are not in the new DBS(20/21)
    remove_from_dic = ['DS_IF_OUT', 'DT_IF', 'IF_ADENO', 'IF_OUTRO',
                       'IF_PARA1', 'IF_PARA2', 'IF_PARA3', 'IF_RESUL',
                       'IF_VSR', 'POS_IF_FLU', 'POS_IF_OUT', 'TP_FLU_IF']
    new_srag = SRAG_DIC.copy()
    for feat in remove_from_dic:
        del new_srag[feat]
    
    #########################################################################
    ## FILE: /data/__data_dictionary/add_2020.json (in 20210811)
    #########################################################################
    # new features to add to DB-2020 (and not in the original SRAG_DIC)
    # verify: category with only code [1]: can NaN be "ignorado"?
    add_dic_2020 = {
        'AN_ADENO': {"type":"category", "codes":[1], "categories":["sim"]}, 
        'AN_OUTRO': {"type":"category", "codes":[1], "categories":["sim"]}, 
        'AN_PARA1': {"type":"category", "codes":[1], "categories":["sim"]},
        'AN_PARA2': {"type":"category", "codes":[1], "categories":["sim"]},
        'AN_PARA3': {"type":"category", "codes":[1], "categories":["sim"]},
        'AN_SARS2': {"type":"category", "codes":[1], "categories":["sim"]},
        'AN_VSR': {"type":"category", "codes":[1], "categories":["sim"]},
        'DOR_ABD': {"type":"category", "codes":[1,2,9], "categories":["sim","nao","ignorado"]},
        'DS_AN_OUT': {"type":"string"},
        'DT_CO_SOR': {"type":"string"},
        'DT_RES': {"type":"string"},
        'DT_RES_AN': {"type":"string"},
        'DT_TOMO': {"type":"string"},
        'FADIGA': {"type":"category", "codes":[1,2,9], "categories":["sim","nao","ignorado"]}, 
        'FATOR_RISC': {"type":"category", "codes":["S","N"], "categories":["sim","nao"]}, 
        'OUT_ANIM': {"type":"string"},
        'OUT_SOR': {"type":"string"},
        'PERD_OLFT': {"type":"category", "codes":[1,2,9], "categories":["sim","nao","ignorado"]},  
        'PERD_PALA': {"type":"category", "codes":[1,2,9], "categories":["sim","nao","ignorado"]}, 
        'POS_AN_FLU': {"type":"category", "codes":[1,2,9], "categories":["sim","nao","ignorado"]}, 
        'POS_AN_OUT': {"type":"category", "codes":[1,2,9], "categories":["sim","nao","ignorado"]}, 
        'RES_AN': {"type":"category", "codes":[1,2,3,4,5,9], "categories":["positivo","negativo","inconclusivo","nao_realizado","aguardando","ignorado"]}, 
        'RES_IGA': {"type":"category", "codes":[1,2,3,4,5,9], "categories":["positivo","negativo","inconclusivo","nao_realizado","aguardando","ignorado"]},
        'RES_IGG': {"type":"category", "codes":[1,2,3,4,5,9], "categories":["positivo","negativo","inconclusivo","nao_realizado","aguardando","ignorado"]},
        'RES_IGM': {"type":"category", "codes":[1,2,3,4,5,9], "categories":["positivo","negativo","inconclusivo","nao_realizado","aguardando","ignorado"]},
        'SOR_OUT': {"type":"string"},
        'TOMO_OUT': {"type":"string"},
        'TOMO_RES': {"type":"category", "codes":[1,2,3,4,5,6,9], "categories":["tipico_COVID19","indeterminado_COVID19","atipico_COVID19","negativo_pneumonia","outro","nao_realizado","ignorado"]},
        'TP_AM_SOR': {"type":"category", "codes":[1,2,9], "categories":["sangue_plasma_soro","outra_qual","ignorado"]},
        'TP_FLU_AN': {"type":"category", "codes":[1,2], "categories":["influenza_A","influenza_B"]},
        'TP_SOR': {"type":"category", "codes":[1,2,3,4], "categories":["teste_rapido","elisa","quimiluminescencia","outro_qual"]}, 
        'TP_TES_AN': {"type":"category", "codes":[1,2], "categories":["imunofluorescencia_IF","teste_rapido_antigenico"]}
    }
     
    #########################################################################
    ## FILE: /data/__data_dictionary/add_2021.json (in 20210811)
    #########################################################################
    # new features to add to DB-2021 (and not in the original SRAG_DIC nor in DB-2020)
    add_dic_2021 = {
        'DOSE_1_COV': {"type":"string"},
        'DOSE_2_COV': {"type":"string"},
        'ESTRANG': {"type":"category", "codes":[1,2], "categories":["sim","nao"]}, # (v=1)=4188, (v=2)=701246 >> assumi 1 para estrangeiro, 2 para não estrangeiro (dicionario de dados descreve valores=[sim, não])
        'FNT_IN_COV': {"type":"category", "codes":[1,2], "categories":["manual","RNDS"]},
        'LAB_PR_COV': {"type":"string"},
        'LOTE_1_COV': {"type":"string"},
        'LOTE_2_COV': {"type":"string"},
        'VACINA_COV': {"type":"category", "codes":[1,2,9], "categories":["sim","nao","ignorado"]}
    }
    
    if args.verb:
        # executed when argparse --verb
        
        with open(PATH_DATA+SRAG20+'.csv', "r",encoding='utf-8') as f:
            db20_line1 = f.readline()
        with open(PATH_DATA+SRAG21+'.csv', "r",encoding='utf-8') as f:
            db21_line1 = f.readline()
        
        db20_feat = db20_line1.replace('"','').split(';')
        db21_feat = db21_line1.replace('"','').split(';')
        
        print('>> VERBOSE\n>> Inconsistencies between original dictionary and dataset features')
        
        print('>> Features load from:')
        print('.. {}{}.csv'.format(PATH_DATA,SRAG20))
        print('.. {}{}.csv'.format(PATH_DATA,SRAG21))
        
        print('\nAll features in ORIG_DIC are in {}'.format(SRAG20))
        print('>> ', set(DIC_FEAT).issubset(set(db20_feat)))
        
        print('All features in ORIG_DIC are in {}'.format(SRAG21))
        print('>> ', set(DIC_FEAT).issubset(set(db21_feat)))
        
        print('\n >> DB-2020:')
        print('\n - In ORIG_DIC but not in DB:')
        print(list(np.setdiff1d(DIC_FEAT,db20_feat)))
        print('\n - In DB but not in ORIG_DIC:')
        print(list(np.setdiff1d(db20_feat,DIC_FEAT)))
        
        print('\n >> DB-2021:')
        print('\n - In ORIG_DIC but not in DB:')
        print(list(np.setdiff1d(DIC_FEAT,db21_feat)))
        print('\n - In DB but not in ORIG_DIC:')
        print(list(np.setdiff1d(db21_feat,DIC_FEAT)))
        
        print('\n >> DB-2021 vs DB-2020')
        print('\n - In DB-2021 but not in DB-2020:')
        print(list(np.setdiff1d(db21_feat,db20_feat)))
    

    ##################################
    # Save new Dictionary of Features 
    # for srag2020 and srag2021
    ##################################
    
    dic_srag20 = {**new_srag, **add_dic_2020}
    dic_srag21 = {**dic_srag20, **add_dic_2021}
    
    dic20_path = PATH_DATA+SRAG20+ '_dictionary.json'
    dic21_path = PATH_DATA+SRAG21+ '_dictionary.json'
    
    with open(dic20_path,'w') as f:
        json.dump(dic_srag20, f, indent=1)
    with open(dic21_path,'w') as f:
        json.dump(dic_srag21,f, indent=1)
    
    ##################
    # PRINT LOG FILE #
    ##################
    dic20_path = ROOT+dic20_path.split(ROOT)[1]
    dic21_path = ROOT+dic21_path.split(ROOT)[1]
    with open(PATH_DATA+ '\\__data_dictionary\\_log.txt', 'w') as f:
        f.write('>> "_dictionary.json" files generated in {}'.format(datetime.now().strftime("%Y-%m-%d %H:%m:%S")))
        f.write('\n.. path of execution: {}'.format(THIS_FILE))
        f.write('\n.. input base file: {}{}'.format(ROOT,INPUT_FILE))
        f.write('\n.. generated file: {}'.format(dic20_path))
        f.write('\n.. generated file: {}'.format(dic21_path))
        f.write('\n(description) Dictionary of features for the data files with same names')
    
    print('\n\n!!! Saved dictionary files in {}'.format(PATH_DATA))
