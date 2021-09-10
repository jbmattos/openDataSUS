# -*- coding: utf-8 -*-
"""
Created on Fri Sep 10 10:12:35 2021

@author: jubma
"""


########################################################################
########################################################################
#                         SCRIPT EXECUTION                             #
########################################################################
########################################################################


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
                        choices=["icu","icu_death", "death"],
                        default="obitoUTI", type=str,
                        help="The survival event")
    ## ADD ALL data_processing params

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