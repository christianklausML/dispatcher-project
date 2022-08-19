import os

BUCKET_NAME = "wagon-data-817-dispatcher"
RED_DATA_GC_PATH = 'RAW_DATA/Reduced'
CLEAN_DATA_GC_PATH = 'RAW_DATA/Clean'

PRED_RED_DATA_GC_PATH = 'RAW_DATA/Pred_Reduced' # Path to be created in gcp
PRED_CLEAN_DATA_GC_PATH = 'RAW_DATA/Pred_Clean' # Path to be created in gcp

PATH_TO_LOCAL_MODEL = 'model.joblib'
RED_DATA_LOCAL_PATH = os.path.abspath(os.path.join(__file__,'..','..',
                                                'raw_data','reduced'))
CLEAN_DATA_LOCAL_PATH = os.path.abspath(os.path.join(__file__,'..','..',
                                                'raw_data','clean'))

PRED_RED_DATA_LOCAL_PATH = os.path.abspath(os.path.join(__file__,'..','..',
                                                'raw_data','pred_reduced'))
PRED_CLEAN_DATA_LOCAL_PATH = os.path.abspath(os.path.join(__file__,'..','..',
                                                'raw_data','pred_clean'))