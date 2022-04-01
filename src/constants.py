import os
from pyspark.sql.types import StructType,StructField, StringType


# Dirs
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INPUT_DIR = os.path.join(PROJECT_ROOT, "Input")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "Output")
[os.makedirs(dir_, exist_ok=True) for dir_ in (INPUT_DIR, OUTPUT_DIR)]

# GCS
GCS_PROJECT = os.getenv('TF_VAR_project')
GCS_BUCKET = 'de-final-datalake_dtc-de-course-2022-339213'
INPUT_PARTITION = "Input/"
OUTPUT_PARTITION = "Output/Active-Attribution/output/"
ARCHIVE_PARTITION = "Archive/output/"



sheet_name_list = ['ESG Active-Classical GICS ', 'ESG Active-Total Exposure ', \
                   'ESG Active-ESG Risk Score ','ESG Active-Controversy Sco ', \
                    'ESG Active-CO2 Emissions ', 'ESG Active-Waste Intensity ', \
                    'ESG Active-ESG Risk Moment ', 'ESG Active-NNIP Risk Indic ', \
                     'ESG Active-Water Intensity ', 'ESG Active-Classical GICS (1) ', \
                    'ESG Active-ESG Risk Score(1) ','ESG Active-Total Exposure (1) ',  \
                    'ESG Active-Controversy Sco(1) ','ESG Active-CO2 Emissions(1) ',  \
                    'ESG Active-Waste Intensity(1) ','ESG Active-ESG Risk Moment(1) ',  \
                    'ESG Active-NNIP Risk Indic(1) ','ESG Active-Water Intensity(1) ',   \
                    'ESG Active-Classical GICS (2) ','ESG Active-ESG Risk Score(2) ',  \
                     'ESG Active-Total Exposure (2) ','ESG Active-Controversy Sco(2) ', \
                    'ESG Active-CO2 Emissions(2) ','ESG Active-Waste Intensity(2) ',   \
                     'ESG Active-ESG Risk Moment(2) ','ESG Active-NNIP Risk Indic(2) ',  \
                     'ESG Active-Water Intensity(2) ']

schema =  StructType([StructField('Description', StringType(), True), \
    StructField('Filter_Level_1', StringType(), True), \
    StructField('Filter_Level_2', StringType(), True),  \
     StructField('Filter_Level_3', StringType(), True),  \
    StructField('Level', StringType(), True),     \
     StructField('CUSIP', StringType(), True),   \
     StructField('Portfolio_weight', StringType(), True),  \
     StructField('Benchmark_weight', StringType(), True),   \
     StructField('Active_weight', StringType(), True),     \
     StructField('Portfolio_return', StringType(), True),   \
     StructField('Benchmark_return', StringType(), True),   \
     StructField('Active_return', StringType(), True),    \
     StructField('Portfolio_returncontribution', StringType(), True), \
     StructField('Benchmark_returncontribution', StringType(), True), \
    StructField('Active_returncontribution', StringType(), True),   \
    StructField('Sector_Allocation', StringType(), True),   \
     StructField('Security_Selection', StringType(), True),  \
     StructField('time_lens', StringType(), True),      \
     StructField('Unique_id', StringType(), True),    \
      StructField('Portfolio_Code', StringType(), True), \
      StructField('Year', StringType(), True),  \
     StructField('Month', StringType(), True),  \
    StructField('Day', StringType(), True)
                     ])


columns_dict = {
       "Filter Level 1": "Filter_Level_1",
       "Filter Level 2": "Filter_Level_2",
       "Filter Level 3": "Filter_Level_3",
       "Portfolio": "Portfolio_weight",
       "Benchmark": "Benchmark_weight",
       "Active": "Active_weight",
       "Portfolio (bp)": "Portfolio_return",
       "Benchmark (bp)": "Benchmark_return",
       "Active (bp)": "Active_return",
       "Portfolio (bp).1": "Portfolio_returncontribution",
       "Benchmark (bp).1": "Benchmark_returncontribution",
       "Active (bp).1": "Active_returncontribution",
       "Sector Allocation (bp)": "Sector_Allocation",
       "Security Selection (bp)": "Security_Selection",
       "Portfolio_from_file": "Portfolio"
                     }

print('*****************---------->>>>>>>>')
print(GCS_BUCKET)
print(os.getenv('TF_VAR_project'))