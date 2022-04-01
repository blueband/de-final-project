#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext
import pyspark

from functools import reduce
from pyspark.sql.functions import *

from pyspark.sql import SparkSession, DataFrame
import os, sys
from os import walk

from constants import *
from file_retriever import *

spark = SparkSession.builder.config('spark.jars.packages', 'com.crealytics:spark-excel_2.12:0.14.0') \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.exec_memory", "3g") \
        .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')     # Changing error output to ERROR insteadof WARNING



def get_current_sheet_data(file_name, sheet_name):
#     Get sheet_name and its content
    df = spark.read.format("excel").option("header", 'true'). \
        option("dataAddress", f"0!A1:Q258") \
        .schema(schema).load(f"{file_name}")
    
    return sheet_name, df



def rename_column(columns_dict, dataframe):
    for old_col, new_col in columns_dict.items():
        dataframe = dataframe.withColumn('Filter_Level_1', regexp_replace('Filter_Level_1',old_col,  new_col))
        dataframe = dataframe.withColumn('Filter_Level_2', regexp_replace('Filter_Level_2',old_col,  new_col))
        dataframe = dataframe.withColumn('Filter_Level_3', regexp_replace('Filter_Level_3',old_col,  new_col))
        dataframe = dataframe.withColumn('Portfolio_weight', regexp_replace('Portfolio_weight', old_col, new_col))
        dataframe = dataframe.withColumn('Benchmark_weight', regexp_replace('Benchmark_weight', old_col, new_col))
        dataframe = dataframe.withColumn('Active_weight', regexp_replace('Active_weight', old_col, new_col))
        if old_col == 'Portfolio (bp)':
            old_col = 'Portfolio'
            dataframe = dataframe.withColumn('Portfolio_return', regexp_replace('Portfolio_return', old_col, new_col))
        if old_col == 'Benchmark (bp)':
            old_col = 'Benchmark'
            dataframe = dataframe.withColumn('Benchmark_return', regexp_replace('Benchmark_return', old_col, new_col))
        if old_col == 'Active (bp)':
            old_col = 'Active'
            dataframe = dataframe.withColumn('Active_return', regexp_replace('Active_return', old_col, new_col))
        if old_col == 'Portfolio (bp).1':
            old_col = 'Portfolio'
            dataframe = dataframe.withColumn('Portfolio_returncontribution', regexp_replace('Portfolio_returncontribution', old_col, new_col))
        if old_col == 'Benchmark (bp).1':
            old_col = 'Benchmark'
            dataframe = dataframe.withColumn('Benchmark_returncontribution', regexp_replace('Benchmark_returncontribution', old_col, new_col))
        if old_col == 'Active (bp).1':
            old_col = 'Active'
            dataframe = dataframe.withColumn('Active_returncontribution', regexp_replace('Active_returncontribution', old_col, new_col))
        if old_col == 'Sector Allocation (bp)':
            old_col = 'Sector Allocation'
            dataframe = dataframe.withColumn('Sector_Allocation', regexp_replace('Sector_Allocation', old_col, new_col))
        if old_col == 'Security Selection (bp)':
            old_col = 'Security Selection'
            dataframe = dataframe.withColumn('Security_Selection', regexp_replace('Security_Selection', old_col, new_col))
    return dataframe


def row_filter(df):
    dataframe = df.filter(df.Level.contains('Level') | (df.Level.contains('3') & ~df.Level.endswith('0')))
    
    return dataframe
    


def get_time_lens(sheet_name):
    # Adding time_len based on sheet_postfix
    if '1' in sheet_name:
        return 'QTD'
    elif '2' in sheet_name:
        return 'YTD'
    else:
        return 'MTD'

def generate_unique_id(Portfolio_Code, Reporting_Date,time_lens,dataframe):
    # Adding Unique ID to each record
    # format: Portfolio Code_Reporting Date_time_lens_Filter Level 1_Filter Level 2
    # IF0984_30-SEP-2020_MTD_ESG GICS_Cash & FX
    exclude_cell = [None, '', 'Layout', 'ESG Active Attributions MTD', 'Filter Level 1', 'Filter Level 2',
                   'NN Europees Deelnemingen Fonds', 'Portfolio Full Name']
    
    for row in dataframe.collect():
        
        if not (row.Filter_Level_1 or row.Filter_Level_2) in exclude_cell:
            if type(row.Filter_Level_2) == str:
                unique_id = list(Portfolio_Code.values())[0] + '_' + list(Reporting_Date.values())[0] \
                            + '_' + time_lens + '_' + row.Filter_Level_1 + '_' + row.Filter_Level_2
                # updating row in col19 base current row in col2 on the current sheet
                dataframe = dataframe.withColumn('Unique_id', when(dataframe.Filter_Level_1 == lit(row.Filter_Level_1), \
                            unique_id).otherwise("")) 
                
            else:
                unique_id = list(Portfolio_Code.values())[0] + '_' + list(Reporting_Date.values())[0] \
                + '_' + time_lens + '_' + row.Filter_Level_1
                dataframe = dataframe.withColumn('Unique_id', when(dataframe.Filter_Level_1 == lit(row.Filter_Level_1), \
                            unique_id).otherwise(""))
    return dataframe


def add_year_month_day_value(Portfolio_Code,Reporting_Date, dataframe):
    Portfolio_code = list(Portfolio_Code.values())[0]
    expl_date_format = list(Reporting_Date.values())[0].split('-')
    Day = expl_date_format[0]
    Month = expl_date_format[1]
    Year = expl_date_format[2]

                          
    for row in dataframe.collect():
        # updating row in col19 base current row in col2 on the current sheet
        dataframe = dataframe.withColumn('Portfolio_Code', when(dataframe.Filter_Level_1 == lit(row.Filter_Level_1), \
                   Portfolio_code).otherwise("")) 
        dataframe = dataframe.withColumn('Year', when(dataframe.Filter_Level_1 == lit(row.Filter_Level_1), \
                    Day).otherwise(""))
        dataframe = dataframe.withColumn('Month', when(dataframe.Filter_Level_1 == lit(row.Filter_Level_1), \
                    Month).otherwise(""))
        dataframe = dataframe.withColumn('Day', when(dataframe.Filter_Level_1 == lit(row.Filter_Level_1),  \
                    Year).otherwise(""))
    return dataframe


def data_transformation(file, sheet_name_list):
    full_data = dict()
    print('***************** Transformation Job process Start**************')
    print('***************** Starting working on filename {} now **************'.format(file))
    
    tmpdata = dict()
    Portfolio_Code = dict()
    Reporting_Date = dict()
    for sheet in sheet_name_list:
        current_sheet, data = get_current_sheet_data(file,sheet)
        data = data.withColumn('Filter_Level_3', regexp_replace('Filter_Level_3','Portfolio Short Name',  'Portfolio Code'))
        data = data.withColumn('Level', regexp_replace('Level','Date',  'Reporting Date'))
        data = data.withColumn('Portfolio_return', when(data.Portfolio_return.startswith("-"), lit('0.00')).otherwise(data.Portfolio_return))
        data = data.withColumn('Benchmark_return', when(data.Benchmark_return.startswith("-"), lit('0.00')).otherwise(data.Benchmark_return))
        data = data.withColumn('Active_return', when(data.Active_return.startswith("-"), lit('0.00')).otherwise(data.Active_return))
        data = data.withColumn('Portfolio_returncontribution', when(data.Portfolio_returncontribution.startswith("-"), lit('0.00')).otherwise(data.Portfolio_returncontribution))
        data = data.withColumn('Benchmark_returncontribution', when(data.Benchmark_returncontribution.startswith("-"), lit('0.00')).otherwise(data.Benchmark_returncontribution))
        data = data.withColumn('Active_returncontribution', when(data.Active_returncontribution.startswith("-"), lit('0.00')).otherwise(data.Active_returncontribution))
        data = data.withColumn('Sector_Allocation', when(data.Sector_Allocation.startswith("-"), lit('0.00')).otherwise(data.Sector_Allocation))
        data = data.withColumn('Security_Selection', when(data.Security_Selection.startswith("-"), lit('0.00')).otherwise(data.Security_Selection))
        
        # Retrieve specific value from a cordinate (Row, Col)
        Portfolio_Code['Portfolio Code'] = data.collect()[2][3]
        Reporting_Date['Reporting Date'] = data.collect()[2][4]
        
        
    
        # Derive curent time_len based on the current sheet_postfix
        time_len_value = get_time_lens(sheet)
        
        # update current sheet with derived time_len_value
        # data = data.withColumn('col18', when(data.col18.startswith(''), lit(time_len_value)).otherwise(data.col18)) 
        
        # Write new column name
        data = data.withColumn('time_lens', when(data.Filter_Level_3 == lit('Filter Level 3'), time_len_value).otherwise(''))
        
        # Adding Unique ID to each record
        # format: Portfolio Code_Reporting Date_time_lens_Filter Level 1_Filter Level 2
        # IF0984_30-SEP-2020_MTD_ESG GICS_Cash & FX
        data = generate_unique_id(Portfolio_Code, Reporting_Date, time_len_value, data)
        data = add_year_month_day_value(Portfolio_Code,Reporting_Date, data)
        
        # Column rename
        data = rename_column(columns_dict, data)
        data = row_filter(data)
        tmpdata[current_sheet] = data
        # full_data[file] = (tmpdata, Portfolio_Code, Reporting_Date)
        
    print('***************** Complete Transformation of filename {} now **************'.format(file))
    return tmpdata, Portfolio_Code, Reporting_Date




def df_merge(df):
    print('********* Initializing Merge step for all DataFrame in the current sheet/folder ******')
    tmp_dataframe = []

    df_item = list(df.values())
    for dataframe in df.values():
        tmp_dataframe.append(dataframe)

    merged_file = []
    merged_df = reduce(DataFrame.unionAll, tmp_dataframe)
    return merged_df        
    

# write to parguet file with format 
def write_parquet_file(dataframe_obj):
    # Partition based Year=?Month=?Day?Portfolio_code=? 
    print('****** Initialize writing Dataframe to parquet format *******************')
    dataframe_obj.write.partitionBy('Year', 'Month', 'Day', 'Portfolio_code').parquet(OUTPUT_DIR)
    print('***** Completed writing Dataframe to parquet format *******************')





if __name__ == "__main__":

    # *********************** Retrieve File from Remote bucket*******************
    source_file_name = {
            "bucket": GCS_BUCKET,
            "name": "Input/30_09_2020.zip",
            "metageneration": "",
            "timeCreated": "",
            "updated": ""
        }

    file_name = source_file_name["name"]
    if check_local_Source_file(file_name):
        local_file_dir = get_fetch_local_file(file_name)
    else:
        local_file_dir, backfill_status = fetch_files(file_name, GCS_BUCKET)
        



    # complete_file_name = []
    file_list = os.listdir(os.path.join(local_file_dir, "Active Attribution"))
    complete_file_name = [ os.path.join(local_file_dir, "Active Attribution", file_name) for file_name in file_list]

    # print(file_path_list)
    for file in complete_file_name:
        data_df, Portfolio_Code, Reporting_Date = data_transformation(file, sheet_name_list)


        # # Merge all cell in a file file and return file_name and its equivalent datafrmae
        merged_dataframe = df_merge(data_df)


        # for dataframe in merged_data:
        write_parquet_file(merged_dataframe)


        # # upload files
        print(f"Starting GCS Upload to {OUTPUT_PARTITION}")
        upload_dir(OUTPUT_DIR, OUTPUT_PARTITION, GCS_BUCKET)

        # print(f"Pushing {file_metadata[-1]} to Archive")
        # gcs_archive_path = f"{ARCHIVE_PARTITION}{file_metadata[-1]}"
        # zippath = os.path.join(INPUT_DIR, file_metadata[-1])
        # upload_blob(zippath, gcs_archive_path, bucket)
