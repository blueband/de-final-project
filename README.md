# Problem 
 Background Descripition:

'These dataset are made available at end of each month in a compressed format on Google Drive. A single compressed file consists of more than one folder, and each folder consist of more than one excel files, and each excel file consists of multiple sheet

# Project Goals
Generally, the goal is to  design and build data pipeline and equally perform transformation process on the raw data in DL and move transform data into DWH for further processing before presentation. Below are list of transformation stage goals

1. Ingest file from Google drive on monthly as the file will be available on 30th 28th of every month, then perform below process on the file
Extract the given input compress file recursively, and reruieve the folder named "Active_Attribution" 
2. Perform following on the excel file in the folder named "Active_Attribution"
   1. Filters out across all excel sheets
        ◦ first 6 rows
        ◦ last 4 rows
        ◦ Records where column (“Level”) is “1” and “2”
    2. Identify 3 categories of time_lens (MTD, QTD, YTD)
        ◦ MTD: an excel sheet doesn’t end with any number
        ◦ QTD: an excel sheet ends with “(1)”
        ◦ YTD: an excel sheet ends with “(2)”
    3. Derive field “time_lens” based on the 3 time_lens category identified. This means for every excel sheet associated with a specific time_lens, there is a new field with the time_lens value e.g (MTD, QTD, YTD)
    4. Derive field “Reporting Date”. This field is present in line 4 of every excel sheet as “Date”
    5. Derive field “Portfolio Code”. This field is present in line 4 of every excel sheet with as “Portfolio Short Name”
    6. Create a unique key for each record, using a combination of the newly derived “Portfolio Code", "Reporting Date", "time_lens” and existing fields  "Filter Level 1",  and "Filter Level 2"
    7. Replace all numeric values that have (-1 < x > 1000000) with 0
    8. All excel sheets needs to be merged together as a single parquet file
    9. Partition based Year=?Month=?Day?Portfolio_code=? (Parquetize)
    10. Rename the following columns: columns_dict = {
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



##  Cloud Infrasructure Layout
The Data pipeline architecture requirments for this project are listed below. All identified GCP products with be deploy with IAAC process for automation (see Terraform folder)

 Terraform Environmet Variable  (use your Parameter)

 ```
 export TF_VAR_service_account_email=""
export TF_VAR_account_key=""
export TF_VAR_data_lake_bucket=""
export TF_VAR_project=""
export TF_VAR_BQ_DATASET=""
```

Service Account Role/Permission
1. dataproc.agents.worker
2. storage.admin
3. 

# The IAAC code is used to create following GCP infrasrture

1. Datalake
2. Datawarehouse
3. Analytic Cluster (Dataproc)
4. Staging bucket


# Transformation
The job is submited to Pyspark (see dataproc_job folder code)


# PySpark Job Submission Via Gloud
To submit job run `./install.sh` on the terminal


# Dashboard View
https://datastudio.google.com/s/s_KLK9FYT8Y
