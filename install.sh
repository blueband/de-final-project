#!/bin/bash

# # copy local files to  DL
# gsutil cp  30_09_2020.zip gs://de-final-datalake_dtc-de-course-2022-339213/Input


# Launch Pyspark Job submission
gcloud dataproc jobs submit pyspark ./dataproc_job/main.py  --py-files ./dataproc_job/constants.py,./dataproc_job/file_retriever.py --jars ./dataproc_job/jars/commons-collections4-4.4.jar,./dataproc_job/jars/poi-ooxml-4.1.2.jar,./dataproc_job/jars/poi-ooxml-schemas-4.1.2.jar,./dataproc_job/jars/spark-excel_2.12-0.14.0.jar,./dataproc_job/jars/xmlbeans-3.1.0.jar --project=dtc-de-course-2022-339213 --cluster=de-dataproc-cluster --region=us-central1