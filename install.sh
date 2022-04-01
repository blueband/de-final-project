# copy local files to  DL
gsutil cp  30_09_2020.zip gs://de-final-datalake_dtc-de-course-2022-339213/Input


# Launch Pyspark Job submission
` 
gcloud dataproc jobs submit pyspark main.py  --py-files constants.py,file_retriever.py --jars ./jars/commons-collections4-4.4.jar,./jars/poi-ooxml-4.1.2.jar,./jars/poi-ooxml-schemas-4.1.2.jar,./jars/spark-excel_2.12-0.14.0.jar,./jars/xmlbeans-3.1.0.jar --project=dtc-de-course-2022-339213 --cluster=de-dataproc-cluster --region=us-central1

`