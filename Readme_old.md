This page describe step to follow in order to submit a successful Pyspark Job to Google Dataproc Cluster

There are different ways to submit job to Dataproc via gcloud CLI. If your job script is in a single Python file it is different from if you have other dependencies Python files and also, if your scripts made use of jars file, the approach will be different.

In this current Pyspark Job submission, our main job code is saved in “main.py“ file, while other dependencies are in the following files “constants.py“ and “file_retriever.py“ and Maven coordinate file (jars) used by the script is available as spark-excel_2.12-0.14.0.jar (In addition, our Maven packages dependend on other Maven packages, hence all those package(commons-collections4-4.4.jar,poi-ooxml-4.1.2.jar, poi-ooxml-schemas-4.1.2.jar, xmlbeans-3.1.0.jar ) are manually download and bundled along with Pyspack job submission command)

Submit job from local Machine: (all file are available on local Machine)

In our case, the following gcloud command is run in the current project directory

 


gcloud dataproc jobs submit pyspark main.py 
--py-files constants.py,file_retriever.py 
--jars ./jars/commons-collections4-4.4.jar,./jars/poi-ooxml-4.1.2.jar,./jars/poi-ooxml-schemas-4.1.2.jar,./jars/spark-excel_2.12-0.14.0.jar,./jars/xmlbeans-3.1.0.jar 
--cluster=d2b-poc 
--region=us-central1
Pay attention to the following,

your main.py  follow  gcloud dataproc jobs submit pyspark

2. other python dependencies will follow flag name --py-files  (important, files are separated by comma, and no space)

if you script depended on Maven Coordinate package, use flag --jars  then follow by all your maven packages with their dependencies as well (important, files are separated by comma, and no space)

2. Submit job from local Machine: (all file are available on GSC buckect)

Pyspark Job (script) is copy to GSC bucket). The URI will be needed in the coming step below, from local system, in the Job working Directory, pyspark script can be copy to GCS bucket with below command 


gsutil cp job_script.py gs://remote_bucket_on_GCS
on the local machine (with already authenticated gcloud)

The following gcloud command is run

 


gcloud dataproc jobs submit pyspark gs://to_main_file_path 
--py-files gs://to_each_python_dependecnt_file_path,gs://to_each_python_dependecnt_file_path 
--jars gs://to_each_maven_dependecnt_file_path,gs://to_each_maven_dependecnt_file_path 
--cluster=d2b-poc 
--region=us-central1
where below GCS Uri point to Pyspark Job script


gs://dataproc-staging-europe-west4-633840651662-wivuy1ks/dataproc-job/main.py   
 

and  both --clusterand  --region  flag pointed to Dataproc cluster created earler and the region in which the cluster is created

 

Some Challenges on Dataproc and Resolution

WARN org.apache.spark.HeartbeatReceiver: Removing executor 1 with no recent heartbeats: 135186 ms exceeds timeout 120000 ms . The proposed solution here by implement the following

Increase value of  spark.executor.heartbeatInterval=10000000

spark.network.timeout 10000000

Otherwise, you might want to scale up your machines to accommodate the need for memory.

for the last part suggested fix

Observation: Above Issue was resolve by increasing  the underlying infrastructure from 2vCPU with 7.5GB RAM to 4vCPU with 15GB RAM

 

2. 22/03/06 03:30:20 WARN org.apache.spark.scheduler.DAGScheduler: Broadcasting large task binary with size 5.2 MiB

The above Whenever above earn triggered, parquet writing is truncated, hence full data had not be successfully written to GCS (partial data available in the specify bucket). The proposed solution here by configure the following parameter had not work

.config('spark.sql.autoBroadcastJoinThreshold', -1) 

 

Finally, The Solution lies in the Hardware provision for Dataproc

What currently work without issue

 8 vCPU

32GB RAM

500GB hard Disk

PS: The last fixed was added as possible solution on Stackoverflow here 