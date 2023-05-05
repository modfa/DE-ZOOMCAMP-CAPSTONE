
# Caspstone Project - India Hydropower Database Pipeline

- Description of the Problem :
- The International Hydropower Association (IHA) is a non-profit membership organization, working to advance sustainable hydropower development. This dataset of Indian hydropower dams available on energydata.info. We want to create a pipeline for analyzing the power production capacity, utilizations and various other factors related to the energy produced by the dams in India which is very essential for any nation to estimate their capacity and consumption during the periods.


(For more information read this dataset - https://energydata.info/dataset/india-hydropower-database)

-- Technologies and Services Used -

- Python 3.9 - Programming Language used to build this project. 
- Linux - Preferred OS Environment
- Pandas - For Various Data manipulation
- Prefect - For Workflow Orchestration 
- Apache Spark - For Batch Transformation of the data 
- Google Cloud - Using various services in cloud environment 
- GCP DataProc - For setting up the Managed Apache Spark Cluster
- Terraform - Infrastructure as code (IaC) tool for provisioning the resources in GCP 
- Google Cloud Storage (GCS) - As Datalake 
- Google BigQuery - As Data Warehouse

# Instructions on how to run the project -



Note - Preferred Environment (Linux Based - Ubuntu, Redhat, CentOS etc ), also we used the GCP Cloud for VM Instance.

Install Python >= 3.9 and conda for packages and environment management.


-- Create a virtual environment using the conda
conda create -n project 

-- Install the dependencies for running the project using -
pip install -r requirements.txt

-- Now activate the created virtual environment using 
conda activate project 

-- Now make sure you have Google SDK client CLI install in your system, a

-- Make sure you have already created a GCP account (which is free for 90 days) created.

-- Install the Terraform in your working machine.

-- Go to GCP, create a service account and assign the role for Storage Admin, Big Query Admin, DataProc Admin which will be used by Terraform for provisioning the resources in GCP. (hint- watch the DE Zoomcamp Video related to this)

-- Now following the instructions as mentioned during the course, save your credentials in a safe directory and authenticate it using google cloud cli.

-- Once successfully authenticated, go to terraform folder and run these commdans - 
 terraform init 
 terraform plan
 terraform apply
This will create the resources (Google Cloud Storage and Bigquery) in your GCP account which we will use as datalake and data warehouse respectively.

-- Now start the prefect (workflow Orchestration) server using,
prefect orion start

we can see the UI on localhost and 4200 port. If you are using the VM from cloud, make sure you are forwarding the port.

-- Now Go to GCP and create a DataProc (Apache Spark Cluster) which is a managed service, hint - can watch the DE Zoomcamp video as how to setup a dataproc cluster.

-- Now we will use this script (   ) for extracting the data from web and store/load it to DataLake (Google Cloud Storage).

 We use the workflow orchestration tool for scheduling this operation which we can setup using the prefect UI.


 Use this command which is used to apply the prefect deployment
 prefect deployment apply etl_web_to_gcs-deployment.yaml  



-- Now we will copy this script to GCS so that it can be used by the Dataproc Cluster 
gsutil cp spark_gcs_to_bq.py gs://final_project_data_lake_de-zoomcamp-project-384608/code/spark_gcs_to_bq.py

-- Once we have the DataProc cluster setup, script in GCS and our data is saved in Datalake(GCS) in parquet format, we can submit our job to spark cluster using this

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-capstone  \
    --region=asia-south2 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://final_project_data_lake_de-zoomcamp-project-384608/code/spark_gcs_to_bq.py 

Now we can see the Transformed data is shifted to Bigquery which we can use for the Visualization









