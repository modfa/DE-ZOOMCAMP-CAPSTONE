import pyspark
from pyspark.sql import SparkSession
import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from pyspark.sql import types
from pathlib import Path

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()



@task(retries=3)
def fetch(dataset_url: str, output_path:str) -> pd.DataFrame:
    """Download hydropwer data from web into local path"""
    os.system(f"wget {dataset_url} -O {output_path} ")

@task()
def read_dataframe(path_to_read_from: str):
    # Spark schema to retain
    schema = types.StructType([
    types.StructField('Hydropower Station Name', types.StringType(), True),
    types.StructField('Database Status', types.StringType(), True), 
    types.StructField('ID', types.StringType(), True),
    types.StructField('Country', types.StringType(), True),
    types.StructField('ISO', types.StringType(), True),
    types.StructField('Reservoir Name', types.StringType(), True),
    types.StructField('Lake Name', types.StringType(), True),
    types.StructField('Purpose', types.StringType(), True),
    types.StructField('Admin_unit', types.StringType(), True),
    types.StructField('Owner ', types.StringType(), True),
    types.StructField('Near_city', types.StringType(), True),
    types.StructField('District ', types.StringType(), True),
    types.StructField('River', types.StringType(), True),
    types.StructField('Main_basin', types.StringType(), True),
    types.StructField('Catch_Area ', types.StringType(), True),
    types.StructField('Op_Status', types.StringType(), True),
    types.StructField('Comissioned', types.StringType(), True),
    types.StructField('DamComplete', types.DoubleType(), True),
    types.StructField('No_Units', types.StringType(), True),
    types.StructField('Dam_hgt ', types.DoubleType(), True),
    types.StructField('WaterHeadhgt( max)', types.DoubleType(), True),
    types.StructField('WaterHeadHgt (min)', types.DoubleType(), True),
    types.StructField('Res_capacityMm3', types.DoubleType(), True),
    types.StructField('Res_area km2', types.StringType(), True),
    types.StructField('HPP_elec_cap', types.StringType(), True),
    types.StructField('Transm_exist', types.StringType(), True),
    types.StructField('Transm_length', types.DoubleType(), True), 
    types.StructField('Transm_planned', types.DoubleType(), True), 
    types.StructField('Hydropower_Type', types.StringType(), True), 
    types.StructField('Ann_firm_gen', types.StringType(), True), 
    types.StructField('Ann_tot_gen', types.StringType(), True), 
    types.StructField('Lat (reservoir/dam)', types.DoubleType(), True), 
    types.StructField('Long (reservoir/dam)', types.DoubleType(), True),
    types.StructField('Lat (hydropower station 1)', types.DoubleType(), True),
    types.StructField('Long (hydropower station 1)', types.DoubleType(), True),
    types.StructField('Lat (hydropower station 2)', types.DoubleType(), True),
    types.StructField('Long (hydropower station 2)', types.DoubleType(), True),
    types.StructField('Display', types.StringType(), True),
    types.StructField('Reference 1', types.StringType(), True),
    types.StructField('Reference 2', types.StringType(), True),
    types.StructField('Reference 3', types.StringType(), True), 
    types.StructField('Reference 4', types.StringType(), True), 
    types.StructField('Reference 5', types.StringType(), True), 
    types.StructField('Reference 6', types.StringType(), True), 
    types.StructField('Reference 7', types.StringType(), True), 
    types.StructField('Reference 8', types.StringType(), True), 
    types.StructField('Reference 9', types.StringType(), True), 
    types.StructField('Reference 10', types.StringType(), True), 
    types.StructField('Comment', types.StringType(), True), 
    types.StructField('Column1', types.StringType(), True)])

    df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(path_to_read_from)
    return df


@task()
def write_local(df, parquet_path):
    df = df.repartition(24)
    path = Path(f"/home/modf/capstone_project/code/notebooks/data/pq/")
    df.write.parquet(parquet_path, mode='overwrite')
    return path 


@task()
def read_parquet(parquet_path):
    df = spark.read.parquet(parquet_path)
    print(df.printSchema())

@task()
def write_gcs(folder) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("capstone-gcs-project")
    gcs_block.upload_from_folder(from_folder=folder, to_folder=folder)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    
    output_path = "/home/modf/capstone_project/code/notebooks/data/hydrodata.csv"
    parquet_path = "/home/modf/capstone_project/code/notebooks/data/pq/"
    dataset_url = "https://energydata.info/dataset/01f6fdfc-ee24-4f3f-9580-389eb39ef959/resource/71e057a7-585b-4cc7-925c-b62061aff404/download/iha_hydrodatabase_240919_csv.csv"

    fetch(dataset_url, output_path)
    df = read_dataframe(output_path)
    path = write_local(df,parquet_path)
    # read_parquet(path)
    folder = "/home/modf/capstone_project/code/notebooks/data/pq/"
    write_gcs(folder)


if __name__ == "__main__":
    etl_web_to_gcs()
