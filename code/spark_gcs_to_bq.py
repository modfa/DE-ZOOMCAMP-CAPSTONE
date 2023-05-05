import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
import pandas as pd

# from prefect import flow, task
# from prefect_gcp.cloud_storage import GcsBucket
# from prefect_gcp import GcpCredentials


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-south2-824846179269-mago6uy3')

# @task(retries=3)
# def extract_from_gcs() -> Path:
#     """Download trip data from GCS"""
#     gcs_path = f"/home/modf/capstone_project/code/notebooks/data/pq/*.parquet"
#     gcs_block = GcsBucket.load("capstone-gcs-project")
#     gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
#     return Path(f"../data/{gcs_path}")

# @task()
def transform(parquet_path):
    """Data cleaning example"""
    df = spark.read.parquet(parquet_path)
    #renaming the columns name so as to compliant with the bigquey naming conventions standard
    df = df \
    .withColumnRenamed('WaterHeadhgt( max)','WaterHeadhgt_max') \
    .withColumnRenamed('WaterHeadHgt (min)','WaterHeadHgt_min') \
    .withColumnRenamed('Lat (reservoir/dam)','Lat_reservoir_dam') \
    .withColumnRenamed('Long (reservoir/dam)','Long_reservoir_dam') \
    .withColumnRenamed('Lat (hydropower station 1)','Lat_hydropower_station_1') \
    .withColumnRenamed('Long (hydropower station 1)','Long_hydropower_station_1') \
    .withColumnRenamed('Lat (hydropower station 2)','Lat_hydropower_station_2') \
    .withColumnRenamed('Long (hydropower station 2)','Long_hydropower_station_2')
    # df.rename(columns=columns, inplace =True)
    # df.drop(159,axis=0, inplace=True)
    # df['HPP_elec_cap']= pd.to_numeric(df['HPP_elec_cap'])
    # print(f"pre: missing count: {df['WaterHeadHgt_min'].isna().sum()}")
    # print(f"pre: missing count: {df['WaterHeadhgt_max'].isna().sum()}")
    # df['WaterHeadHgt_min'] = df['WaterHeadHgt_min'].fillna(df['WaterHeadHgt_min'].median())
    # df['WaterHeadhgt_max'] = df['WaterHeadhgt_max'].fillna(df['WaterHeadhgt_max'].median())
    # print(f"post: missing  count: {df['WaterHeadHgt_min'].isna().sum()}")
    # print(f"post: missing  count: {df['WaterHeadhgt_max'].isna().sum()}")
    return df

# @task()
# def write_gcs(folder) -> None:
#     """Upload local parquet file to GCS"""
#     gcs_block = GcsBucket.load("capstone-gcs-project")
#     gcs_block.upload_from_folder(from_folder=folder, to_folder=folder)
#     return


# parquet_path = "/home/modf/capstone_project/code/notebooks/data/pq/"
# df = spark.read.parquet(parquet_path)

# print(df.groupBy('Hydropower_Type').count().show())
# print(df.printSchema())

# @task()
def write_bq(df):
    df.write.format('bigquery') \
    .option('table', 'de-zoomcamp-project-384608.hydro.hydrogen') \
    .save()

# @flow()
def gcs_to_bq():
    # extract_from_gcs()
    df = transform("gs://final_project_data_lake_de-zoomcamp-project-384608/pq")

    # final_project_data_lake_de-zoomcamp-project-384608//home/modf/capstone_project/code/notebooks/data/pq
    
    df.registerTempTable('hydropowerdata')

    res = spark.sql("""
    SELECT
    Hydropower_Type, Purpose,River,Admin_unit,
    SUM(Ann_tot_gen) as gen,
    COUNT(River) as River_count,
    COUNT(Purpose) as Purpose_count,
    COUNT(Hydropower_Type) as Hydropower_count
    FROM
    hydropowerdata
    GROUP BY 
    Hydropower_Type, Purpose, River, Admin_unit
    """)
    reports_path = '/home/modf/capstone_project/code/notebooks/data/reports/'
    # df.coalesce(1).write.parquet(reports_path, mode='overwrite')
    # write_gcs(reports_path)
    write_bq(res)
   



if __name__ == "__main__":
    gcs_to_bq()