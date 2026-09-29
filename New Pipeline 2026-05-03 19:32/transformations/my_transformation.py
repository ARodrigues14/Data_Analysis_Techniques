import dlt
from pyspark.sql.functions import col, regexp_replace, concat, lit, lpad, when

folder_path = "/Volumes/smart_meters/bronze/tad_volume"

# --- CAMADA BRONZE ---

@dlt.table
def consumption_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(folder_path+"/hhblock_dataset")
    )

@dlt.table
def tariffs_bronze():
    # Para tabelas de referência (Lookup), podes usar o read normal (estático)
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{folder_path}/Tariffs.csv")
    )

@dlt.table
def households_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{folder_path}/informations_households.csv")
    )

@dlt.table
def weather_bronze():
    # Para tabelas de referência (Lookup), podes usar o read normal (estático)
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{folder_path}/weather_hourly_darksky.csv")
    )

@dlt.table
def holidays_bronze():
    # Para tabelas de referência (Lookup), podes usar o read normal (estático)
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{folder_path}/uk_bank_holidays_with.csv")
    )
