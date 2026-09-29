# Data Analysis Techniques

A Databricks lakehouse project analysing UK smart-meter electricity data (the London "Smart Meters in London" dataset). It builds a medallion pipeline (Bronze → Silver → Gold) in Unity Catalog, exposes KPI views for BI tools such as Tableau, and trains simple models to forecast hourly energy consumption.

All tables live in the `smart_meters` catalog (`bronze`, `silver` and `gold` schemas). Raw CSVs are read from the Unity Catalog volume `/Volumes/smart_meters/bronze/tad_volume`.

## Data sources

| File / folder | Content |
|---|---|
| `hhblock_dataset/*.csv` | Half-hourly consumption blocks per household (`LCLid`, `day`, `hh_0`…`hh_47`) |
| `informations_households.csv` | Household metadata: Acorn group, `stdorToU` tariff type |
| `Tariffs.csv` | Dynamic Time-of-Use tariff level (Low / Normal / High) per timestamp |
| Weather and bank-holiday files | Hourly weather and UK bank holidays |

## Pipeline

| Layer | Notebook | What it does |
|---|---|---|
| Bronze | `Bronze.ipynb` | Ingests the raw CSVs into `smart_meters.bronze.*` Delta tables and seeds the `dim_tariffs` price table (Std £0.14, ToU Low £0.04 / Normal £0.12 / High £0.67 per kWh) |
| Silver | `Bronze To Silver.ipynb` | Cleans and enriches the data: household dimension (filtered by Acorn group), holidays and holiday eves, tariffs, weather with thermal-stress levels and temperature bins, half-hourly → hourly consumption unpivot, `fact_consumption`, and `dim_time` |
| Gold | `Silver to Gold.ipynb`, `GOLD.ipynb` | Builds the star schema: `dim_household`, `dim_weather`, `dim_tariffs` (SCD Type 2), `dim_time` and `fact_consumption` (energy and money spent), with incremental `MERGE`/`INSERT` and duplicate checks |

### Delta Live Tables / Lakeflow pipelines

The `New Pipeline <date>/transformations/` folders hold declarative pipeline experiments that ingest the Bronze tables (`consumption_bronze`, `tariffs_bronze`, `households_bronze`, `weather_bronze`) using Auto Loader. The later `2026-05-05` version uses the newer `pyspark.pipelines` API (`dp`) instead of `dlt`.

## Analytics and KPI views

The `ZQuery *.dbquery.ipynb` notebooks create Gold views for dashboards:

| Notebook | View / purpose |
|---|---|
| `ZQuery Custo e Consumo Por Acorn` | `vw_kpi_demographics` – cost and consumption by Acorn group and tariff type |
| `ZQuery Impacto do Tempo` | `vw_weather_impact` – energy by weather, temperature bin and holiday type |
| `ZQuery Night Owls` | `vw_night_owls` – households that use over 40% of their daily energy between 22:00 and 07:00 |
| `ZQuery Perfil Horário Por Estação do Ano` | `vw_load_curve_seasonal` – average hourly load curve per season |
| `ZQuery Top 5% Consumers` | `vw_top_5_percent_consumers` – top 5% of consumers within each Acorn group |
| `ZToU vs Standard Tariffs` | `vw_tou_vs_std_savings` – actual ToU cost versus simulated Standard-tariff cost |
| `ZQuery Verificação Real / Artificial Mudança de Tarifa` | Checks that households switching tariff type are handled correctly, using real data and injected test rows |
| `Table drops` | Housekeeping (commented-out `DROP TABLE` statements) |

## Forecasting

- **`Previsao1.ipynb`** – SARIMAX time-series model of total hourly energy with temperature bin as an exogenous variable; reports MAE, RMSE and MAPE.
- **`Previsao2.ipynb`** – Spark ML pipeline (one-hot encoding + linear regression, with a random-forest variant) predicting energy per household from hour, day and temperature; reports R², RMSE and MAE and saves results to `smart_meters.gold.energy_predictions` for Tableau.

## Requirements

- A Databricks workspace with Unity Catalog and the `smart_meters` catalog and `tad_volume` volume
- Python libraries: `pyspark`, `pandas`, `numpy`, `statsmodels`, `scikit-learn`, `matplotlib`

## Running it

Import the notebooks into Databricks and run them in this order:

1. `Bronze.ipynb`
2. `Bronze To Silver.ipynb`
3. `Silver to Gold.ipynb` (then `GOLD.ipynb` for incremental loads)
4. `ZQuery *` notebooks to create the KPI views
5. `Previsao1.ipynb` / `Previsao2.ipynb` for forecasting

Some notebook comments and markdown are written in Portuguese.
