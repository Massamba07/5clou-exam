# Building a data platform for near real-time event analysis

## Project introduction

In today's data-driven world, organizations rely on **data platforms** to collect, manage, and analyze data as it arrives.
A **data platform** is an integrated solution that brings together tools and services to **ingest**, **process**, **store**, and **visualize** data.
It supports scalable data handling and fast analytics, so that decisions can be based on fresh data.

In this project, you will design and implement a **near real-time data platform** on **Microsoft Azure**.
Your platform will ingest frequently updated data from an **open data source** of your choice, process it, and display insights in an interactive dashboard.

You will work with three building blocks:

| Layer | Azure service | Local alternative |
| --- | --- | --- |
| Ingestion & orchestration | **Azure Data Factory** | **Apache Airflow** |
| Storage | **Azure Data Lake Storage Gen2** | Local files (JSON, CSV) or SQLite |
| Processing | **Azure Synapse Analytics** (Spark pool + serverless SQL pool) | **Python / Pandas** |
| Visualization | **Power BI Desktop** | **Jupyter Notebooks** or **Tableau Public** |

**Python** is the language of choice for data transformation, processing, and automation in both paths.

> [!NOTE]
> This project uses the **Azure PaaS services** (Data Factory and Synapse Analytics created from the Azure portal).
> It does **not** use Microsoft Fabric.

---

## Objectives

- Build a data platform that can **ingest**, **process**, **store**, and **visualize** frequently updated data.
- Choose a relevant **open data source** and turn it into useful, up-to-date insights.
- Define and **measure** the freshness of your data (see [What "near real-time" means here](#what-near-real-time-means-here)).
- Use **Python** for processing and automation.
- Keep your cloud costs **under control** and stay within free offers.

---

## Practical information

| Item | Value |
| --- | --- |
| Group size | `<2 to 3 students>` |
| Deadline (repository + video) | `<date>` |
| Defense | `<date>` — 15 min demo + 10 min individual Q&A |
| Submission | Link to your GitHub repository on Moodle |

---

## Prelude

### Getting free Azure credits

Use **Azure for Students**: it gives a yearly credit with **no credit card required**, as long as you can sign in with your school account.

- Sign up at [Azure for Students](https://azure.microsoft.com/free/students/).
- The [GitHub Student Developer Pack](https://education.github.com/pack) also links to this offer and to other free tools for students.

All the tools used in this project are free, or are paid for by this credit:

| Tool | Cost |
| --- | --- |
| Power BI Desktop | Free (Windows) |
| Tableau Public, Jupyter, Pandas, Airflow | Free |
| Data Lake Storage, Data Factory, Synapse serverless SQL | A few cents to a few euros for this project |
| Synapse Spark pool | **Main cost** — billed per minute while the pool is running |

### Cost rules (mandatory on the Azure path)

1. Create **one resource group** for the whole project, and tag every resource (`project=5clou-exam`).
2. Set a **budget alert** on your subscription (e.g. 20 €) in *Cost Management*.
3. **Never** create a **dedicated SQL pool**: it costs money every hour, even when idle. Use the **serverless SQL pool** (built-in).
4. Create the **smallest Spark pool** (node size *Small*, 3 nodes) with **auto-pause after 15 minutes**.
5. Do not schedule the Spark processing more often than you need (see the latency target below).
6. After the defense, **delete the resource group**.

### Azure services and local alternatives

If you cannot use Azure (no credit, subscription quota, account issue), you may follow the **local path** at each step.
The concepts are the same: an orchestrated ingestion pipeline, a processing layer, a storage layer, and a dashboard.

> [!IMPORTANT]
> The local path is accepted **only if you explain in your report why you could not use Azure**.
> Both paths are graded on the same grid; the Terraform bonus is only available on the Azure path.

### What "near real-time" means here

Neither path is true streaming: data is collected and processed on a **schedule**.
Your platform must therefore have an explicit **latency target**, chosen and justified by your group:

> *Maximum time between the moment the data is published by the source and the moment it appears in the dashboard.*

Typical values: **15 minutes to 1 hour**. During the demo, you must **show** one new data point travelling from the source to the dashboard.

---

## Steps to complete the project

### Step 1: Selecting an open data source

Choose an **open data source** that is **updated frequently** (at least every hour) and that you can call **for free**.

| Source | Update frequency | Key needed | Notes |
| --- | --- | --- | --- |
| [Vélib' availability – Paris Open Data](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/) | About every minute | No | Bike and dock availability per station |
| [Open-Meteo](https://open-meteo.com/) | Hourly / every 15 min | No | Weather, air quality |
| [RTE éCO2mix – ODRÉ](https://odre.opendatasoft.com/) | Every 15 min | No | French electricity production and consumption |
| [OpenWeatherMap](https://openweathermap.org/api) | Every 10 min | Yes (free tier) | Daily call quota |
| [Alpha Vantage](https://www.alphavantage.co/) | Intraday | Yes (free tier) | Very small daily quota: plan your schedule accordingly |

> [!WARNING]
> Static datasets (for example the Paris 2024 Olympic Games schedule, which is now over) are **not** suitable: nothing new would reach your dashboard.

Check the source's **licence**, **rate limits**, and **data format** (JSON, CSV) before you start.

The examples below use **Open-Meteo** (current weather in Paris):

```text
https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&current=temperature_2m,relative_humidity_2m,wind_speed_10m
```

Sample response (simplified):

```json
{
  "latitude": 48.86,
  "longitude": 2.35,
  "current": {
    "time": "2026-10-19T09:00",
    "temperature_2m": 12.4,
    "relative_humidity_2m": 81,
    "wind_speed_10m": 9.7
  }
}
```

**Output**: a documented data source (URL, licence, update frequency, rate limits, fields you will use).

---

### Step 2: Setting up the data ingestion pipeline

> [!IMPORTANT]
> **Azure path** — Azure Data Factory + Azure Data Lake Storage Gen2.

1. **Create the storage**:

   - In the **Azure portal**, create a **Storage account** (*Standard*, *LRS*) and enable **Hierarchical namespace** (this makes it a Data Lake Storage Gen2 account).
   - Create a **container** named `data`. You will use two folders: `raw/` (data as received) and `curated/` (cleaned data).

2. **Create an Azure Data Factory**:

   - Search for **Data factories** in the portal and create a new instance in the same resource group and region.
   - Open **Azure Data Factory Studio**.

3. **Ingest the data**:

   - Create a **linked service** of type **REST** (or **HTTP**) pointing to your API, and a linked service to your **Data Lake Storage Gen2** account.
   - Create a pipeline with a **Copy data** activity:
     - **Source**: the REST dataset.
     - **Sink**: a **JSON** dataset in `data/raw/<source>/`.
   - Give each file a **unique, timestamped name**, so that every run is kept (this is your history). In the sink dataset, add a `fileName` parameter and set it in the Copy activity with:

     ```text
     @concat('weather_', formatDateTime(utcNow(), 'yyyyMMddTHHmmss'), '.json')
     ```

4. **Schedule the pipeline**:

   - Add a **schedule trigger** (e.g. every 15 minutes or every hour) consistent with your latency target and with the API's rate limits.
   - Check the runs in the **Monitor** tab.

5. **Secrets**: if your API needs a key, store it in **Azure Key Vault** and reference it from the linked service. Never write it in the pipeline JSON or in your repository.

**Output**: a scheduled pipeline that lands a new timestamped raw file in the Data Lake at each run.

> [!IMPORTANT]
> **Local path** — Apache Airflow.

1. **Install Airflow locally**:

   - Recommended: use the official [Docker Compose setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
   - Add a data volume to the `volumes` section of the `x-airflow-common` block in `docker-compose.yaml`, so that files written by your tasks are visible on your machine:

     ```yaml
     - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
     ```

2. **Write a DAG** in the `dags/` folder. The example below targets **Airflow 3**:

   ```python
   import json
   from datetime import datetime, timedelta, timezone
   from pathlib import Path

   import requests
   from airflow.sdk import dag, task

   API_URL = (
       "https://api.open-meteo.com/v1/forecast"
       "?latitude=48.8566&longitude=2.3522"
       "&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
   )
   RAW_DIR = Path("/opt/airflow/data/raw/weather")


   @dag(
       dag_id="weather_ingestion",
       schedule="@hourly",
       start_date=datetime(2026, 1, 1),
       catchup=False,  # do not replay every hour since start_date
       default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
       tags=["5clou"],
   )
   def weather_ingestion():
       @task
       def fetch_data() -> str:
           response = requests.get(API_URL, timeout=30)
           response.raise_for_status()

           RAW_DIR.mkdir(parents=True, exist_ok=True)
           timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
           output_file = RAW_DIR / f"weather_{timestamp}.json"
           output_file.write_text(json.dumps(response.json()))
           return str(output_file)

       fetch_data()


   weather_ingestion()
   ```

   > On **Airflow 2.x**, replace `from airflow.sdk import dag, task` with `from airflow.decorators import dag, task`.

3. **Run it**: enable the DAG in the Airflow UI and check that a new file appears in `data/raw/weather/` at each run.

4. **Secrets**: put API keys in an **Airflow Connection** or **Variable**, or in a `.env` file listed in `.gitignore`.

**Output**: a scheduled Airflow DAG that stores a new timestamped raw file at each run.

---

### Step 3: Data processing with Synapse Analytics (Python)

> [!IMPORTANT]
> **Azure path** — Synapse Spark pool (PySpark) + serverless SQL pool.

1. **Create a Synapse workspace**:

   - In the portal, create an **Azure Synapse Analytics** workspace and select your existing Data Lake Storage Gen2 account and `data` container.
   - Give yourself (and every group member) the **Storage Blob Data Contributor** role on the storage account. Without it, notebooks and SQL queries cannot read the files.

2. **Create a Spark pool**:

   - Node size **Small**, **3 nodes**, autoscale **off**, **auto-pause after 15 minutes** (see the cost rules).
   - If creation fails with a **quota** error, request a quota increase or switch to the local path for processing and explain it in your report.

3. **Process the data with PySpark** in a Synapse notebook (the `spark` session already exists):

   ```python
   from pyspark.sql import functions as F

   storage_account = "<storage_account>"
   base = f"abfss://data@{storage_account}.dfs.core.windows.net"

   # Read every raw JSON file (one JSON object per file)
   raw = spark.read.option("multiLine", True).json(f"{base}/raw/weather/*.json")

   # Flatten, type, clean
   weather = (
       raw.select(
           F.to_timestamp("current.time").alias("observed_at"),
           F.col("current.temperature_2m").cast("double").alias("temperature_c"),
           F.col("current.relative_humidity_2m").cast("double").alias("humidity_pct"),
           F.col("current.wind_speed_10m").cast("double").alias("wind_kmh"),
       )
       .dropna(subset=["observed_at"])
       .dropDuplicates(["observed_at"])
       .withColumn("processed_at", F.current_timestamp())
   )

   weather.write.mode("overwrite").parquet(f"{base}/curated/weather/")
   weather.orderBy(F.desc("observed_at")).show(5)
   ```

4. **Expose the data with the serverless SQL pool** (built-in, billed only on the data each query reads):

   ```sql
   -- Run once, connected to the "Built-in" pool
   CREATE DATABASE weather_db;
   GO
   USE weather_db;
   GO
   CREATE VIEW dbo.weather AS
   SELECT *
   FROM OPENROWSET(
       BULK 'https://<storage_account>.dfs.core.windows.net/data/curated/weather/*.parquet',
       FORMAT = 'PARQUET'
   ) AS rows;
   ```

5. **Orchestrate the processing**: run the notebook automatically after ingestion, with a **Synapse pipeline** (*Notebook* activity) or from your Data Factory pipeline. Choose the frequency according to your latency target: each run starts the Spark pool and consumes credit.

**Output**: a scheduled PySpark job that turns raw files into clean Parquet files, exposed as a SQL view ready for Power BI.

> [!IMPORTANT]
> **Local path** — Python / Pandas.

1. **Install the libraries**: `pip install pandas`.

2. **Write the transformation**, ideally as a **second task of your Airflow DAG**, so that processing runs right after ingestion:

   ```python
   import json
   import sqlite3
   from pathlib import Path

   import pandas as pd

   RAW_DIR = Path("data/raw/weather")
   CURATED_DIR = Path("data/curated")

   rows = []
   for file in sorted(RAW_DIR.glob("*.json")):
       current = json.loads(file.read_text())["current"]
       rows.append(
           {
               "observed_at": current["time"],
               "temperature_c": current["temperature_2m"],
               "humidity_pct": current["relative_humidity_2m"],
               "wind_kmh": current["wind_speed_10m"],
           }
       )

   df = pd.DataFrame(rows)
   df["observed_at"] = pd.to_datetime(df["observed_at"])
   df = (
       df.dropna(subset=["observed_at"])
       .drop_duplicates(subset="observed_at")
       .sort_values("observed_at")
   )

   CURATED_DIR.mkdir(parents=True, exist_ok=True)
   df.to_csv(CURATED_DIR / "weather.csv", index=False)

   with sqlite3.connect("data/weather.db") as conn:
       df.to_sql("weather", conn, if_exists="replace", index=False)
   ```

**Output**: an automated Pandas processing step that produces a clean dataset (CSV and/or SQLite table).

---

### Step 4: Visualizing data with Power BI

> [!IMPORTANT]
> **Azure path** — Power BI Desktop (free).

1. **Connect Power BI to Synapse**:

   - In **Power BI Desktop**: *Get data* → **Azure Synapse Analytics SQL**.
   - Server: the **serverless SQL endpoint** of your workspace (`<workspace>-ondemand.sql.azuresynapse.net`), database `weather_db`.
   - Choose **DirectQuery**: every visual queries the latest data, so no dataset refresh is needed.

2. **Build interactive reports**: KPIs (last value, trend), time series, maps or rankings, filters and slicers. Show the **timestamp of the latest data point** on the report: it is how you prove freshness.

3. **Automatic refresh**: in DirectQuery mode, enable **automatic page refresh** on the report page (*Format* → *Page refresh*) with an interval consistent with your latency target.

> [!NOTE]
> In **Import** mode, refreshes are scheduled in the Power BI service and limited to a few per day on non-premium workspaces: this is not enough to demonstrate fresh data.
> Publishing and sharing reports in the Power BI service requires a paid licence: you do **not** need it. The demo is done in **Power BI Desktop**.

**Output**: an interactive Power BI report that updates automatically and shows the freshness of its data.

> [!IMPORTANT]
> **Local path** — Jupyter Notebooks or Tableau Public.

1. **Jupyter Notebooks**:

   - Install: `pip install jupyter plotly`.
   - Load the curated data and build interactive charts:

   ```python
   import pandas as pd
   import plotly.express as px

   df = pd.read_csv("data/curated/weather.csv", parse_dates=["observed_at"])

   print("Latest data point:", df["observed_at"].max())

   fig = px.line(df, x="observed_at", y="temperature_c", title="Temperature in Paris")
   fig.show()
   ```

   - Re-running the notebook shows the latest data produced by your pipeline.

2. **Tableau Public**:

   - Connect Tableau Public to `data/curated/weather.csv` and build your dashboard.
   - Tableau Public does not refresh local files automatically: refresh the extract and republish to show new data during the demo.

**Output**: interactive visualizations that display the latest processed data and its timestamp.

---

## Deliverables

Your GitHub repository must contain:

1. **`README.md`** with:
   - the chosen data source and why it fits (Step 1);
   - an **architecture diagram** (source → ingestion → storage → processing → dashboard);
   - your **latency target** and how you measure it;
   - setup instructions that let someone else reproduce your platform;
   - for the local path: why Azure could not be used.
2. **Code**: Data Factory pipeline JSON exports (or Airflow DAGs), notebooks / Python scripts, SQL scripts, and Terraform files if you do the bonus.
3. **Screenshots**: pipeline runs (Monitor tab or Airflow UI), files in storage, and the dashboard.
4. **A backup video** (3 to 5 minutes) of the full demo, in case the live demo fails during the defense.

**Rules**:

- **No secrets** in the repository (API keys, passwords, connection strings, SAS tokens). Use Key Vault, Airflow Connections, or a `.env` file listed in `.gitignore`. Secrets found in the commit history cost points.
- Every group member must commit to the repository.

---

## Grading (20 points)

| Criterion | Points | Full marks when… |
| --- | :---: | --- |
| Ingestion & orchestration | 4 | Scheduled pipeline, history kept (timestamped files), retries / error handling, runs visible in monitoring |
| Processing & data quality (Python / PySpark) | 4 | Typed and cleaned data, duplicates and missing values handled, processing automated after ingestion |
| Dashboard | 3 | Relevant KPIs and visuals, automatic refresh, freshness shown and demonstrated live |
| Architecture, justification & reliability | 2 | Clear diagram, latency target justified and met, service choices and costs explained |
| Code, repository & documentation | 2 | Reproducible setup, readable code, no secrets, contributions from every member |
| Presentation & demo | 1 | Clear, on time, new data shown travelling from source to dashboard |
| Individual Q&A | 4 | Each member can explain any part of the platform and the reasons behind the choices |
| **Bonus: Terraform** (Azure path only) | **+2** | Infrastructure deployed from code, see below |

---

## Bonus: Infrastructure as Code (IaC) with Terraform

Automate the deployment of your Azure resources with **Terraform** and the [**AzureRM provider**](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs).

1. **Set up Terraform**: install the [Terraform CLI](https://developer.hashicorp.com/terraform/install) and sign in with `az login`.

2. **Write the configuration**: storage account (Data Lake Gen2), container, Data Factory, Synapse workspace, Spark pool, firewall rule, and role assignments.

3. **Deploy**: `terraform init`, `terraform plan`, `terraform apply`. Destroy everything after the defense with `terraform destroy`.

> [!NOTE]
> Power BI reports are **not** deployed with Terraform: keep the `.pbix` file in your repository.

**Example configuration** (`main.tf`):

```hcl
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

variable "subscription_id" {
  type = string
}

variable "sql_admin_password" {
  type      = string
  sensitive = true # pass it with TF_VAR_sql_admin_password, never commit it
}

variable "location" {
  type    = string
  default = "francecentral"
}

data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  upper   = false
  special = false
}

locals {
  tags = { project = "5clou-exam" }
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-5clou-exam"
  location = var.location
  tags     = local.tags
}

# Data Lake Storage Gen2
resource "azurerm_storage_account" "datalake" {
  name                     = "st5clou${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  tags                     = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "data" {
  name               = "data"
  storage_account_id = azurerm_storage_account.datalake.id
}

# Data Factory
resource "azurerm_data_factory" "adf" {
  name                = "adf-5clou-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

# Synapse workspace (the serverless SQL pool is included)
resource "azurerm_synapse_workspace" "synapse" {
  name                                 = "syn-5clou-${random_string.suffix.result}"
  resource_group_name                  = azurerm_resource_group.rg.name
  location                             = azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.data.id
  sql_administrator_login              = "sqladminuser"
  sql_administrator_login_password     = var.sql_admin_password
  tags                                 = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_synapse_firewall_rule" "allow_azure_services" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

# Smallest Spark pool, paused after 15 minutes of inactivity
resource "azurerm_synapse_spark_pool" "spark" {
  name                 = "sparksmall"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Small"
  node_count           = 3
  spark_version        = "3.4" # check the versions currently supported by Synapse

  auto_pause {
    delay_in_minutes = 15
  }

  tags = local.tags
}

# Access to the Data Lake for Data Factory, Synapse and yourself
resource "azurerm_role_assignment" "adf_storage" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_role_assignment" "synapse_storage" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.synapse.identity[0].principal_id
}

resource "azurerm_role_assignment" "me_storage" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}
```

To go further: add the Data Factory linked services, datasets, pipeline and trigger (`azurerm_data_factory_*` resources), a budget alert (`azurerm_consumption_budget_resource_group`), and a Key Vault for your API key.
