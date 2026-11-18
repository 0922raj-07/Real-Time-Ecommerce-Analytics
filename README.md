\# 🛒 E-Commerce Shopper Behaviour: Medallion Data Pipeline (Databricks + PySpark + Delta Lake)



An end-to-end data engineering project that ingests \*\*\~1 million rows\*\* of raw e-commerce shopper data, cleans it, and produces analytics-ready tables using the \*\*Medallion Architecture\*\* (Bronze → Silver → Gold) on \*\*Databricks\*\*, \*\*Apache Spark (PySpark)\*\*, \*\*Structured Streaming\*\*, and \*\*Delta Lake\*\*.



\---



\## 📌 Table of Contents

1\. \[Project Overview](#-project-overview)

2\. \[Architecture](#-architecture)

3\. \[Tech Stack](#-tech-stack)

4\. \[Repository Structure](#-repository-structure)

5\. \[Dataset](#-dataset)

6\. \[Pipeline Walkthrough](#-pipeline-walkthrough)

7\. \[Data Quality Issues Handled](#-data-quality-issues-handled)

8\. \[Gold Layer Tables](#-gold-layer-tables)

9\. \[Key Results](#-key-results)

10\. \[How to Run](#-how-to-run)

11\. \[Design Decisions and Known Limitations](#-design-decisions-and-known-limitations)

12\. \[Future Improvements](#-future-improvements)



\---



\## 🎯 Project Overview



Raw behavioural data is rarely analysis-ready. This project simulates a realistic ingestion scenario:



\- A \*\*producer\*\* drops CSV files into a landing zone in small batches (like an upstream system would).

\- A \*\*consumer\*\* picks those files up with Spark Structured Streaming and stores them as a Delta table (\*\*Bronze\*\*).

\- A \*\*cleaning job\*\* removes duplicates, fixes typos, handles outliers and invalid values, and standardises categories (\*\*Silver\*\*).

\- An \*\*aggregation job\*\* builds business-friendly summary tables by country, category, customer segment, device/payment method and demographics (\*\*Gold\*\*).



The dataset covers \*\*60 attributes per shopper\*\*: demographics, purchasing behaviour, app/session engagement, lifestyle and wellbeing indicators.



\---



\## 🏗 Architecture



```mermaid

flowchart LR

&#x20;   A\[Raw CSV<br/>1,020,000 rows] -->|01 Producer<br/>15k-row batches| B\[Landing Zone<br/>68 CSV files]

&#x20;   B -->|02 Consumer<br/>Structured Streaming<br/>availableNow| C\[(BRONZE<br/>Delta, raw)]

&#x20;   C -->|03 Cleaning| D\[(SILVER<br/>Delta, cleaned<br/>1,006,176 rows)]

&#x20;   D -->|04 Aggregation| E\[(GOLD<br/>5 Delta summary tables)]

```



| Layer | Purpose | Format | Rows |

|-------|---------|--------|------|

| \*\*Raw / Landing\*\* | Source CSV and simulated incoming batch files | CSV | 1,020,000 |

| \*\*Bronze\*\* | Exact copy of ingested data with a fixed schema, no cleaning | Delta | 1,020,000 |

| \*\*Silver\*\* | Deduplicated, standardised, validated data | Delta | 1,006,176 |

| \*\*Gold\*\* | Aggregated, business-ready tables | Delta | 10 / 90 / 90 / 280 / 300 |



\---



\## 🧰 Tech Stack



\- \*\*Platform:\*\* Databricks (Unity Catalog Volumes)

\- \*\*Language:\*\* Python 3 / PySpark

\- \*\*Processing:\*\* Spark Structured Streaming (`readStream`, `foreachBatch`, `trigger(availableNow=True)`)

\- \*\*Storage:\*\* Delta Lake

\- \*\*Utilities:\*\* pandas (batch splitting in the producer)



\---



\## 📂 Repository Structure



```

.

├── 01\_bronze\_producer.ipynb      # Splits raw CSV into batch files in the landing zone

├── 02\_bronze\_consumer.ipynb      # Streams landing files into the Bronze Delta table

├── 03\_silver\_cleaning.ipynb      # Cleans and standardises Bronze -> Silver

├── 04\_gold\_layer.ipynb           # Builds aggregated Gold tables (with country dimension)

└── README.md

```



\### Storage layout (Databricks Volume)



```

/Volumes/workspace/default/ecommerce\_data/

├── raw/            e\_commerce\_shopper\_behaviour\_RAW\_bronze.csv

├── landing/        batch\_0000.csv ... batch\_0067.csv

├── checkpoints/

│   └── bronze/     Structured Streaming checkpoint

├── bronze/         Delta table (raw ingested data)

├── silver/         Delta table (cleaned data)

└── gold/

&#x20;   ├── country\_summary/

&#x20;   ├── category\_summary/

&#x20;   ├── customer\_segment\_summary/

&#x20;   ├── device\_payment\_summary/

&#x20;   └── age\_gender\_summary/

```



\---



\## 📊 Dataset



\*\*Source file:\*\* `e\_commerce\_shopper\_behaviour\_RAW\_bronze.csv` (1,020,000 rows × 60 columns)



| Group | Example columns |

|-------|-----------------|

| \*\*Demographics\*\* | `user\_id`, `age`, `gender`, `country`, `urban\_rural`, `income\_level`, `employment\_status`, `education\_level`, `relationship\_status`, `has\_children`, `household\_size`, `occupation`, `ethnicity`, `language\_preference` |

| \*\*Purchasing behaviour\*\* | `weekly\_purchases`, `monthly\_spend`, `average\_order\_value`, `preferred\_payment\_method`, `coupon\_usage\_frequency`, `product\_category\_preference`, `shopping\_time\_of\_day`, `weekend\_shopper`, `impulse\_purchases\_per\_month`, `return\_frequency`, `return\_rate`, `budgeting\_style`, `last\_purchase\_date` |

| \*\*Engagement\*\* | `device\_type`, `daily\_session\_time\_minutes`, `product\_views\_per\_day`, `ad\_views\_per\_day`, `ad\_clicks\_per\_day`, `wishlist\_items\_count`, `cart\_items\_average`, `cart\_abandonment\_rate`, `checkout\_abandonments\_per\_month`, `purchase\_conversion\_rate`, `app\_usage\_frequency`, `notification\_response\_rate` |

| \*\*Loyalty and social\*\* | `loyalty\_program\_member`, `brand\_loyalty\_score`, `referral\_count`, `premium\_subscription`, `social\_media\_influence\_score`, `social\_sharing\_frequency`, `review\_writing\_frequency`, `account\_age\_months` |

| \*\*Lifestyle and wellbeing\*\* | `environmental\_consciousness`, `health\_conscious\_shopping`, `travel\_frequency`, `hobby\_count`, `reading\_habits`, `exercise\_frequency`, `overall\_stress\_level`, `stress\_from\_financial\_decisions`, `sleep\_quality`, `physical\_activity\_level`, `mental\_health\_score` |



The Bronze schema is defined explicitly (`StructType` with 60 fields) rather than inferred, which makes ingestion fast and consistent.



\---



\## 🔄 Pipeline Walkthrough



\### 1️⃣ `01\_bronze\_producer`: Simulate incoming data

\- Reads the raw CSV with pandas.

\- Splits it into chunks of \*\*15,000 rows\*\* and writes each as `batch\_XXXX.csv` into the landing folder.

\- Mimics an upstream system delivering files over time (68 files of 15,000 rows each = 1,020,000 rows).



\### 2️⃣ `02\_bronze\_consumer`: Ingest into Bronze

\- Reads the landing folder with `spark.readStream` using an explicit 60-field schema.

\- Uses `foreachBatch` to append each micro-batch into a \*\*Delta\*\* table.

\- Runs with `trigger(availableNow=True)`, processing all available files and then stopping (batch-style streaming).

\- A \*\*checkpoint\*\* location tracks which files were already processed, so a re-run does not re-ingest them.

\- Validation: Bronze contains \*\*1,020,000 rows\*\* (all rows processed in one micro-batch at \~118k rows/sec).



\### 3️⃣ `03\_silver\_cleaning`: Clean and standardise

Steps applied in order:



1\. \*\*Remove exact duplicates\*\* with `dropDuplicates()`: 1,020,000 → 1,006,176 rows (\*\*13,824 removed\*\*).

2\. \*\*Profile\*\* numeric columns (`describe()`) and categorical values to find issues.

3\. \*\*Cap extreme values at the 99th percentile\*\* (rows are kept, not deleted) for `monthly\_spend`, `average\_order\_value`, `income\_level`, `daily\_session\_time\_minutes`, `product\_views\_per\_day`, `referral\_count`, `brand\_loyalty\_score`. Example: `monthly\_spend` max 150,000 → 12,000.

4\. \*\*Fix invalid values with the median of valid values:\*\*

&#x20;  - `age` outside 10–100 (2,000 rows) → median 49

&#x20;  - `household\_size` outside 1–15 (2,000 rows) → median 6

&#x20;  - negative `monthly\_spend` (657 rows) → median 2,210

5\. \*\*Fill missing `last\_purchase\_date`\*\* with the median purchase date (2026-01-02).

6\. \*\*Standardise categorical text\*\* (trim, lowercase, then map to a canonical value) for `country`, `product\_category\_preference`, `gender`, `device\_type`, `preferred\_payment\_method`.

7\. \*\*Fill missing categorical values with `"Unknown"`\*\* for `employment\_status`, `education\_level`, `product\_category\_preference`, `device\_type`, `preferred\_payment\_method`, `gender`, `country`.

8\. \*\*Write\*\* the result to a Silver Delta table (\*\*1,006,176 rows × 60 columns\*\*).



\### 4️⃣ `04\_gold\_layer`: Business aggregations

Reads Silver and builds five summary tables, all including \*\*country\*\* so every table can be filtered or joined by country. Each is written to its own Delta folder under `gold/` (see next sections).



\---



\## 🧹 Data Quality Issues Handled



| Issue | Example | Fix |

|-------|---------|-----|

| Duplicate rows | 13,824 exact duplicates | `dropDuplicates()` |

| Misspellings in country | `austrailia`, `canda`, `chinaa`, `franc`, `germny`, `inida`, `jappan` | Mapping dictionary |

| Misspellings in category | `beuty`, `electroincs`, `fasion`, `sport`, `toy` | Mapping dictionary |

| Misspelled gender | `mael` | Mapping dictionary |

| Inconsistent case / format | `brazil`, `desktop`, `u.s.a`, `u.k.`, `home and kitchen` | `lower(trim())` + map |

| Invalid ages | `< 10` or `> 100` | Replace with median |

| Invalid household size | `< 1` or `> 15` | Replace with median |

| Negative spend | `monthly\_spend < 0` | Replace with median |

| Extreme outliers | `monthly\_spend` up to 150,000 | Cap at 99th percentile |

| Missing categorical values | \~20k to 32k nulls per column | Fill with `"Unknown"` |

| Missing `last\_purchase\_date` | nulls | Fill with median date |



\---



\## 🥇 Gold Layer Tables



| Table | Grain (group by) | Rows | Main metrics |

|-------|------------------|------|--------------|

| `country\_summary` | country | 10 | customers, avg monthly spend, avg order value, avg cart abandonment, avg return rate, avg brand loyalty |

| `category\_summary` | country × product category | 90 | customers, avg monthly spend, avg weekly purchases, avg coupon usage, avg conversion rate |

| `customer\_segment\_summary` | country × spend segment × loyalty segment | 90 | customers, avg monthly spend, avg return rate |

| `device\_payment\_summary` | country × device × payment method | 280 | customers, avg order value, avg session time, avg conversion rate |

| `age\_gender\_summary` | country × age group × gender | 300 | customers, avg monthly spend, avg stress level, avg mental health score |



\*\*Segment definitions\*\*



| Segment | Rule |

|---------|------|

| Spend: High / Mid / Low | `monthly\_spend` ≥ 5,000 / ≥ 1,500 / below 1,500 |

| Loyalty: High / Medium / Low | `brand\_loyalty\_score` ≥ 7 / ≥ 4 / below 4 |

| Age groups | 18-24, 25-34, 35-44, 45-54, 55-64, 65+ |



\---



\## 📈 Key Results



Average monthly spend and order value by country, from `country\_summary`:



| Country | Customers | Avg monthly spend | Avg order value |

|---------|-----------|-------------------|-----------------|

| USA | 100,584 | 3,942.83 | 425.36 |

| Germany | 100,680 | 3,618.62 | 399.36 |

| UK | 100,467 | 3,515.86 | 386.49 |

| Australia | 100,597 | 3,396.70 | 372.75 |

| Canada | 100,962 | 3,384.95 | 372.54 |

| France | 100,633 | 3,164.32 | 357.38 |

| China | 100,805 | 2,755.34 | 296.42 |

| Brazil | 101,133 | 1,805.66 | 219.02 |

| India | 100,545 | 1,531.93 | 187.28 |



\*(Japan is also in the table; see the Gold output for its figures.)\*



Observations:

\- Customer counts are almost evenly split across the 10 countries (\~100k each), while \*\*spend differs a lot\*\*: the USA's average monthly spend is roughly 2.6× India's.

\- Cart abandonment (\~44.5%), return rate (\~41.5%) and brand loyalty (\~5.3) are nearly identical across countries.

\- \*\*Electronics\*\* is the most common category preference in every country shown.

\- \*\*Mobile\*\* is the most common device in the top device/payment combinations, with a preferred payment method that varies by country (e.g. Google Pay in India and China, Credit Card in the USA, Bank Transfer in Germany and France).



\---



\## ▶️ How to Run



\*\*Prerequisites:\*\* a Databricks workspace (the project was built on serverless compute, environment version 5) with a Unity Catalog Volume.



1\. \*\*Create the volume folders\*\* under `/Volumes/workspace/default/ecommerce\_data/`: `raw/`, `landing/`, `bronze/`, `silver/`, `gold/`, `checkpoints/bronze/`.

2\. \*\*Upload\*\* `e\_commerce\_shopper\_behaviour\_RAW\_bronze.csv` to the `raw/` folder.

3\. \*\*Import\*\* the four notebooks into your workspace.

4\. \*\*Run in order:\*\*



&#x20;  | Order | Notebook | Result |

&#x20;  |-------|----------|--------|

&#x20;  | 1 | `01\_bronze\_producer` | 68 CSV files in `landing/` |

&#x20;  | 2 | `02\_bronze\_consumer` | Bronze Delta table (1,020,000 rows) |

&#x20;  | 3 | `03\_silver\_cleaning` | Silver Delta table (1,006,176 rows) |

&#x20;  | 4 | `04\_gold\_layer` | 5 Gold Delta tables |



5\. \*\*Query the output\*\*, for example:



```python

spark.read.format("delta").load("/Volumes/workspace/default/ecommerce\_data/gold/country\_summary").display()

```



> If you use a different catalog, schema, or volume name, update the path variables at the top of each notebook.



> \*\*Re-running from scratch:\*\* the Bronze stage appends data. To rebuild, clear the `bronze/` and `checkpoints/bronze/` folders (and old `landing/` files) first, otherwise you may get duplicates or skipped files.



\---



\## 🧠 Design Decisions and Known Limitations



\- \*\*Explicit schema in Bronze:\*\* avoids schema inference cost on 1M+ rows and keeps types stable.

\- \*\*`availableNow` trigger:\*\* gives streaming semantics (checkpointing, incremental file discovery) with batch-style execution, which suits a one-off or scheduled load.

\- \*\*Cap instead of delete outliers:\*\* keeps the row count and customer base intact while limiting distortion of averages.

\- \*\*Median imputation\*\* for invalid numerics is simple and robust but flattens the real distribution; for modelling work, a more careful strategy may be preferred.

\- \*\*Numeric nulls remain in Silver:\*\* only categoricals and `last\_purchase\_date` are filled. Numeric columns such as `age`, `income\_level`, `monthly\_spend`, `cart\_abandonment\_rate`, `average\_order\_value` and `coupon\_usage\_frequency` still contain roughly 20k nulls each. Spark's `avg()` ignores nulls, so Gold averages are unaffected, but consumers should be aware.

\- \*\*Unmapped category values become NULL, then `"Unknown"`:\*\* the mapping dictionaries are exhaustive for the typos observed; any new spelling variant would be silently turned into `Unknown` rather than kept.

\- \*\*`referral\_count` type change:\*\* it is an integer in Bronze but becomes a double in Silver because of the percentile-capping step.

\- \*\*Silver and Gold use full overwrite\*\* (`dbutils.fs.rm` followed by `overwrite`), so each run rebuilds the layer from scratch, which is simple and idempotent.

\- \*\*No primary key enforcement:\*\* `user\_id` is not used as a unique key; duplicates were removed on the full-row match only.



\---



\## 🚀 Future Improvements



\- Register Silver and Gold as \*\*Unity Catalog tables\*\* instead of path-based Delta folders.

\- Add automated \*\*data quality checks\*\* (e.g. Delta expectations or Great Expectations) with row-count reconciliation between layers.

\- Make Bronze ingestion \*\*idempotent\*\* with Auto Loader (`cloudFiles`) and add ingestion metadata (source file, load timestamp).

\- Orchestrate the four notebooks as a \*\*Databricks Workflow / Job\*\* with scheduling and alerts.

\- Add \*\*dashboards\*\* (Databricks SQL / Power BI) on top of the Gold tables.

\- Parse `last\_purchase\_date` into a proper `DateType` column and add recency features.

\- Expand Gold with customer-level features for churn or spend-prediction models.



\---



\## 👤 Author



\*Rajvardhan Devkar\*

