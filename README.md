# 🗄️ ETL Workshop 1 — Recruitment Analytics Data Pipeline

> A complete end-to-end ETL pipeline that ingests 50,000 candidate application records from a CSV file, models them into a Star Schema Data Warehouse (MySQL), and exposes 6 analytical KPIs through SQL queries and Python visualizations.

---

## 📋 Table of Contents

- [Project Objective](#-project-objective)
- [System Architecture](#-system-architecture)
- [Star Schema Design](#-star-schema-design)
- [Grain Definition](#-grain-definition)
- [ETL Logic](#-etl-logic)
- [Data Quality Assumptions](#-data-quality-assumptions)
- [How to Run the Project](#-how-to-run-the-project)
- [KPIs & Example Outputs](#-kpis--example-outputs)
- [Project Structure](#-project-structure)
- [Technologies](#-technologies)

---

## 🎯 Project Objective

This project simulates a real-world Data Engineering challenge. The goal is to design and implement a production-grade ETL pipeline that:

1. **Extracts** raw recruitment data from a CSV file (`candidates.csv`, 50k rows)
2. **Transforms** it into a clean dimensional model (Star Schema) using Python
3. **Loads** it into a MySQL Data Warehouse via a two-phase staging strategy
4. **Exposes analytical KPIs** through SQL queries and matplotlib visualizations

All transformations happen in Python **before** data reaches the database, following a strict ETL (not ELT) approach.

---

## 🏗️ System Architecture

```
CSV File                    Python (ETL)                  MySQL DW
───────────                 ────────────                  ────────
candidates.csv  ──Extract──▶  transform_data()  ──Load──▶  *_stg tables
                              (dimensions +                     │
                              fact table)                       │ load_tables.sql
                                                               ▼
                                                         Final DW Tables
                                                               │
                                                               ▼
                                                     KPI Queries + Charts
```

**Two-phase load strategy:**
- **Phase 1 (Python → Staging):** `load.py` truncates and reloads all `*_stg` tables inside a single transaction
- **Phase 2 (Staging → Final):** `load_tables.sql` promotes data from staging to production tables via `INSERT INTO ... SELECT`

---

## ⭐ Star Schema Design

### Diagram

```
                        ┌─────────────────────┐
                        │   dim_experience    │
                        │─────────────────────│
                        │ PK experience_key   │
                        │    experience_range │
                        └──────────┬──────────┘
                                   │
┌──────────────────┐               │              ┌──────────────────┐
│   dim_seniority  │               │              │   dim_country    │
│──────────────────│               │              │──────────────────│
│ PK seniority_key │               │              │ PK country_key   │
│    seniority_lvl │               │              │    country_name  │
└────────┬─────────┘               │              └────────┬─────────┘
         │                         │                       │
         │              ┌──────────▼──────────┐            │
         │              │   fact_application  │            │
         └────────────▶│─────────────────────│◀──────────┘
                        │ PK application_key  │
                        │ FK experience_key   │◀──────────────────────┐
                        │ FK seniority_key    │                       │
                        │ FK country_key      │          ┌────────────┴──────┐
                        │ FK technology_key   │          │   dim_technology  │
                        │ FK date_key         │          │───────────────────│
                        │    code_ch_score    │          │ PK technology_key │
                        │    tech_int_score   │          │    technology_name│
                        │    hired_flag       │          └───────────────────┘
                        └──────────┬──────────┘
                                   │
                        ┌──────────▼──────────┐
                        │      dim_date        │
                        │─────────────────────│
                        │ PK date_key          │
                        │    full_date         │
                        │    day               │
                        │    month             │
                        │    month_name        │
                        │    quarter           │
                        │    year              │
                        └─────────────────────┘
```
A .png file of the Star Schema is also included in the diagrams/ directory.

### Design Decisions

**Why surrogate keys?**
All dimension primary keys (`experience_key`, `seniority_key`, etc.) are auto-generated integers, never natural values from the CSV. This isolates the warehouse from upstream data changes — if a country name is renamed in the source, the DW keys remain stable.

**Why a separate `dim_experience`?**
Raw `YOE` (years of experience) is a continuous number that is not analytically useful on its own for grouping. Bucketing it into discrete ranges (`0-2`, `3-5`, `6-10`, `10+`) during transformation means every KPI query can `GROUP BY experience_range` without repeating CASE logic.

**Why `dim_date` instead of storing the date directly on the fact?**
A date dimension allows grouping by year, quarter, month, or month name with a single join — no `EXTRACT()` or `DATE_FORMAT()` calls required in analytical queries. This is standard Kimball dimensional modeling practice.

**Why a staging layer?**
Loading to `*_stg` tables first and then promoting to final tables via SQL provides a safety checkpoint. If the Python load fails mid-way, the final tables are untouched. The staging tables have no foreign key constraints, making truncation and reload fast and safe.

**Why full reload?**
The source is a static batch file. A full truncate-and-reload is simpler, auditable, and sufficient. Incremental loading would only be justified if the source grew continuously in a streaming scenario.

---

## 📐 Grain Definition

> **One row per candidate application.**

Each record in `fact_application` represents a single application event submitted by one candidate for one technology role on one date. A candidate can appear multiple times if they applied more than once (different rows, different `application_key`).

**Measures stored at the grain:**
- `code_challenge_score` — score on the coding test (0–10)
- `technical_interview_score` — score on the technical interview (0–10)
- `hired_flag` — 1 if both scores ≥ 7, otherwise 0 (business rule)

---

## ⚙️ ETL Logic

### Extract (`src/extract.py`)

- Reads `data/raw/candidates.csv` using `;` as delimiter, UTF-8 encoding
- Immediately validates and coerces data types:
  - `Application Date` → `datetime`
  - `YOE`, `Code Challenge Score`, `Technical Interview Score` → `numeric`
- Invalid values become `NaT` / `NaN` (handled in Transform)
- Raises `SystemExit` with a clear message if the file is missing — no silent `None` returns

### Transform (`src/transform.py`)

| Step | What happens |
|---|---|
| Column normalization | Strip whitespace, lowercase, replace spaces with `_` |
| String cleaning | Strip + lowercase: `first_name`, `last_name`, `email`, `country`, `technology`, `seniority` |
| Date normalization | `.dt.normalize()` removes any time component so each calendar date maps to exactly one `date_key` |
| Null dropping | Rows missing `application_date`, `yoe`, or either score are dropped |
| Range validation | `yoe >= 0`; scores must be in `[0, 10]` |
| `hired_flag` | `1` if `code_challenge_score >= 7 AND technical_interview_score >= 7`, else `0` |
| Experience range | `pd.cut()` with `include_lowest=True` bins YOE into `0-2 / 3-5 / 6-10 / 10+`; residual NaN becomes `"Unknown"` |
| Categorical cast | `experience_range` cast to `str` so SQLAlchemy can serialize it to MySQL |
| Dimension build | Deduplicate each categorical column, assign `index + 1` as surrogate key |
| Fact build | Left-join cleaned data against each dimension; drop any row with a null FK after merge |

### Load (`src/load.py`)

- Reads DB credentials from `.env` (never hard-coded)
- **Single transaction:** TRUNCATE all staging tables + INSERT all DataFrames inside one `engine.begin()` block — a partial failure rolls back automatically
- Dimensions are inserted **before** the fact table to satisfy referential integrity
- Verifies row count against `fact_application_stg` after load

### Promote (`sql/load_tables.sql`)

- Executed by `main.py` after staging is confirmed
- Disables FK checks → truncates final tables → re-enables FK checks → `INSERT INTO final SELECT FROM staging`
- Dimensions are promoted before the fact table

---

## 🧹 Data Quality Assumptions

| Issue | Decision |
|---|---|
| Null `application_date`, `yoe`, or scores | Row dropped — these fields define the grain and measures |
| `yoe < 0` | Filtered out as physically impossible |
| Score outside `[0, 10]` | Filtered out as a data entry error |
| String whitespace / mixed case | Stripped and lowercased for consistent dimension joins |
| `yoe = 0` | Captured in the `0-2` bin via `include_lowest=True` |
| Timestamp on date field | Normalized to midnight so one calendar date maps to one `date_key` |
| Unmatched FK after dimension merge | Row dropped to guarantee referential integrity in the DW |
| Pandas `Categorical` dtype | Cast to `str` before load to avoid SQLAlchemy serialization errors |

---

## 🚀 How to Run the Project

### Prerequisites

- Python 3.9+
- MySQL 8+ running locally
- Git

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/etl-workshop-1.git
cd etl-workshop-1
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Install and start MySQL (if not already running)

```bash
# Windows
winget install MySQL.MySQL
net start MySQL80
```

### 5. Create the database and tables

```bash
mysql -u root -p < sql/create_tables.sql
```

### 6. Configure environment variables

Create a `.env` file at the project root:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password_here
DB_NAME=etl_workshop
```

### 7. Add the source data

```
data/
└── raw/
    └── candidates.csv
```

### 8. Run the full ETL pipeline

```bash
python -m src.main
```

Expected output:

```
Starting Extraction process (Extract)...
Extraction completed successfully.
Rows extracted: 50000
Starting Transformation process (Transform)...
Transformation completed successfully.
Experience rows : 5
Seniority rows  : 5
Country rows    : X
Technology rows : X
Date rows       : X
Fact rows       : ~49XXX
Starting Load process (Load)...
✅ Load completed successfully to staging tables.
Staging fact rows loaded: ~49XXX
✅ Final tables loaded successfully.
🎉 ETL Pipeline completed successfully.
```

### 9. Generate KPI visualizations

```bash
python kpi_visualizations.py
```

Or run the notebook interactively:

```bash
jupyter notebook notebooks/kpi_visualizations.ipynb
```

Charts are saved to the `visualizations/` folder.

---

## 📊 KPIs & Example Outputs

All queries run against the final DW tables — never the raw CSV.

### KPI 1 — Hires by Technology

```sql
SELECT dt.technology_name, SUM(fa.hired_flag) AS total_hires
FROM fact_application fa
JOIN dim_technology dt ON fa.technology_key = dt.technology_key
GROUP BY dt.technology_name
ORDER BY total_hires DESC;
```

> Bar chart — shows which technologies produce the most hires

---

### KPI 2 — Hires by Year

```sql
SELECT dd.year, SUM(fa.hired_flag) AS total_hires
FROM fact_application fa
JOIN dim_date dd ON fa.date_key = dd.date_key
GROUP BY dd.year
ORDER BY dd.year;
```

> Line chart with area fill — shows the hiring trend over time

---

### KPI 3 — Hires by Seniority

```sql
SELECT ds.seniority_level, SUM(fa.hired_flag) AS total_hires
FROM fact_application fa
JOIN dim_seniority ds ON fa.seniority_key = ds.seniority_key
GROUP BY ds.seniority_level
ORDER BY total_hires DESC;
```

> Pie chart — shows the distribution of hires across seniority levels

---

### KPI 4 — Hires by Country over Years (USA, Brazil, Colombia, Ecuador)

```sql
SELECT dc.country_name, dd.year, SUM(fa.hired_flag) AS total_hires
FROM fact_application fa
JOIN dim_country dc ON fa.country_key = dc.country_key
JOIN dim_date    dd ON fa.date_key    = dd.date_key
WHERE dc.country_name IN ('usa', 'brazil', 'colombia', 'ecuador')
GROUP BY dc.country_name, dd.year
ORDER BY dc.country_name, dd.year;
```

> Multi-line chart — compares hiring trends across 4 focus countries over time

---

### KPI 5 — Hire Rate (%) by Technology

```sql
SELECT dt.technology_name,
       COUNT(*) AS total_applications,
       SUM(fa.hired_flag) AS total_hires,
       ROUND(SUM(fa.hired_flag) * 100.0 / COUNT(*), 2) AS hire_rate_pct
FROM fact_application fa
JOIN dim_technology dt ON fa.technology_key = dt.technology_key
GROUP BY dt.technology_name
ORDER BY hire_rate_pct DESC;
```

> Horizontal bar chart — reveals which technologies have the highest selection rate, not just the most applications

---

### KPI 6 — Average Scores by Seniority

```sql
SELECT ds.seniority_level,
       ROUND(AVG(fa.code_challenge_score), 2)      AS avg_code_score,
       ROUND(AVG(fa.technical_interview_score), 2) AS avg_interview_score
FROM fact_application fa
JOIN dim_seniority ds ON fa.seniority_key = ds.seniority_key
GROUP BY ds.seniority_level
ORDER BY avg_interview_score DESC;
```

> Grouped bar chart — compares average code challenge vs interview performance per seniority level

---

## 📁 Project Structure

```
etl-workshop-1/
│
├── data/
│   ├── raw/
│   │   └── candidates.csv
│   └── processed/
│
├── notebooks/
│   └── kpi_visualizations.ipynb
│
├── sql/
│   ├── create_tables.sql
│   └── load_tables.sql
│
├── diagrams/
│   └── star_schema.png
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── main.py
│
├── visualizations/
│   ├── kpi_dashboard.png
│   ├── kpi1_hires_by_technology.png
│   ├── kpi2_hires_by_year.png
│   ├── kpi3_hires_by_seniority.png
│   ├── kpi4_hires_by_country_year.png
│   ├── kpi5_hire_rate_by_technology.png
│   └── kpi6_avg_scores_by_seniority.png
│
├── kpi_visualizations.py
├── README.md
├── requirements.txt
└── .gitignore
```

---

## 🛠️ Technologies

| Tool | Purpose |
|---|---|
| Python 3.9+ | ETL pipeline logic |
| pandas | Data extraction, transformation, and dimension building |
| SQLAlchemy + PyMySQL | Database connection and data loading |
| MySQL 8 | Data Warehouse |
| matplotlib | KPI visualizations |
| python-dotenv | Secure credential management via `.env` |
| Jupyter Notebook | Interactive KPI exploration |
| Git / GitHub | Version control and portfolio hosting |

---

## 📄 License

Developed as part of the ETL (G01) course — Data Engineering and Artificial Intelligence program, Faculty of Engineering and Basic Sciences.