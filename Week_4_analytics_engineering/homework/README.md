## Module 4 Homework

# 📖 Understanding dbt Model Resolution

## 🧠 1️⃣ Context

We have the following `sources.yml` file:

```yaml
version: 2

sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```

And we have the following environment variables set:

```bash
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_SOURCE_DATASET=my_nyc_tripdata
```

### 🛠️ SQL Query:

```sql
SELECT *
FROM {{ source('raw_nyc_tripdata', 'ext_green_taxi') }}
```

## 🔍 2️⃣ How dbt Resolves the Source()

The `source()` function in dbt compiles into a fully-qualified table reference using the following structure:

```sql
SELECT * FROM `<database>.<schema>.<table>`
```

### 📜 Breakdown:

| Component | Definition              | Resolution              |
|-----------|-------------------------|--------------------------|
| **Database** | Defined in `sources.yml` | `myproject` (from `DBT_BIGQUERY_PROJECT`) |
| **Schema**   | Defined in `sources.yml` | `my_nyc_tripdata` (from `DBT_BIGQUERY_SOURCE_DATASET`) |
| **Table**    | Provided in `sources.yml` | `ext_green_taxi`           |

### ⚙️ Compilation Steps:

1. **Database:**
   - dbt reads the `database` field from the `sources.yml` file.
   - It finds `{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}`.
   - It checks the environment variable `DBT_BIGQUERY_PROJECT`, which is set to `myproject`.
   - Final value: **`myproject`**.

2. **Schema:**
   - dbt reads the `schema` field from the `sources.yml` file.
   - It finds `{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}`.
   - It checks the environment variable `DBT_BIGQUERY_SOURCE_DATASET`, which is set to `my_nyc_tripdata`.
   - Final value: **`my_nyc_tripdata`**.

3. **Table:**
   - dbt finds the table name `ext_green_taxi` directly from `sources.yml`.
   - No dynamic value or environment variable is involved here.

### ✅ Compiled SQL:

```sql
SELECT * FROM myproject.my_nyc_tripdata.ext_green_taxi
```

