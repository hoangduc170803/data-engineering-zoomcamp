## Module 4 Homework

# 📖 Q1

## 🧠 1️⃣ Context:

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

# 📖 Q2
## 🧠 1️⃣ Context

We need to modify the following dbt model (`fct_recent_taxi_trips.sql`) to allow Analytics Engineers to dynamically control the date range:

- **Development:** Process the last 7 days.
- **Production:** Process the last 30 days.

### 🔍 Original Query:

```sql
SELECT *
FROM {{ ref('fact_taxi_trips') }}
WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '30 days'
```

## 🎯 2️⃣ Requirements

- Command-line arguments should take precedence over environment variables.
- Environment variables should take precedence over default values.

## 🚀 3️⃣ Options Analysis

| **Option** | **Query** | **Explanation** |
|------------|----------|-----------------|
| 1 | `ORDER BY pickup_datetime DESC LIMIT {{ var("days_back", 30) }}` | Irrelevant to date filtering. |
| 2 | `WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", 30) }}' DAY` | Uses `var()` but ignores `env_var()`. |
| 3 | `WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", "30") }}' DAY` | Relies only on environment variables. |
| 4 | `WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY` | ✅ **Correct - follows the right priority order: command > env > default**. |
| 5 | `WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", var("days_back", "30")) }}' DAY` | Environment variable takes precedence over command-line argument. |


## 🏆 4️⃣ Correct Answer

The correct solution is:

```sql
WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
```

### 🧠 **Explanation:**
1. **`var("days_back", ...)`**: Allows passing parameters via the command line.
2. **`env_var("DAYS_BACK", ...)`**: Uses the environment variable if the command-line parameter is not provided.
3. **`"30"`**: Default value if both are missing.

### 🛠️ **Priority Order:**
1. **Command-line argument**: Run with `--vars '{"days_back": "7"}'`.
2. **Environment variable**: Set with `export DAYS_BACK=7`.
3. **Default value**: Uses `30` if both are missing.

## 🧪 5️⃣ Debugging Tips

### 🛠️ **Check Compilation**
```bash
dbt compile --select fct_recent_taxi_trips
```

### 🛠️ **Test Parameter Override**
```bash
dbt run --select fct_recent_taxi_trips --vars '{"days_back": "7"}'
```

### 🛠️ **Check Environment Variable**
```bash
echo $DAYS_BACK
```

### 🛠️ **Run dbt Debug**
```bash
dbt debug
```

## 🔍 6️⃣ Conclusion

- **Best Query:**

```sql
WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
```

- **Priority:** Command-line argument > environment variable > default value.
- **Flexible solution** for both development (7 days) and production (30 days).

🎯 **Happy modeling with dbt! 🚀**

