# dbt Data Models — SaaS Analytics
### Lakshmi Surya — Analytics Engineer

> Production-style dbt project demonstrating staging, intermediate, and mart layers for a SaaS business on Snowflake.

---

## 📁 Project Structure

```
dbt-data-models/
├── models/
│   ├── staging/
│   │   ├── stg_subscriptions.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_invoices.sql
│   │   └── stg_events.sql
│   ├── intermediate/
│   │   ├── int_customer_mrr.sql
│   │   ├── int_revenue_movements.sql
│   │   └── int_customer_health.sql
│   └── marts/
│       ├── finance/
│       │   ├── fct_mrr.sql
│       │   └── fct_arr_by_segment.sql
│       └── cs/
│           ├── fct_churn.sql
│           └── dim_customer_health.sql
├── macros/
│   ├── date_spine.sql
│   └── safe_divide.sql
├── tests/
│   └── generic/
│       └── not_negative.sql
├── dbt_project.yml
└── README.md
```

---

## 📄 Key Models

### Staging — `stg_subscriptions.sql`
```sql
-- Cleans raw subscriptions from source system (e.g. Salesforce / Stripe)
WITH source AS (
    SELECT * FROM {{ source('raw', 'subscriptions') }}
),
renamed AS (
    SELECT
        subscription_id                             AS subscription_id,
        LOWER(TRIM(customer_id))                    AS customer_id,
        LOWER(TRIM(plan_name))                      AS plan_name,
        LOWER(TRIM(status))                         AS status,
        CAST(start_date AS DATE)                    AS start_date,
        CAST(end_date AS DATE)                      AS end_date,
        CAST(annual_contract_value AS FLOAT)        AS acv,
        CAST(monthly_recurring_revenue AS FLOAT)    AS mrr,
        LOWER(TRIM(billing_frequency))              AS billing_frequency,
        _loaded_at
    FROM source
    WHERE subscription_id IS NOT NULL
)
SELECT * FROM renamed
```

---

### Intermediate — `int_customer_mrr.sql`
```sql
-- One row per customer per month, with MRR contribution
WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2023-01-01' as date)",
        end_date="cast(current_date as date)"
    ) }}
),
subscriptions AS (
    SELECT * FROM {{ ref('stg_subscriptions') }}
),
customer_months AS (
    SELECT
        d.date_month,
        s.customer_id,
        s.plan_name,
        s.mrr,
        s.acv,
        s.status
    FROM date_spine d
    JOIN subscriptions s
        ON d.date_month >= DATE_TRUNC('month', s.start_date)
        AND (s.end_date IS NULL OR d.date_month < DATE_TRUNC('month', s.end_date))
)
SELECT
    date_month,
    customer_id,
    plan_name,
    SUM(mrr)                                        AS monthly_mrr,
    SUM(acv)                                        AS annual_acv,
    COUNT(DISTINCT plan_name)                       AS product_count
FROM customer_months
GROUP BY 1, 2, 3
```

---

### Mart — `fct_mrr.sql`
```sql
-- Final MRR fact table used for BI dashboards
WITH customer_mrr AS (
    SELECT * FROM {{ ref('int_customer_mrr') }}
),
prior_month AS (
    SELECT
        date_month,
        customer_id,
        monthly_mrr,
        LAG(monthly_mrr) OVER (
            PARTITION BY customer_id
            ORDER BY date_month
        )                                           AS prior_mrr
    FROM customer_mrr
),
revenue_movements AS (
    SELECT
        date_month,
        customer_id,
        monthly_mrr,
        prior_mrr,
        CASE
            WHEN prior_mrr IS NULL                  THEN 'new'
            WHEN monthly_mrr > prior_mrr            THEN 'expansion'
            WHEN monthly_mrr < prior_mrr            THEN 'contraction'
            WHEN monthly_mrr = 0 AND prior_mrr > 0  THEN 'churned'
            ELSE                                         'retained'
        END                                         AS revenue_category,
        monthly_mrr - COALESCE(prior_mrr, 0)        AS net_mrr_change
    FROM prior_month
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['date_month', 'customer_id']) }} AS mrr_id,
    date_month,
    customer_id,
    monthly_mrr,
    prior_mrr,
    revenue_category,
    net_mrr_change,
    SUM(monthly_mrr) OVER (ORDER BY date_month)     AS cumulative_mrr,
    CURRENT_TIMESTAMP                               AS _updated_at
FROM revenue_movements
```

---

### Custom Macro — `safe_divide.sql`
```sql
{% macro safe_divide(numerator, denominator) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN NULL
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}
```

Usage: `{{ safe_divide('expansion_mrr', 'total_beginning_mrr') }}`

---

### Generic Test — `not_negative.sql`
```sql
{% test not_negative(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 0
{% endtest %}
```

Usage in `schema.yml`:
```yaml
columns:
  - name: monthly_mrr
    tests:
      - not_null
      - not_negative
```

---

### `dbt_project.yml`
```yaml
name: 'saas_analytics'
version: '1.0.0'
config-version: 2

profile: 'snowflake_prod'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

models:
  saas_analytics:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      finance:
        +schema: finance
      cs:
        +schema: customer_success
```

---

## 🏗️ Architecture Principles

| Layer | Materialization | Purpose |
|---|---|---|
| Staging | View | 1:1 source cleaning, no logic |
| Intermediate | Ephemeral | Business logic, reusable joins |
| Mart | Table | Final reporting layer for BI tools |

- **Sources** defined with freshness tests
- **Schema tests** on all primary/foreign keys
- **Surrogate keys** generated via `dbt_utils`
- Models organized by **business domain** (finance, cs, product)

---

## 🧰 Stack
`dbt Core` `Snowflake` `dbt_utils` `dbt-expectations` `Power BI (consumer)`

## 📬 Contact
**Lakshmi Surya** · lakshmibicog@gmail.com · [Portfolio](https://lakshmisrisurya.github.io)
