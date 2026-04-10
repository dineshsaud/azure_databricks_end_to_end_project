# 🚀 Azure Databricks End-to-End Data Engineering Project

A production-grade, real-world data engineering project built on **Azure Databricks** for a food delivery platform. This project covers the full data engineering lifecycle — from raw streaming events to AI-powered business insights — using the **Medallion Architecture (Bronze → Silver → Gold)**.

---

## 📐 Architecture

![Architecture Diagram](architecture.png)

```
Data Sources          Ingestion              Bronze          Silver           Gold
─────────────         ─────────              ──────          ──────           ────
streaming.orders  →   Azure Event Hubs   →   orders      →  fact_orders   →  daily_sales_summary
                                                         →  fact_order_items
customers         →   Azure SQL          →   customers   →  dim_customers →  customer_360
restaurants       →   Lakeflow Connect   →   restaurants →  dim_restaurants→ restaurant_reviews
menu_items        →   + Ingestion        →   menu_items  →  dim_menu_items
historical_orders →     Gateway          →   hist_orders
reviews           →                      →   reviews     →  fact_reviews  →
                                                            (AI Sentiment)
```

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Data Platform | Azure Databricks |
| Pipeline Framework | Delta Live Tables (DLT) |
| Real-time Ingestion | Azure Event Hubs |
| Batch Ingestion | Azure SQL + Lakeflow Connect |
| Storage | Azure Data Lake Storage Gen2 (Delta Lake) |
| AI/ML | Azure OpenAI GPT-4o |
| Data Governance | Unity Catalog |
| Languages | Python, SQL |
| Version Control | GitHub |

---

## 📁 Project Structure

```
azure_databricks_end_to_end_project/
│
├── pipeline_ingestion_eventhub/
│   ├── explorations/
│   │   └── eventhub.py              # Event Hub connection exploration
│   └── transformations/
│       └── bronze_orders.py         # Bronze orders streaming table
│
├── pipeline_transformation_silver/
│   ├── explorations/
│   │   └── fact_orders.ipynb        # Silver exploration notebooks
│   └── transformations/
│       ├── facts_order.py           # fact_orders DLT table
│       ├── fact_order_items.py      # fact_order_items DLT table
│       └── facts_reviews.py         # fact_reviews + AI sentiment analysis
│
├── pipeline_transform_gold/
│   ├── daily_sales_summary.py       # Daily sales aggregations
│   ├── daily_customer_360.py        # Customer 360 view
│   └── daily_restaurant_reviews.py  # Restaurant review analytics
│
└── README.md
```

---

## 🔄 Data Flow

### 1. Data Sources

**Streaming (Real-time):**
- Orders generated every 3 seconds and sent to **Azure Event Hubs**
- Each order contains order ID, customer, restaurant, items, amount, payment method

**Batch (Static):**
- `customers`, `restaurants`, `menu_items`, `historical_orders`, `reviews` loaded into **Azure SQL Database**

---

### 2. Data Ingestion

**Event Hub → Bronze:**
- Spark Structured Streaming reads live orders from Event Hub
- Raw JSON messages parsed and stored in `01_bronze.orders`

**Azure SQL → Bronze (via Lakeflow Connect):**
- **Ingestion Gateway** extracts changes from SQL Server using CDC
- Stages data in `00_landing` schema
- **Ingestion Pipeline** loads staged data into `01_bronze` tables

---

### 3. Bronze Layer (`01_bronze`)

Raw data landed as-is with minimal transformation:

| Table | Source | Type |
|---|---|---|
| `orders` | Azure Event Hubs | Streaming |
| `customers` | Azure SQL | Batch |
| `restaurants` | Azure SQL | Batch |
| `menu_items` | Azure SQL | Batch |
| `historical_orders` | Azure SQL | Batch |
| `reviews` | Azure SQL | Batch |

---

### 4. Silver Layer (`02_silver`)

Cleaned, structured and enriched tables:

| Table | Description |
|---|---|
| `fact_orders` | Orders with parsed timestamps, date/hour extraction, is_weekend flag |
| `fact_order_items` | Exploded order items with quantity, price, subtotal |
| `dim_customers` | Clean customer dimension with loyalty tier |
| `dim_restaurants` | Restaurant dimension with cuisine and location |
| `dim_menu_items` | Menu items with categories and pricing |
| `fact_reviews` | Reviews enriched with **AI sentiment analysis** |

**Data Quality Expectations (DLT):**
- `valid_order_id` — order_id IS NOT NULL
- `valid_amount` — total_amount > 0
- `valid_sentiment` — sentiment IN ('positive', 'neutral', 'negative')
- And more per table...

---

### 5. AI Integration — Sentiment Analysis

Since Databricks Model Serving is not available in Canada East, **Azure OpenAI GPT-4o** was integrated directly via a Python UDF:

```python
def analyze_review(review_text):
    client = openai.AzureOpenAI(...)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": f"Analyze review: {review_text}"}]
    )
    return response.choices[0].message.content
```

For each review, GPT-4o returns a structured JSON:
```json
{
  "sentiment": "negative",
  "issue_delivery": true,
  "issue_delivery_reason": "Food arrived late",
  "issue_food_quality": true,
  "issue_food_quality_reason": "Food was cold",
  "issue_pricing": false,
  "issue_portion_size": false
}
```

---

### 6. Gold Layer (`03_gold`)

Business-ready analytical tables:

| Table | Description |
|---|---|
| `daily_sales_summary` | Daily revenue, orders, avg order value by date |
| `customer_360` | Complete customer view with lifetime spend, favourite restaurant, sentiment |
| `restaurant_reviews` | Per-restaurant issue breakdown (delivery, food quality, pricing, portion size) |

---

## ⚙️ Azure Resources

| Resource | Type | Purpose |
|---|---|---|
| `ws-dbxproject` | Azure Databricks | Main workspace |
| `sqlserverdbrs/sqldb` | Azure SQL | Reference data storage |
| `rg-ns-dbhproject` | Event Hubs Namespace | Real-time order streaming |
| `dnsuberstrg` | Storage Account (ADLS) | Delta Lake storage |
| `dbx-openai` | Azure OpenAI | GPT-4o sentiment analysis |

---

## 🚀 How to Run

### Prerequisites
- Azure subscription
- Azure Databricks workspace (Premium tier)
- Azure SQL Database
- Azure Event Hubs
- Azure OpenAI resource with GPT-4o deployment

### Steps

1. **Generate reference data:**
```bash
python generate_data.py
```

2. **Load data into Azure SQL:**
```bash
python load_to_sql.py
```

3. **Start streaming orders to Event Hub:**
```bash
python generate_orders.py
```

4. **Deploy pipelines in Databricks:**
- Create Lakeflow Connect ingestion pipeline (SQL → Bronze)
- Create ETL pipeline (Bronze → Silver → Gold)
- Run pipelines

---

## 📊 Key Learnings

- CDC (Change Data Capture) with Lakeflow Connect and Ingestion Gateway
- Streaming vs batch ingestion patterns in production
- Data quality expectations in Delta Live Tables
- Integrating external AI/LLM APIs into data pipelines
- Star schema design for analytical workloads
- Working around regional cloud limitations creatively

---

## 🔗 Links

- **GitHub:** https://github.com/dineshsaud/azure_databricks_end_to_end_project
- **Azure Databricks Documentation:** https://docs.databricks.com
- **Delta Live Tables:** https://docs.databricks.com/delta-live-tables

---

## 📝 License

This project is for educational purposes.
