# 🛍️ Retail Purchase Log ETL Pipeline with PySpark

This project demonstrates a full end-to-end ETL pipeline using PySpark to process and analyze retail purchase logs. Built as a portfolio-ready showcase for Big Data Engineering, it simulates a real-world scenario of data ingestion, transformation, aggregation, and export — while handling system-level challenges along the way.

---

## 🧰 Tools & Technologies

- **Apache Spark (PySpark)**
- **Python 3.13**
- **Jupyter Notebook**
- **pandas (for final export)**
- **Windows OS (local environment)**

---

## 📂 Project Structure

```
retail_etl_project/
├── data/
│   └── retail_purchase_log.csv
├── notebooks/
│   └── retail_etl_pipeline.ipynb
├── output/
│   └── retail_purchase_cleaned.csv
├── README.md
```

---

## 📈 ETL Pipeline Overview
### ✅ 1. Data Ingestion
- Loaded raw CSV file using `spark.read.csv()` with header and schema inference.

### 🧼 2. Data Cleaning
- Removed duplicates.
- Dropped rows with nulls in key columns (`transaction_id`, `customer_id`, `product_id`, `quantity`, `price`).
- Converted `purchase_date` to timestamp.
- Created new column `total_amount = quantity * price`.

### 🔄 3. Data Transformation & Aggregation
- Total spend per customer.
- Top products by total revenue.
- Revenue by store location.
- Daily revenue trends (by purchase date).

### 💾 4. Data Export
- Originally intended to write partitioned Parquet files via Spark.
- Encountered native I/O errors due to `NativeIO$Windows.access0` limitations on Windows.
- ✅ Pivoted to using `toPandas().to_csv()` to ensure output delivery.

---

## 📌 Note on Hadoop & Windows Compatibility

> This ETL pipeline was originally designed to export partitioned Parquet files. However, due to Spark's dependency on Hadoop’s native Windows bindings (NativeIO$Windows.access0), the JVM consistently triggered system-level errors — even with all recommended workarounds (winutils.exe, Spark config overrides, and coalesced writes).  
>  
> To maintain momentum and preserve ETL logic, I pivoted to using `pandas.to_csv()` for final output. This approach guaranteed a successful export while showcasing adaptability and real-world troubleshooting.

---

## ✅ Final Output

📄 `retail_purchase_cleaned.csv`  
Cleaned, transformed, and enriched dataset exported with headers.

Example fields:
- `transaction_id`
- `customer_id`
- `product_id`
- `product_category`
- `quantity`
- `price`
- `purchase_date` (timestamp)
- `store_location`
- `total_amount` (calculated)

---
## 🚀 Run the ETL Pipeline

You can run the full process via command line using the included script:

python scripts/etl.py

This performs:

Spark session creation

CSV ingestion

ETL transformation

Export to output/retail_purchase_cleaned.csv

---
## 🔧 Tech Stack

PySpark

Python (3.13)

pandas (for export fallback)

Jupyter Notebook (for development)

---
## 🧠 What I’d Do Differently Next Time

- Set up Spark and Hadoop inside a Docker container to ensure native compatibility across operating systems.
- Export results to both Parquet and CSV formats for flexibility in downstream analytics or data warehousing.
- Integrate Apache Hive or simulate a data warehouse layer for further demonstration of production workflows.

---

## 📨 Recruiter Summary

> Built a complete PySpark-based ETL pipeline to clean and analyze retail purchase logs. Used DataFrame API, SQL expressions, and Spark transformations to compute key insights like customer spend, product revenue, and store performance. Originally planned Parquet export, but navigated and overcame Hadoop-native permission issues on Windows by pivoting to CSV export via pandas. This project highlights my ability to adapt, solve real-world data engineering problems, and deliver results under constraints.

---

## 📎 Related Projects

- Coming soon: Docker-based Spark environment for partitioned Parquet testing  
- Coming soon: Retail analytics dashboard in Tableau / Looker

---

## 🔗 Author

**Veronica Magdaleno**  
[GitHub Portfolio](https://github.com/your-username)  
[Portfolio](https://linkedin.com/in/your-profile)  
Built as part of a Big Data Engineering portfolio project.

Skills: PySpark, ETL, Data Modeling, Analytics, Jupyter

---

## 📝 License

MIT — Free to use for learning or adaptation.