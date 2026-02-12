# 🌍 Zero Hunger — Big Data Analytics Pipeline  
(Hadoop, MapReduce, Spark, Hive)

**University of Houston–Clear Lake | Aug 2024 – Dec 2024**

An end-to-end big data workflow built to analyze global hunger indicators and identify hunger hotspots using distributed computing technologies including Hadoop, MapReduce, Spark, and Hive.

This project aligns with **United Nations Sustainable Development Goal (SDG) 2: Zero Hunger** and demonstrates how big data can transform global policy planning through scalable analytics and predictive modeling.

---

## 🎯 Project Objective

To identify hunger hotspots and analyze root causes of food insecurity by integrating:

- Agricultural productivity data
- Malnutrition rates
- Economic indicators
- Food affordability metrics

The system processes large-scale global datasets and generates actionable, policy-ready insights for governments, NGOs, and international organizations.

---

## 📊 Dataset Overview

- Records: **86,657**
- Features: **13**
- Sources:
  - FAO
  - World Bank
  - UNICEF
  - Kaggle agricultural datasets

### Key Indicators:
- Cost of healthy diet
- Prevalence of food unaffordability
- Number of people unable to afford nutritious food
- Agricultural production output
- Economic stability indicators

---

Raw Data Sources (FAO / World Bank / UNICEF)
↓
Data Cleaning & Normalization
↓
HDFS Storage (Distributed)
↓
Hadoop Streaming MapReduce (Python)
↓
Hive Queries & Spark Aggregations
↓
Machine Learning Forecasting
↓
Visualization & Dashboard
↓
Policy Insights

---

## 🧹 Data Engineering Pipeline

### 1️⃣ Data Collection
- Downloaded structured CSV datasets from trusted global sources
- Documented metadata and feature descriptions

### 2️⃣ Preprocessing
- Handled missing values
- Normalized economic indicators
- Standardized malnutrition metrics
- Removed inconsistencies
- Integrated multiple datasets into unified schema

---

## ⚙️ Big Data Technologies Used

### 🗄️ HDFS
- Distributed storage of hunger datasets

### 🧮 Hadoop Streaming MapReduce (Python)
Implemented custom mappers and reducers to:

- Aggregate hunger metrics by region and year
- Compute average malnutrition rates
- Analyze food affordability trends
- Identify high-risk regions

Example MapReduce Logic:

**Mapper**
- Extract (Region, Year, Indicator)
- Emit key-value pairs

**Reducer**
- Aggregate averages or totals per region

---

### 🧾 Hive
- SQL-based aggregation
- Structured queries for trend analysis
- Generated analysis-ready tables

---

### ⚡ Spark
- Distributed aggregation
- Faster transformations
- Feature engineering for predictive modeling

---

## 🤖 Machine Learning Module

Built predictive models to forecast hunger trends.

- Used historical hunger indicators
- Applied regression-based forecasting models
- Cross-validation to prevent overfitting

### 🎯 Model Performance
- Achieved ~95% internal accuracy for hunger trend prediction
- Validated via cross-validation metrics

---

## 📊 Visualization & Insights

Generated:

- Heatmaps for hunger hotspots
- Trend lines for malnutrition rates
- Regional comparison dashboards
- Food affordability patterns
- Agricultural productivity correlations

### Key Insights

- Strong correlation between agricultural output and malnutrition rates
- Economic instability directly impacts food affordability
- Identified emerging high-risk regions
- Data-driven prioritization of intervention zones

---

## 🛠 Tech Stack

- Hadoop
- HDFS
- MapReduce (Python)
- Hive
- Apache Spark
- Python
- Pandas
- Matplotlib
- Seaborn

---


---

## 📈 Evaluation

The system was evaluated using:

- Accuracy
- Precision
- Recall
- F1-score
- Cross-validation
- Trend consistency validation

The pipeline demonstrated scalable and reliable hunger hotspot detection.

---

## 🌍 Real-World Impact

- Supports proactive hunger intervention planning
- Enables scalable multi-country analysis
- Reduces reliance on manual data processing
- Bridges gap between raw data and policy decisions
- Fully aligned with SDG 2 (Zero Hunger)

---

## 🚀 Future Enhancements

- Integrate climate data for crop impact modeling
- Add satellite imagery analysis
- Real-time dashboard deployment (Cloud)
- Expand socioeconomic features
- Deploy as cloud-based API system
- Apply deep learning models for forecasting

---

## 🔬 Key Learnings

- Distributed computing significantly reduces processing time for large datasets.
- MapReduce is powerful for scalable aggregation.
- Combining big data tools with ML enhances predictive insight.
- Data-driven policymaking can dramatically improve global intervention strategies.

---

## 👨‍💻 Authors

Govardhan Reddy Narala  
Divya Keelanur  
Ramya Vankayalapati  
Rojesh Thapa  
Tharun Athota  

M.S. Data Science  
University of Houston–Clear Lake  

---


## 🏗️ Architecture Overview

