# WorkShoop1

# **🚀 ETL Process for Candidate Management**

This project was created with a clear objective: to design and implement an end-to-end ETL pipeline that simulates a real-world recruitment process in technology companies.

The idea is to take raw data (CSV with candidate information), process it under a Dimensional Model, and load it into a Data Warehouse (MySQL). Subsequently, key performance indicators (KPIs) and visualizations are generated to strategically evaluate the available talent and the effectiveness of the recruitment process.

In other words, this project demonstrates how to transform scattered data into structured and useful information for decision-making.

------------


## **📂 General Project Flow**

Extraction
Starting with a CSV file (candidates.csv) containing applicant information: name, email, country, technology, years of experience, technical test scores, and interview scores.

Transformation

Data cleaning: column name standardization, data type conversion, handling of nulls.

Creation of derived fields (e.g., hired flag based on score thresholds).

Construction of dimensions: Date, Candidate, Country, Technology, and Seniority.

Preparation of the fact table (fact_application) that centralizes metrics and foreign keys.

Load

Insertion of data into a relational Data Warehouse in MySQL.

Persistence of dimensions and facts following best practices of star schema modeling.

## 📊Visualization and KPIs

From the DW, metrics such as:

🔹 Hiring rate → % of candidates hired out of those evaluated.
🔹 Hiring distribution by country → identifies the most competitive talent markets.
🔹 Average years of experience (YOE) → comparison across seniority levels.
🔹 Technical efficiency → relationship between code challenge and interview scores.

These indicators are key in a recruitment process and serve as inputs for HR and technology managers.
**
# ⚙️ Prerequisites

Before running the project, make sure you have:

Python 3.9+

MySQL 8.x (as database engine for the DW)

Updated pip

### 📦 Installation

Clone this repository:

    git clone https://github.com/youruser/etl-candidates.git
    cd etl-candidates


Create a virtual environment (optional but recommended):

python -m venv venv
source venv/bin/activate   # Linux / Mac
venv\Scripts\activate      # Windows


 # **Install dependencies:***

pip install -r requirements.txt


Main dependencies:

    pandas → data manipulation
    numpy → numerical operations

    sqlalchemy → MySQL connection

    mysql-connector-python → MySQL driver

    python-dotenv → environment variable management
    
***Configure your .env file:***

Example .env:
    DB_USER=user
    DB_PASS=password
    DB_HOST=localhost
    DB_PORT=port
    DB_NAME=dbname


