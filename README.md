# DBManager – Secure PostgreSQL Connection Manager in Python

A reusable Python class designed to **securely connect to PostgreSQL databases**, execute SQL queries, and upload `pandas` DataFrames using external configuration files.

This project demonstrates practical skills in:

* Python object-oriented programming (OOP)
* Secure credential management with `config.ini`
* Database connectivity with `psycopg2`
* Query execution with `pandas`
* Data uploads using `SQLAlchemy`
* ETL and reporting workflows

---

## Project Structure

```text
db-manager/
│
├── DBManager.py
├── config.example.ini
├── requirements.txt
└── README.md
```

---

## Features

* Secure connection to PostgreSQL databases
* Externalized credentials and SQL queries
* Read SQL results directly into `pandas` DataFrames
* Upload DataFrames to database tables
* Reusable class-based design
* Easy integration into analytics and ETL pipelines

---

## Installation

Clone this repository:

```bash
git clone https://github.com/CAPalacio03/db-manager.git
cd db-manager
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Configuration

Create a `config.ini` file based on the example template:

```ini
[database]
host=your_host
port=5432
dbname=your_database
user=your_user
password=your_password

[queries]
sample_query=SELECT * FROM your_table;
```

> Important: Do **not** upload your real `config.ini` file to GitHub.

Use `.gitignore` to exclude sensitive credentials.

---

## Example Usage

### Connect to database

```python
from DBManager import DBManager

db = DBManager("config.ini")
```

---

### Run stored query

```python
df = db.fetch_dataframe("sample_query")
print(df.head())
```

---

### Execute custom SQL

```python
query = "SELECT * FROM users LIMIT 10"
df = db.execute_sql(query)
```

---

### Upload DataFrame

```python
db.upload_dataframe(df, "users_backup", if_exists="append")
```

---

### Close connection

```python
db.close()
```

---

## Skills Demonstrated

This project was developed to showcase technical capabilities in:

* Python programming
* Database management
* Secure configuration handling
* Data engineering workflows
* Reporting automation

---

## Tech Stack

* Python
* pandas
* psycopg2
* SQLAlchemy
* PostgreSQL

---

## Author

**Carlos A. Palacio A.**
Data Analyst | Python | SQL | Statistics | Experimentation
