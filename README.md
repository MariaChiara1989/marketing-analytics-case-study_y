# Marketing Analytics Case Study

This project analyzes how marketing spend relates to revenue performance using SQL and SQLite.

## Project Structure

```
/data               # CSV files go here (not committed to repo)
/sql                # SQL scripts
/results            # Output results (optional exports)
```

> CSV files are NOT included in the repository. Download the data and place it inside the `/data/` folder. Do **not** commit large CSV files to GitHub — add them to `.gitignore`.

---

## CSV files expected

Place these in `data/` (local only):

- `marketing_spend.csv`
- `revenue.csv`
- `external_factors.csv`

---

## How to run

### Create the SQLite database & import data

```
sqlite3 analytics.db < sql/01_schema_setup
```

This script will create the tables and import the CSV files.

### Run analysis scripts

Example sequence:

```
sqlite3 analytics.db < sql/02_data_quality
sqlite3 analytics.db < sql/03_exploratory_analysis
sqlite3 analytics.db < sql/04_channel_performance.sql
```
---

## Notes

- Data represents daily marketing spend, revenue, and external context.
- All SQL scripts are tested for SQLite compatibility.
- Column names follow the case study naming conventions.

---

## .gitignore suggestion in a proper file

```
data/*.csv
*.db
```
