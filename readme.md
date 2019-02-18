# README

## Instalation

```
pip install pipenv
pipenv install 
pipenv shell
```

# How to Use

## ETL

In one terminal
```
airflow webserver 
```

In another one
```
airflow scheduler 
```

Then open in http://localhost:8080 and execute `clean` DAG

### NOTE
The final dataset is named `final_result.csv`


## Web Scraping

```
cd dags
python web_scrapper.py
```

### NOTE

The final dataset is named `webscrapper.csv`

