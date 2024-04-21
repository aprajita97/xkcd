# XKCD Data Pipeline

## Introduction
This project aims to extract, load, and transform data from the XKCD webcomic API into a database, providing insights into the number of views, cost of creation, and customer reviews for each comic.

In this repository, we fetch XKCD's comics data and insert it into Postgres database every Monday, Wednesday and Friday. We further use DBT to create fact tables to learn about their reviews, views and cost. We use Airflow to automate this process. 

## Getting Started

1. Clone this repository
2. Create virtual enviroment 
    - python3 -m venv env
    - source env/bin/activate
    Note: If you are creating virtual environment with another name, modify .gitignore to have it
3. Perform the following commands
    - docker-compose up airflow-init 
    - docker-compose up
4. Open localhost:8080 on your web-browser and create an Airflow connection with following values
    - Connection Id: postgres
    - Connection Type: Postgres
    - Host: host.docker.internal
    - Database: airflow
    - Login: airflow
    - Password: airflow
    - Post: 5432
5. Use a database management tool like TablePlus and create a postgres connection to see the dbt and airflow output
6. Start all DAGS from Airflow UI and run them
