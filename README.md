# ETL-processes HW Final
Итоговое домашнее задание (модуль 3) по учебной дисциплине «ETL-процессы».  
Тема: «**Реализация ETL-процессов с использованием MongoDB, PostgreSQL и Apache Airflow**».  
Выполнил студент: Хорасанджян Левон, МИНДА251.

## Task
Необходимо реализовать **2 ETL-процесса**.

### ETL-процесс №1 — репликация данных
1. генерация тестовых данных;
2. загрузка сгенерированных данных в **нереляционную БД MongoDB**;
3. репликация и трансформация данных из **MongoDB в PostgreSQL**.

### ETL-процесс №2 — построение аналитических витрин
1. построение **аналитических витрин** на основе данных в PostgreSQL;
2. обновление витрин с использованием **Apache Airflow**.

В проекте должны быть реализованы:

- нереляционная БД (MongoDB);
- реляционная БД (PostgreSQL);
- генерация тестовых данных;
- ETL-репликация данных;
- аналитические витрины;
- Airflow DAG для оркестрации процессов.

## Solution
Для решения задачи использован инструмент **Apache Airflow**.  
Реализованы два отдельных DAG:

- `etl_mongo_to_postgres` — пайплайн репликации данных из **MongoDB** в **PostgreSQL**.  
  DAG выполняет проверку доступности источника и целевой БД, после чего запускает
  ETL-скрипт `etl_pipeline.py`, который извлекает данные из MongoDB, выполняет их
  трансформацию (нормализацию вложенных структур) и загружает в реляционную БД PostgreSQL.

- `etl_refresh_marts` — пайплайн обновления **аналитических витрин** в PostgreSQL.  
  DAG выполняет проверку подключения к PostgreSQL и обновляет материализованные
  представления `etl.user_activity_mart` и `etl.support_efficiency_mart`
  с помощью команды `REFRESH MATERIALIZED VIEW`.

В качестве источника данных используется **MongoDB**, в которой хранятся
сгенерированные тестовые данные.  
Целевой системой является **PostgreSQL** (база данных `etl_db`), где
хранятся нормализованные таблицы и аналитические витрины.  
Все компоненты системы (**MongoDB, PostgreSQL и Apache Airflow**) развёрнуты
в контейнерах **Docker Compose**.

## Repository Description
В репозитории настроено окружение **Apache Airflow** с использованием **Docker Compose**.  

Содержимое репозитория по папкам:
### dags
Содержит **Airflow DAG**, управляющие ETL-процессами:
- `etl_mongo_to_postgres.py` — DAG репликации данных из **MongoDB** в **PostgreSQL**;
- `etl_refresh_marts.py` — DAG обновления аналитических витрин (`user_activity_mart` и `support_efficiency_mart`).

### scripts
Содержит Python-скрипты ETL-процессов:
- `generate_data.py` — генерация тестовых данных для MongoDB;
- `etl_pipeline.py` — ETL-скрипт репликации данных MongoDB → PostgreSQL.

### sql
SQL-скрипты для создания структуры базы данных и аналитических витрин:
- `create_tables.sql` — создание схемы `etl` и таблиц в PostgreSQL;
- `marts.sql` — создание аналитических витрин (`materialized views`).

### postgres/init
Папка с SQL-скриптом инициализации PostgreSQL при запуске контейнера.

### task
Папка с файлом с постановкой задания.

### Infrastructure

- `docker-compose.yml` — конфигурация контейнеров **Airflow, PostgreSQL и MongoDB**.
- `Dockerfile` — кастомный образ Airflow с установленными зависимостями.
- `requirements.txt` — Python-зависимости проекта.

## Credentials

### Airflow UI
URL: http://localhost:8080  
Username: levon  
Password: 12345  

### PostgreSQL
Host: postgres  
Port: 5432  
Database: etl_db  
User: levon  
Password: 12345

### MongoDB
URI: mongodb://mongo:27017/  
Database: etl_mongo

## Results
Подведём итоги выполнения задания с использованием **MongoDB, PostgreSQL и Apache Airflow**:
- сгенерированы тестовые данные и загружены в MongoDB;
- реализована ETL-репликация MongoDB -> PostgreSQL;
- создана реляционная модель данных в схеме `etl`;
- построены аналитические витрины `user_activity_mart` и `support_efficiency_mart`;
- оркестрация процессов реализована через Airflow DAG.
