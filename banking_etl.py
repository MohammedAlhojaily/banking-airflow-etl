from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="banking_etl",
    start_date=datetime(2026, 8, 19),
    schedule=None,
    catchup=False
) as dag:

    create_staging = BashOperator(
        task_id="create_staging_table",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        CREATE DATABASE IF NOT EXISTS banking;

        DROP TABLE IF EXISTS banking.stg_transactions;

        CREATE TABLE banking.stg_transactions (
            transaction_id STRING,
            customer_id STRING,
            account_id STRING,
            amount DOUBLE,
            status STRING,
            city STRING,
            channel STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        TBLPROPERTIES ('skip.header.line.count'='1');
        "
        """
    )

    load_staging = BashOperator(
        task_id="load_staging_table",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        LOAD DATA LOCAL INPATH '/opt/data/raw/raw20260819.csv'
        OVERWRITE INTO TABLE banking.stg_transactions;
        "
        """
    )

    create_final = BashOperator(
        task_id="create_final_table",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        DROP TABLE IF EXISTS banking.final_transactions;

        CREATE TABLE banking.final_transactions
        STORED AS PARQUET
        AS
        SELECT
            transaction_id,
            customer_id,
            account_id,
            amount,
            status,
            city,
            channel
        FROM banking.stg_transactions
        WHERE transaction_id IS NOT NULL
          AND account_id IS NOT NULL
          AND amount > 0
          AND channel IN ('MOBILE','ATM','BRANCH');
        "
        """
    )

    quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        SELECT COUNT(*) AS final_count
        FROM banking.final_transactions;
        "
        """
    )

    create_staging >> load_staging >> create_final >> quality_check
