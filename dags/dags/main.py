from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator, BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from datetime import datetime, timedelta
import pandas as pd
import gspread
from datetime import datetime
import numpy as np
import os

project_name = "etl_gcp"
dataset_name = "datalake_anyeli8_composer"
url = "https://docs.google.com/spreadsheets/d/1y7GQejKLerQgmkSFw0YBd7ih74kBGSIwaccMA5JgGqA/"
sa_config_path = "/home/airflow/gcs/dags/papyrus.json"
tables_config = {
    "transportistas": [
        {"name": "idTransportista", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "nombreEmpresa", "type": "STRING", "mode": "REQUIRED"},
    ],
    "categorias": [
        {"name": "idCategoria", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "nombreCategoria", "type": "STRING", "mode": "REQUIRED"},
        {"name": "descripcion", "type": "STRING", "mode": "REQUIRED"},
    ],
    "pedidos": [
        {"name": "idPedido", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "idCliente", "type": "STRING", "mode": "REQUIRED"},
        {"name": "idEmpleado", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "fechaPedido", "type": "STRING", "mode": "REQUIRED"},
        {"name": "fechaRequerida", "type": "STRING", "mode": "NULLABLE"},
        {"name": "fechaEnvio", "type": "STRING", "mode": "NULLABLE"},
        {"name": "idTransportista", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "flete", "type": "FLOAT64", "mode": "NULLABLE"},
    ],
    "empleados": [
        {"name": "idEmpleado", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "nombreEmpleado", "type": "STRING", "mode": "REQUIRED"},
        {"name": "titulo", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ciudad", "type": "STRING", "mode": "REQUIRED"},
        {"name": "pais", "type": "STRING", "mode": "REQUIRED"},
        {"name": "reportaA", "type": "STRING", "mode": "NULLABLE"},
    ], 
    "clientes": [
        {"name": "idCliente", "type": "STRING", "mode": "REQUIRED"},
        {"name": "nombreEmpresa", "type": "STRING", "mode": "REQUIRED"},
        {"name": "nombreContacto", "type": "STRING", "mode": "REQUIRED"},
        {"name": "tituloContacto", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ciudad", "type": "STRING", "mode": "REQUIRED"},
        {"name": "pais", "type": "STRING", "mode": "REQUIRED"},
    ],
    "pedidos_detalles": [
        {"name": "idPedido", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "idProducto", "type": "STRING", "mode": "REQUIRED"},
        {"name": "precioUnitario", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "cantidad", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "descuento", "type": "FLOAT64", "mode": "REQUIRED"},

    ],
    "productos": [
        {"name": "idProducto", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "nombreProducto", "type": "STRING", "mode": "REQUIRED"},
        {"name": "cantidadPorUnidad", "type": "STRING", "mode": "REQUIRED"},
        {"name": "precioUnitario", "type": "STRING", "mode": "REQUIRED"},
        {"name": "descontinuado", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "idCategoria", "type": "INTEGER", "mode": "REQUIRED"},
    ]
}


def fetch_and_load_data_to_bigquery(**kwargs): 
    gc = gspread.service_account(sa_config_path)
    spreadsheet = gc.open_by_url(url)

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=dataset_name, project_id=project_name, if_exists="ignore")
    
    create_dataset.execute(kwargs)

    for table_name, schema in tables_config.items(): 
        worksheet = spreadsheet.worksheet(table_name) 
        index_float_columns = []

        for index, column in enumerate(schema):
            if column["type"] == "FLOAT64":
                index_float_columns.append(index+1)
                
        data = worksheet.get_all_records(numericise_ignore=index_float_columns) 

        create_table = BigQueryCreateEmptyTableOperator(
            task_id=f'create_table_{table_name}',
            dataset_id=dataset_name,
            table_id=table_name,
            schema_fields=schema,
            project_id=project_name, 
            if_exists="ignore", 
        )
        create_table.execute(kwargs)
    
        if data:
            df = pd.DataFrame(data)
            df_split = [df]


            for column in schema:
                if column["type"] == "FLOAT64":
                    df[column["name"]] = df[column["name"]].str.replace(",", ".").astype(float)
                elif column["type"] == "STRING":
                    df[column["name"]] = df[column["name"]].astype(str)

            print(df)

            if len(df) > 1000:
                df_split = np.array_split(df, 3)
            
            for df in df_split:
                INSERT_ROWS_QUERY = f"INSERT `{project_name}.{dataset_name}.{table_name}` VALUES "
                for index, row in df.iterrows():
                    INSERT_ROWS_QUERY += "("
                    for index, column in enumerate(schema):

                        if column['type'] == 'STRING':
                            column_value_scaped_special_characters = row[column['name']].replace("'", "\\'")
                            
                            INSERT_ROWS_QUERY += f"\'{column_value_scaped_special_characters}\'"
                        else:
                            INSERT_ROWS_QUERY += "\'\'" if row[column['name']]== "" else f"{row[column['name']]}"

                        if index == len(schema)-1:
                            INSERT_ROWS_QUERY += f"), "
                        else:
                            INSERT_ROWS_QUERY += f", "
            
                INSERT_ROWS_QUERY = INSERT_ROWS_QUERY[:-2] + ";"

                insert_query_job = BigQueryInsertJobOperator(
                    task_id=f"insert_data_{table_name}",
                    configuration={
                        "query": {
                            "query": INSERT_ROWS_QUERY,
                            "useLegacySql": False,
                            'allowLargeResults': True,
                            "priority": "BATCH", 
                        }
                    }
                )
                insert_query_job.execute(kwargs)

                create_curated_table = BigQueryInsertJobOperator(
                    task_id=f"create_curated_table_{table_name}",
                    configuration={
                        "query": {
                            "query": open(f'/home/airflow/gcs/dags/sql/{table_name}_curated.sql', 'r').read().format(dataset_name=dataset_name, project_name=project_name, table_name=table_name),
                            "useLegacySql": False,
                            "writeDisposition": "WRITE_TRUNCATE", #
                            'destinationTable': {
                                'projectId': project_name,
                                'datasetId': dataset_name,
                                'tableId': f"{table_name}_curated"
                            },
                        }
                    },
                )

                create_curated_table.execute(kwargs)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'google_sheet_to_bigquery',
    default_args=default_args,
    description='Fetch data from Google Spreadsheet and load to BigQuery',
    schedule_interval='0 */2 * * *', # Run every 2 hours
    catchup=False #
) as dag:


    fetch_and_load_data_to_bigquery = PythonOperator(
        task_id='fetch_and_load_data_to_bigquery',
        python_callable=fetch_and_load_data_to_bigquery,
        provide_context=True,
        dag=dag
    )

    create_report1_table = BigQueryInsertJobOperator(
        task_id=f"create_report_table_1",
        configuration={
            "query": {
                "query": open(f'/home/airflow/gcs/dags/sql/reporte1.sql', 'r').read().format(dataset_name=dataset_name, project_name=project_name),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                'destinationTable': {
                    'projectId': project_name,
                    'datasetId': dataset_name,
                    'tableId': "reporte1"
                },
            }
        },
        dag=dag
    )

    create_report2_table = BigQueryInsertJobOperator(
        task_id=f"create_report_table_2",
        configuration={
            "query": {
                "query": open(f'/home/airflow/gcs/dags/sql/reporte2.sql', 'r').read().format(dataset_name=dataset_name, project_name=project_name),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                'destinationTable': {
                    'projectId': project_name,
                    'datasetId': dataset_name,
                    'tableId': "reporte2"
                },
            }
        },
        dag=dag
    )

    create_report3_table = BigQueryInsertJobOperator(
        task_id=f"create_report_table_3",
        configuration={
            "query": {
                "query": open(f'/home/airflow/gcs/dags/sql/reporte3.sql', 'r').read().format(dataset_name=dataset_name, project_name=project_name),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                'destinationTable': {
                    'projectId': project_name,
                    'datasetId': dataset_name,
                    'tableId': "reporte3"
                },
            }
        },
        dag=dag
    )

    create_report4_table = BigQueryInsertJobOperator(
        task_id=f"create_report_table_4",
        configuration={
            "query": {
                "query": open(f'/home/airflow/gcs/dags/sql/reporte4.sql', 'r').read().format(dataset_name=dataset_name, project_name=project_name),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                'destinationTable': {
                    'projectId': project_name,
                    'datasetId': dataset_name,
                    'tableId': "reporte4"
                },
            }
        },
        dag=dag
    )

    fetch_and_load_data_to_bigquery >> create_report1_table >> create_report2_table >> create_report3_table >> create_report4_table