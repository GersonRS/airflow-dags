"""
### Ingestion Pipeline with SAP HANA

"""
import airflow
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
import platform
from hdbcli import dbapi

# from astro import sql as aql

default_args = {
    "owner": "gerson.santos",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email": ["gerson.santos@owshq.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(1),
}


@dag(
    dag_id="ingestion_sap_hana",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once",
    default_view="graph",
    tags=["development", "sap", "hana", "python", "sap-hana", "ingestion", "pipeline"],
)
def ingestion_get_sap_hana_from_datalake_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def create_connection_sap_hana():
        # verify the architecture of Python
        print("Platform architecture: " + platform.architecture()[0])
        try:
            # Initialize your connection
            conn = dbapi.connect(
                address="10.158.2.40",
                port="39015",
                user="SYNAPSE_READ",
                password="$Lc@2020!Syn@ps322",
            )
            return conn
        except dbapi.Error as er:
            print("Connect failed, exiting")
            print(er)
            exit()

    @task
    def get_data_sap_hana(conn):
        # If no errors, print connected
        print("connected")

        cursor = conn.cursor()
        sql_command = "SELECT * FROM MAD;"
        cursor.execute(sql_command)

        rows = cursor.fetchall()
        for row in rows:
            for col in row:
                print("%s" % col, end=" ")
            print("  ")
        cursor.close()
        print("\n")
        cursor.close()
        conn.close()

    @task
    def get_data_sap_hana_with_sqlalchemi():
        from sqlalchemy import create_engine, select

        engine = create_engine("hana://SYNAPSE_READ:$Lc@2020!Syn@ps322@10.158.2.40:30015")
        stmt = select("mad")
        print(stmt)
        with engine.connect() as conn:
            for row in conn.execute(stmt):
                print(row)

    # save_data_to_other_s3 = aql.export_file(
    #     task_id="save_data_to_s3",
    #     input_data=extracted_df,
    #     output_file=File(
    #         path=os.path.join("s3://", DATA_BUCKET_NAME, FILE_PATH), conn_id=AWS_CONN_ID
    #     ),
    #     if_exists="replace",
    # )

    connection = create_connection_sap_hana()
    get_data = get_data_sap_hana(connection)

    start >> [get_data, get_data_sap_hana_with_sqlalchemi] >> end


ingestion_get_sap_hana_from_datalake_dag()
