#
# Author: GersonRS
# Email: gersonrodriguessantos8@gmail.com
#
"""
Este é um exemplo de DAG que usa SparkKubernetesOperator e SparkKubernetesSensor.
Neste exemplo, crio duas tarefas que são executadas sequencialmente.
A primeira tarefa é enviar sparkApplication no cluster Kubernetes.
E a segunda tarefa é verificar o estado final do sparkApplication que enviou
no primeiro estado.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

# [INICIO import_module]
# O decorator dag; precisaremos disso para instanciar um DAG
# Operadores; precisamos disso para funcionar!

# [FIM import_module]
# Documentação baseada em Markdown que serão renderizados nas páginas Grid , Graph e Calendar.
doc_md_DAG = """
# DAG Entrega dos dados que vem do nb_carga_contaminantes para o Snowflake

Este é um exemplo de DAG que usa SparkKubernetesOperator e SparkKubernetesSensor.
Neste exemplo, crio duas tarefas que são executadas sequencialmente.
A primeira tarefa é enviar sparkApplication no cluster Kubernetes.
E a segunda tarefa é verificar o estado final do sparkApplication que enviou no primeiro estado.

## Objetivo desta DAG

* Processar todos os dados da curated zone referentes aos contaminantes e passar para o snowflake

Execute para testar.
"""

# [INICIO default_args]
# Esses argumentos serão basicamete repassados para cada operador
# Você pode substituí-los pelos valores que quiser durante a inicialização do operador
default_args = {
    "owner": "GersonRS",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["gerson.santos@owshq.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(1),
}
# [FIM default_args]


# [INICIO dag]
@dag(
    dag_id="delivery-data-from-nb-carga-contaminantes-to-snowflake",
    default_args=default_args,
    catchup=False,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=["spark", "kubernetes", "sensor", "snowflake", "minio", "s3"],
    doc_md=doc_md_DAG,
)
def delivery_data_from_nb_carga_contaminantes_to_snowflake_dag() -> None:
    """
    `delivery_data_from_nb_carga_contaminantes_to_snowflake_dag()` é uma função que define um DAG
    (Directed Gráfico acíclico) no Apache Airflow. Este DAG é responsável por ingerir
    dados do do minio para o Snowflake. Consiste em duas tarefas:
    """

    # [INICIO set_tasks]

    # A variável(task) `submit` está criando uma instância da classe
    # `SparkKubernetesOperator`. Esse operador é responsável por enviar um
    # `SparkApplication` para execução em um cluster Kubernetes. Atravez da definição
    # de yaml para acionar o processo, usando o spark-on-k8s para operar com base nos
    # dados e criando um `SparkApplication` em contêiner.
    submit = SparkKubernetesOperator(
        task_id="delivery_data_from_nb_carga_contaminantes_to_snowflake_submit",
        namespace="processing",
        application_file="spark_jobs/delivery_data_from_nb_carga_contaminantes_to_snowflake.yaml",
        kubernetes_conn_id="kubernetes_in_cluster",
        do_xcom_push=True,
        # O parâmetro `params` no `SparkKubernetesOperator` é usado para passar parâmetros
        # adicionais para o `SparkApplication` que será executado no cluster Kubernetes.
        # Esses parâmetros podem ser acessados no código do aplicativo Spark.
        params={
            "mainApplicationFile": "s3a://scripts/curated/agroindustrial/nb_carga_contaminantes.py",
            "job_name": "delivery-data-from-nb-carga-contaminantes-to-snowflake-{{ ts_nodash | lower }}-{{ task_instance.try_number }}",  # noqa: E501
            "load_type": "overwrite",
            "owner": "DVRY_AGROINDUSTRIAL",
            "table": "CONTAMINANTES",
            "minio_bucket": "curated",
            "url_snowflake": "slcagricola.east-us-2.azure.snowflakecomputing.com",
            "database_snowflake": "SLC",
            "warehouse_snowflake": "SLC_ALL",
            "role_snowflake": "SLC_ALL",
        },
        doc_md="""
        ### Proposta desta tarefa

        * Ser responsável por enviar um `SparkApplication` para execução em um cluster Kubernetes.

        * Definir um yaml para acionar o processo, usando o spark-on-k8s para operar com base nos
        dados e criando um `SparkApplication` em contêiner.
        """,
    )

    # A variável(task) `sensor` está criando uma instância da classe
    # `SparkKubernetesSensor`. Este sensor é responsável por monitorar o status de um
    # `SparkApplication` em execução em um cluster Kubernetes. Usando o sensor para ler
    # e visualizar o resultado do `SparkApplication`, lê do xcom e verifica o par de
    # status [chave e valor] do `submit`, contenco o nome do `SparkApplication` e
    # passando para o `SparkKubernetesSensor`.
    sensor = SparkKubernetesSensor(
        task_id="delivery_data_from_nb_carga_contaminantes_to_snowflake_sensor",
        namespace="processing",
        application_name="{{task_instance.xcom_pull(task_ids='delivery_data_from_nb_carga_contaminantes_to_snowflake_submit')['metadata']['name']}}",  # noqa: E501
        kubernetes_conn_id="kubernetes_in_cluster",
        attach_log=True,
        doc_md="""
        ### Proposta desta tarefa

        * Ser responsável por monitorar o status de um `SparkApplication` em execução em um cluster
        Kubernetes.

        * Usar o sensor para ler e visualizar o resultado do `SparkApplication`.

        * Ler do xcom e verifica o par de status [chave e valor] do `submit`, contenco o nome do
        `SparkApplication` e passando para o `SparkKubernetesSensor`.
        """,
    )
    # [FIM set_tasks]

    # [INICIO task_sequence]
    # `submit >> sensor` está definindo a dependência entre a tarefa `submit` e a
    # tarefa `sensor`. Isso significa que a tarefa `sensor` só começará a ser executada
    # após a tarefa `submit` for concluída com sucesso.
    submit >> sensor
    # [FIM task_sequence]


# [FIM dag]

# [INICIO start_dag]
# `delivery-data-from-nb-carga-contaminantes-to-snowflake_dag()` está criando uma instância da DAG
# `delivery-data-from-sap-hana-to-kafka`. Esta função(instância) pode ser usada para
# iniciar a execução da DAG no Apache Airflow.
delivery_data_from_nb_carga_contaminantes_to_snowflake_dag()
# [FIM start_dag]
