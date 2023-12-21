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
from datetime import timedelta

# [INICIO import_module]
# O decorator dag; precisaremos disso para instanciar um DAG
from airflow.decorators import dag

# Operadores; precisamos disso para funcionar!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

# [FIM import_module]

# [INICIO default_args]
# Esses argumentos serão basicamete repassados para cada operador
# Você pode substituí-los pelos valores que quiser durante a inicialização do operador
default_args = {
    "owner": "GersonRS",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["gersonrodriguessantos8@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 1,
    "retry_delay": timedelta(1),
}
# [FIM default_args]


# [INICIO dag]
@dag(
    dag_id="delivery-data-from-sap-hana-to-kafka",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once",
    default_view="graph",
    tags=["spark", "kubernetes", "sensor", "kafka", "sap", "hana"],
)
def delivery_data_from_sap_hana_to_kafka_dag() -> None:
    """
    `delivery_data_from_sap_hana_to_kafka_dag()` é uma função que define um DAG
    (Directed Gráfico acíclico) no Apache Airflow. Este DAG é responsável por ingerir
    dados do SAP HANA para Kafka. Consiste em duas tarefas:
    """

    # [INICIO set_tasks]

    # A variável(task) `submit` está criando uma instância da classe
    # `SparkKubernetesOperator`. Esse operador é responsável por enviar um
    # `SparkApplication` para execução em um cluster Kubernetes. Atravez da definição
    # de yaml para acionar o processo, usando o spark-on-k8s para operar com base nos
    # dados e criando um `SparkApplication` em contêiner.
    submit = SparkKubernetesOperator(
        task_id="delivery_data_from_sap_hana_to_kafka_submit",
        namespace="processing",
        application_file="spark_jobs/delivery_data_from_silver_to_gold.yaml",
        kubernetes_conn_id="conn_kubernetes",
        do_xcom_push=True,
    )

    # A variável(task) `sensor` está criando uma instância da classe
    # `SparkKubernetesSensor`. Este sensor é responsável por monitorar o status de um
    # `SparkApplication` em execução em um cluster Kubernetes. Usando o sensor para ler
    # e visualizar o resultado do `SparkApplication`, lê do xcom e verifica o par de
    # status [chave e valor] do `submit`, contenco o nome do `SparkApplication` e
    # passando para o `SparkKubernetesSensor`.
    sensor = SparkKubernetesSensor(
        task_id="delivery_data_from_sap_hana_to_kafka_sensor",
        namespace="processing",
        application_name="{{task_instance.xcom_pull(task_ids='delivery_data_from_sap_hana_to_kafka_submit')['metadata']['name']}}",  # noqa: E501
        kubernetes_conn_id="conn_kubernetes",
        attach_log=True,
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
# `delivery_data_from_sap_hana_to_kafka_dag()` está criando uma instância da DAG
# `delivery-data-from-sap-hana-to-kafka`. Esta função(instância) pode ser usada para
# iniciar a execução da DAG no Apache Airflow.
delivery_data_from_sap_hana_to_kafka_dag()
# [FIM start_dag]
