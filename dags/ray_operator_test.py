"""
DAG de teste do RayJobOperator (apache-airflow-provider-ray).

Testa o operador de ponta a ponta no cluster kind + kuberay-operator + MinIO,
usando a imagem customizada `ray-airflow-provider:2.35.0` (Python 3.12,
alinhada ao host que serializa a função).

Requisitos de configuração no Airflow:
  - Variável `ray_xcom_backend` = "s3"
  - Variável `xcom_s3_bucket` = "ray-xcom-test" (ou usar XCOM_S3_BUCKET env)
  - Connection Kubernetes (`kubernetes_default`) apontando para o cluster
    kind (kubeconfig), ou `in_cluster=True` se o Airflow rodar dentro do cluster.
  - A imagem `kind-registry:5000/ray-airflow-provider:2.35.0` deve estar
    acessível pelo cluster (registry local integrado ao kind).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

# Registra o decorator @task.ray (provider apache-airflow-provider-ray).
from airflow.providers.ray.decorators import ray  # noqa: F401

# Imagem customizada com o bootstrap do provider (Python 3.12, alinhada ao host).
# Usa o registry local integrado ao kind.
RAY_IMAGE = "kind-registry:5000/ray-airflow-provider:2.35.0"
RAY_VERSION = "2.35.0"
RAY_NAMESPACE = "orchestrator"

# Configuração do backend de XCom (S3/MinIO).
# O bucket e as credenciais são lidos de variáveis do Airflow ou env vars.
RAY_ENV = {
    "RAY_XCOM_BACKEND": "s3",
    "XCOM_S3_BUCKET": "ray-xcom-test",
    "MINIO_ENDPOINT": "https://minio-api.apps.172-18-0-100.nip.io",
    "AWS_ACCESS_KEY_ID": "root",
    "AWS_SECRET_ACCESS_KEY": "ciGINTlAcysRpUQq",
    "RAY_XCOM_VERIFY_SSL": "false",
}

default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ray_operator_test",
    description="Teste do RayJobOperator end-to-end (kind + MinIO)",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ray", "teste", "operator"],
) as dag:

    @task(task_id="prepara_dados")
    def prepara_dados() -> dict:
        """Gera os dados de entrada para a tarefa Ray."""
        return {"x": 42, "y": "hello from airflow dag"}

    @task.ray(
        task_id="executa_no_ray",
        namespace=RAY_NAMESPACE,
        image=RAY_IMAGE,
        ray_version=RAY_VERSION,
        worker_replicas=1,
        shutdown_after_job_finishes=True,
        ttl_seconds_after_finished=60,
        env=RAY_ENV,
        deferrable=False,  # síncrono para o teste
        fetch_logs=True,
    )
    def executa_no_ray(dados: dict) -> dict:
        """
        Executa dentro do cluster Ray via RayJob CRD.
        ATENÇÃO: imports devem estar dentro da função.
        """
        import ray

        @ray.remote
        def compute(a, b):
            return {"result": a * 2, "message": b}

        result = ray.get(compute.remote(dados["x"], dados["y"]))
        return result

    @task(task_id="valida_resultado")
    def valida_resultado(resultado: dict) -> str:
        """Valida o resultado retornado pelo RayJob."""
        print(f"Resultado do RayJob: {resultado}")
        assert resultado["result"] == 84, (
            f"Esperado 84, recebeu {resultado['result']}"
        )
        assert resultado["message"] == "hello from airflow dag"
        print("[OK] Teste do RayJobOperator PASSOU!")
        return "PASSOU"

    dados = prepara_dados()
    resultado = executa_no_ray(dados)
    valida_resultado(resultado)
