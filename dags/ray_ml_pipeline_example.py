"""
DAG de exemplo de ML Pipeline usando o operador Ray com TaskFlow API.

Demonstra o uso de:
  - @task.ray  (decorator TaskFlow do provider apache-airflow-provider-ray)
  - XCom via bucket (GCS ou S3/MinIO) para passar dados entre tasks
  - Modo deferrable (libera o worker slot durante o job) + fetch_logs

Como usar:
  1. Instale o pacote apache-airflow-provider-ray no Airflow.
  2. Configure a connection Kubernetes (ex.: conn_kubernetes, in_cluster) ou
     a connection customizada `ray` (conn_type="ray") e passe `ray_conn_id`.
  3. Configure o bucket de XCom (variável ray_xcom_backend + XCOM_GCS_BUCKET
     ou XCOM_S3_BUCKET).
  4. Garanta que o kuberay-operator esteja instalado no cluster.
  5. Use uma imagem Ray com as dependências de ML desejadas.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

# Importa o decorator @task.ray (registra task.ray automaticamente).
from airflow.providers.ray.decorators import ray  # noqa: F401

DEFAULT_IMAGE = "rayproject/ray:2.9.0"
RAY_NAMESPACE = "default"

default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ray_ml_pipeline_example",
    description="Pipeline de ML de exemplo usando @task.ray",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "ray", "exemplo"],
) as dag:

    # ──────────────────────────────────────────────
    # 1. Extração de dados (task Python tradicional)
    # ──────────────────────────────────────────────
    @task(task_id="extrai_dados")
    def extrai_dados() -> dict:
        """Gera um dataset de exemplo e retorna metadados."""
        import pandas as pd

        df = pd.DataFrame(
            {
                "feature_a": [1.2, 3.4, 5.6],
                "feature_b": [0.1, 0.2, 0.3],
                "target": [0, 1, 0],
            }
        )
        return {"shape": df.shape, "columns": list(df.columns)}

    # ──────────────────────────────────────────────
    # 2. Feature engineering (executado no cluster Ray)
    # ──────────────────────────────────────────────
    @task.ray(
        task_id="feature_engineering_ray",
        namespace=RAY_NAMESPACE,
        image=DEFAULT_IMAGE,
        worker_replicas=1,
        shutdown_after_job_finishes=True,
        deferrable=True,   # libera o worker slot durante o job
        fetch_logs=True,   # captura logs do submitter em caso de falha
    )
    def feature_engineering_ray(dados_extraidos: dict) -> dict:
        """
        Executa dentro do cluster Ray via RayJob CRD.
        ATENÇÃO: imports devem estar dentro da função.
        """
        import ray

        ray.init(address="auto", ignore_reinit_error=True)

        @ray.remote
        def compute_features(shape):
            # Lógica distribuída real viria aqui.
            return {"n_features": shape[1], "n_rows": shape[0]}

        result = ray.get(compute_features.remote(dados_extraidos.get("shape")))
        return result

    # ──────────────────────────────────────────────
    # 3. Treinamento (executado no cluster Ray)
    # ──────────────────────────────────────────────
    @task.ray(
        task_id="treina_modelo_ray",
        namespace=RAY_NAMESPACE,
        image=DEFAULT_IMAGE,
        worker_replicas=2,
        shutdown_after_job_finishes=True,
        deferrable=True,
        fetch_logs=True,
    )
    def treina_modelo_ray(features: dict) -> dict:
        """Treina um modelo distribuído no cluster Ray."""
        import ray

        ray.init(address="auto", ignore_reinit_error=True)

        @ray.remote
        def train(n_features):
            # Simula treinamento distribuído.
            return {"accuracy": 0.95, "n_features": n_features}

        result = ray.get(train.remote(features.get("n_features")))
        return result

    # ──────────────────────────────────────────────
    # 4. Promoção do modelo (task Python tradicional)
    # ──────────────────────────────────────────────
    @task(task_id="promove_modelo")
    def promove_modelo(treino_result: dict) -> str:
        """Registra o modelo treinado."""
        print(f"Resultado do treinamento: {treino_result}")
        return f"promoted:{treino_result.get('accuracy')}"

    # ──────────────────────────────────────────────
    # Dependências (TaskFlow API resolve tudo sozinha)
    # ──────────────────────────────────────────────
    dados = extrai_dados()
    features = feature_engineering_ray(dados)
    treino = treina_modelo_ray(features)
    promove_modelo(treino)
