from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from utils.constants import default_args
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
import matplotlib.pyplot as plt
import seaborn as sns


def metricas(y_test, y_predict):
    acuracia = accuracy_score(y_test, y_predict)
    precision = precision_score(y_test, y_predict, average="weighted")
    recall = recall_score(y_test, y_predict, average="weighted")
    f1 = f1_score(y_test, y_predict, average="weighted")
    return acuracia, precision, recall, f1


def matriz_confusao(y_test, y_predict):
    matriz_conf = confusion_matrix(y_test.values.ravel(), y_predict)
    fig = plt.figure()
    ax = plt.subplot()
    sns.heatmap(matriz_conf, annot=True, cmap="Blues", ax=ax)

    ax.set_xlabel("Valor Predito")
    ax.set_ylabel("Valor Real")
    ax.set_title("Matriz de Confusão")
    ax.xaxis.set_ticklabels(["Classe 1", "Classe 2", "Classe 3"])
    ax.yaxis.set_ticklabels(["Classe 1", "Classe 2", "Classe 3"])
    plt.close()
    return fig


FILE_PATH = "data.parquet"

# AWS S3 parameters
AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"
FILE_TO_SAVE_PREDICTIONS = "iris_predictions.csv"

# MLFlow parameters
MLFLOW_CONN_ID = "conn_mlflow"
EXPERIMENT_NAME = "poc"
REGISTERED_MODEL_NAME = "modelIris"
MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS = 1000

# Data parameters
TARGET_COLUMN = "target"


@dag(
    dag_id="teste_1",
    default_args=default_args,
    catchup=False,
    schedule=None,
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Train"],
)
def train():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def fetch_feature_test_df(**context):
        "Fetch the feature dataframe from the feature engineering DAG."
        feature_df = context["ti"].xcom_pull(
            dag_id="feaure_engineering", task_ids="feature_eng", include_prior_dates=True
        )
        print(feature_df["X_test"].head())
        return feature_df["X_test"]

    @task
    def fetch_feature_target_df(**context):
        "Fetch the feature dataframe from the feature engineering DAG."
        feature_df = context["ti"].xcom_pull(
            dag_id="feaure_engineering", task_ids="feature_eng", include_prior_dates=True
        )
        print(feature_df["y_test"].head())
        return feature_df["y_test"]

    @task
    def fetch_model_run_id(**context):
        model_run_id = context["ti"].xcom_pull(
            dag_id="train_model", task_ids="train_model", include_prior_dates=True
        )
        return model_run_id

    fetch_feature_test = fetch_feature_test_df()
    fetch_feature_target = fetch_feature_target_df()
    fetched_model_run_id = fetch_model_run_id()

    (start >> [fetch_feature_test, fetched_model_run_id, fetch_feature_target] >> end)


train()
