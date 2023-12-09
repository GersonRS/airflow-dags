import os
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from astro import sql as aql
import pandas as pd
from astro.files import File
from mlflow_provider.operators.pyfunc import ModelLoadAndPredictOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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

    @task
    def add_line_to_file(**context):
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        file_contents = s3_hook.read_key(
            key=context["ti"].xcom_pull(task_ids="fetch_model_run_id")
            + "/artifacts/model/requirements.txt",
            bucket_name=MLFLOW_ARTIFACT_BUCKET,
        )
        updated_contents = file_contents + "\nboto3" + "\npandas"
        s3_hook.load_string(
            updated_contents,
            key=context["ti"].xcom_pull(task_ids="fetch_model_run_id")
            + "/artifacts/model/requirements.txt",
            bucket_name=MLFLOW_ARTIFACT_BUCKET,
            replace=True,
        )

    run_prediction = ModelLoadAndPredictOperator(
        mlflow_conn_id="mlflow_default",
        task_id="run_prediction",
        model_uri=f"s3://{MLFLOW_ARTIFACT_BUCKET}/"
        + "{{ ti.xcom_pull(task_ids='fetch_model_run_id')}}"
        + "/artifacts/model",
        data=fetch_feature_test,
    )

    @aql.dataframe()
    def list_to_dataframe(column_data):
        df = pd.DataFrame(column_data, columns=["Predictions"], index=range(len(column_data)))
        return df

    @aql.dataframe()
    def metrics(y_test, y_pred, run_id):
        import mlflow

        with mlflow.start_run(run_id=run_id):
            # Métricas
            acuracia, precision, recall, f1 = metricas(y_test, y_pred)
            # Matriz de confusão
            matriz_conf = matriz_confusao(y_test, y_pred)
            temp_name = "confusion-matrix.png"
            matriz_conf.savefig(temp_name)
            mlflow.log_artifact(temp_name, "confusion-matrix-plots")
            try:
                os.remove(temp_name)
            except FileNotFoundError:
                print(f"{temp_name} file is not found")

            # Registro dos parâmetros e das métricas
            mlflow.log_metric("Acuracia", acuracia)
            mlflow.log_metric("Precision", precision)
            mlflow.log_metric("Recall", recall)
            mlflow.log_metric("F1-Score", f1)

    @task
    def plot_predictions(predictions, df):
        import matplotlib.pyplot as plt

        # Create a figure and axes
        fig, ax = plt.subplots(figsize=(10, 6))

        # Plot the prediction column in blue
        ax.plot(
            predictions.index,
            predictions["Predictions"],
            color="#1E88E5",
            label="Predicted tail length",
        )

        # Plot the target column in green
        ax.plot(df.index, df["taill"], color="#004D40", label="True tail length")

        # Set the title and labels
        ax.set_title("Predicted vs True Possum Tail Lengths")
        ax.set_xlabel("Tail length")
        ax.set_ylabel("Animal number")

        # Add a legend
        ax.legend(loc="lower right")

        os.makedirs(os.path.dirname("include/plots/"), exist_ok=True)

        # Save the plot as a PNG file
        plt.savefig("include/plots/iris.png")
        plt.close()

    prediction_data = list_to_dataframe(run_prediction.output)

    pred_file = aql.export_file(
        task_id="save_predictions",
        input_data=prediction_data,
        output_file=File(os.path.join("s3://", DATA_BUCKET_NAME, FILE_TO_SAVE_PREDICTIONS)),
        if_exists="replace",
    )

    (start >> [fetch_feature_test, fetched_model_run_id, fetch_feature_target] >> end)


train()
