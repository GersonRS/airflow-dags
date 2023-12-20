import os
import pandas as pd
from airflow import Dataset
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from mlflow_provider.hooks.client import MLflowClientHook
from astro import sql as aql
from astro.files import File
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
import matplotlib.pyplot as plt
import seaborn as sns
from utils.constants import default_args

# AWS S3 parameters
AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"
MLFLOW_CONN_ID = "conn_mlflow"
# Data parameters
TARGET_COLUMN = "target"
FILE_TO_SAVE_PREDICTIONS = "iris_predictions.csv"


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


@dag(
    dag_id="predict",
    default_args=default_args,
    catchup=False,
    schedule=[Dataset("model_trained")],
    default_view="graph",
    render_template_as_native_obj=True,
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Predict"],
)
def predict():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset("prediction_data")])

    @task
    def fetch_feature_df_test(**context):
        feature_df = context["ti"].xcom_pull(
            dag_id="feaure_engineering",
            task_ids="feature_eng",
            include_prior_dates=True,
        )
        return feature_df["X_test"]

    @task
    def fetch_target_test(**context):
        feature_df = context["ti"].xcom_pull(
            dag_id="feaure_engineering",
            task_ids="feature_eng",
            include_prior_dates=True,
        )
        return feature_df["y_test"]

    @task
    def fetch_model_run_id(**context):
        model_run_id = context["ti"].xcom_pull(
            dag_id="train_model", task_ids="train_model", include_prior_dates=True
        )
        return model_run_id

    @task
    def fetch_experiment_id(**context):
        experiment_id = context["ti"].xcom_pull(
            dag_id="train_model",
            task_ids="fetch_experiment_id",
            include_prior_dates=True,
        )
        return experiment_id

    fetched_feature_df = fetch_feature_df_test()
    fetched_model_run_id = fetch_model_run_id()
    fetched_experiment_id = fetch_experiment_id()

    @task
    def add_line_to_file(run_id: str, experiment_id: str):
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        file_contents = s3_hook.read_key(
            key=f"{experiment_id}/{run_id}/artifacts/model/requirements.txt",
            bucket_name=MLFLOW_ARTIFACT_BUCKET,
        )
        if "boto3" not in file_contents:
            updated_contents = file_contents + "\nboto3" + "\npandas"
            s3_hook.load_string(
                updated_contents,
                key=f"{experiment_id}/{run_id}/artifacts/model/requirements.txt",
                bucket_name=MLFLOW_ARTIFACT_BUCKET,
                replace=True,
            )

    @aql.dataframe()
    def prediction(data, run_id):
        import mlflow

        logged_model = f"runs:/{run_id}/model"
        loaded_model = mlflow.pyfunc.load_model(logged_model)
        result = loaded_model.predict(pd.DataFrame(data))
        return pd.DataFrame(result, columns=["Predictions"], index=range(len(result)))

    run_prediction = prediction(fetched_feature_df, fetched_model_run_id)

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
    def plot_predictions(y_test, y_pred, run_id):
        import mlflow
        import matplotlib.pyplot as plt

        # Create a figure and axes
        fig, ax = plt.subplots(figsize=(10, 6))

        # Plot the prediction column in blue
        ax.plot(
            y_pred.index,
            y_pred["Predictions"],
            color="#1E88E5",
            label="Predicted target",
        )

        # Plot the target column in green
        ax.plot(y_test.index, y_test["target"], color="#004D40", label="True target")

        # Set the title and labels
        ax.set_title("Predicted vs True target")
        ax.set_xlabel("Target")
        ax.set_ylabel("Predict")

        # Add a legend
        ax.legend(loc="lower right")

        os.makedirs(os.path.dirname("include/plots/"), exist_ok=True)
        # Save the plot as a PNG file
        plt.savefig("include/plots/iris.png")
        plt.close()
        with mlflow.start_run(run_id=run_id):
            mlflow.log_artifact("include/plots/iris.png", "iris-plots")

    target_data = fetch_target_test()

    pred_file = aql.export_file(
        task_id="save_predictions",
        input_data=run_prediction,
        output_file=File(
            os.path.join("s3://", DATA_BUCKET_NAME, FILE_TO_SAVE_PREDICTIONS),
            conn_id=AWS_CONN_ID,
        ),
        if_exists="replace",
    )

    (
        start
        >> add_line_to_file(
            run_id=fetched_model_run_id, experiment_id=fetched_experiment_id
        )
        >> [
            metrics(
                y_test=target_data, y_pred=run_prediction, run_id=fetched_model_run_id
            ),
            plot_predictions(
                y_test=target_data, y_pred=run_prediction, run_id=fetched_model_run_id
            ),
        ]
        >> pred_file
        >> end
    )


predict()
