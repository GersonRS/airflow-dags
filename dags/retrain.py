"""
### Track and Register Models with MLflow

Uses the MLflow provider package's CreateRegisteredModelOperator, CreateModelVersionOperator, and TransitionModelVersionStageOperator to create a new model version in the MLflow model registry and transition it to the "Staging" stage.
"""
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.helpers import chain
from astro import sql as aql
from astro.sql.table import Metadata, Table
from mlflow_provider.hooks.base import MLflowBaseHook
from mlflow_provider.operators.deployment import CreateDeploymentOperator, PredictOperator
from mlflow_provider.operators.pyfunc import ModelLoadAndPredictOperator
from mlflow_provider.operators.registry import (
    CreateModelVersionOperator,
    CreateRegisteredModelOperator,
    TransitionModelVersionStageOperator,
)
from pandas import DataFrame
from pendulum import datetime
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split


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


experiment_name = "retrain"

test_sample = {
    "data": [
        [5.1, 3.5, 1.4, 0.2],
        [4.9, 3.0, 1.4, 0.2],
        [4.7, 3.2, 1.3, 0.2],
    ],
    "columns": ["sepal_length_cm", "sepal_width_cm", "petal_length_cm", "petal_width_cm"],
}

MLFLOW_ARTIFACT_BUCKET = "mlflow"


@dag(
    start_date=datetime(2022, 1, 1),
    schedule=None,
    default_args={"mlflow_conn_id": "mlflow_default"},
    tags=["example"],
    default_view="graph",
    catchup=False,
    doc_md=__doc__,
)
def retrain():
    @task
    def get_data(**context):
        "Fetch the feature dataframe from the feature engineering DAG."

        feature_df = context["ti"].xcom_pull(
            dag_id="feature_eng", task_ids="build_features", include_prior_dates=True
        )
        return feature_df

    @aql.dataframe(columns_names_capitalization="original")
    def retrain(iris: DataFrame):
        "Train a model and log it to MLFlow."
        import mlflow
        from sklearn.metrics import accuracy_score, log_loss

        mlflow_hook = MLflowBaseHook(mlflow_conn_id="mlflow_astronomer_dev")
        mlflow_hook._set_env_variables()

        # Creating an experiment and continue if it already exists
        try:
            mlflow.create_experiment(experiment_name)
        except:
            pass

        # Setting the environment with the created experiment
        experiment = mlflow.set_experiment(experiment_name)

        mlflow.sklearn.autolog()

        X = iris.copy()
        y = X.pop("target")

        x_train, x_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=1, stratify=y
        )

        model = LogisticRegression()

        with mlflow.start_run(run_name="retrain"):
            model.fit(x_train, y_train)
            y_pred = model.predict(x_test)

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
            mlflow.log_metric("loss", log_loss(y_test, y_pred))

            # Registro do modelo
            mlflow.sklearn.log_model(model, "model")

            run_id = mlflow.active_run().info.run_id
            artifact_location = mlflow.get_experiment(experiment.experiment_id).artifact_location

            return {
                "experiment_id": experiment.experiment_id,
                "run_id": run_id,
                "artifact_location": artifact_location,
                "metrics": {"accuracy": acuracia, "prescision": precision},
            }

    retrain_info = retrain(get_data())

    send_alert = EmptyOperator(task_id="send_alert")
    # send_alert = SlackAPIPostOperator(
    #     slack_conn_id="slack_default",
    #     task_id="send_alert",
    #     text="Warning: Model accuracy has dropped to {{ ti.xcom_pull(task_ids='retrain')['metrics']['accuracy'] }}",
    #     channel="#integrations",
    # )

    @task.branch
    def choose_branch(result):
        if float(result) > 0.90:
            return ["create_registered_model"]
        return ["send_alert"]

    branch_choice = choose_branch(
        result="{{ ti.xcom_pull(task_ids='retrain')['metrics']['accuracy'] }}"
    )

    create_registered_model = CreateRegisteredModelOperator(
        task_id="create_registered_model",
        name=experiment_name,
        tags=[{"key": "name1", "value": "value1"}, {"key": "name2", "value": "value2"}],
        description="ML Ops Offsite Demo",
    )
    create_registered_model.doc_md = "This task is just in case for a first run and to make sure there is always a registry to add model version to."

    create_model_version = CreateModelVersionOperator(
        task_id="create_model_version",
        name=experiment_name,
        source=f"{retrain_info['artifact_location']}/model",
        run_id=retrain_info["run_id"],
        trigger_rule="none_skipped",
    )

    transition_model = TransitionModelVersionStageOperator(
        task_id="transition_model",
        name=experiment_name,
        version="{{ ti.xcom_pull(task_ids='create_model_version')['model_version']['version'] }}",
        stage="Staging",
        archive_existing_versions=True,
    )

    create_deployment = EmptyOperator(task_id="create_deployment")
    # create_deployment = CreateDeploymentOperator(
    #     task_id="create_deployment",
    #     name="mlops-offsite-deployment-{{ ds_nodash }}",
    #     model_uri="{{ ti.xcom_pull(task_ids='transition_model')['model_version']['source'] }}",
    #     target_uri="sagemaker:/us-east-2",
    #     target_conn_id="aws_default",
    #     config={
    #         "image_url": "{{ var.value.mlflow_pyfunc_image_url }}",
    #         "execution_role_arn": "{{ var.value.sagemaker_execution_arn }}",
    #     },
    #     flavor="python_function",
    # )

    test_prediction = ModelLoadAndPredictOperator(
        mlflow_conn_id="mlflow",
        task_id="test_prediction",
        model_uri=f"s3://{MLFLOW_ARTIFACT_BUCKET}/"
        + "{{ ti.xcom_pull(task_ids='retrain')['run_id']}}"
        + "/artifacts/model",
        data=DataFrame(
            data=test_sample["data"],
            columns=test_sample["columns"],
        ),
    )
    # test_prediction = PredictOperator(
    #     task_id="test_prediction",
    #     target_uri="sagemaker:/us-east-2",
    #     target_conn_id="aws_default",
    #     deployment_name="{{ ti.xcom_pull(task_ids='create_deployment')['name'] }}",
    #     inputs=DataFrame(data=test_sample["data"], columns=test_sample["columns"]),
    # )

    chain(
        create_registered_model,
        create_model_version,
        transition_model,
        create_deployment,
        test_prediction,
    )
    retrain_info >> Label("Run ID & Artifact Location") >> create_model_version

    retrain_info >> branch_choice >> [create_registered_model, send_alert]


retrain = retrain()
