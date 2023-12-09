import logging

import pandas as pd
from airflow import Dataset
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

# from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from astro import sql as aql
from utils.constants import default_args


@dag(
    dag_id="monitoring_feature",
    default_args=default_args,
    catchup=False,
    schedule=[Dataset("prediction_data")],
    default_view="graph",
    render_template_as_native_obj=True,
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Monitoring"],
)
def feature_monitoring():
    @aql.dataframe
    def get_ref_data(**context):
        from sklearn.datasets import load_iris

        # Load the data
        iris = load_iris(as_frame=True)
        data = pd.DataFrame(iris.data)
        logging.info(data.head())
        return data

    @aql.dataframe
    def get_curr_data(**context):
        feature_df = context["ti"].xcom_pull(
            dag_id="feaure_engineering", task_ids="feature_eng", include_prior_dates=True
        )
        return pd.concat([feature_df["X_test"], feature_df["y_test"]], axis=1)

    @aql.dataframe(columns_names_capitalization="lower")
    def generate_reports(ref_data: pd.DataFrame, curr_data: pd.DataFrame):
        from evidently.test_preset import (
            DataDriftTestPreset,
        )
        from evidently.test_suite import TestSuite

        suite = TestSuite(
            tests=[
                # NoTargetPerformanceTestPreset(),
                DataDriftTestPreset(),
                # DataStabilityTestPreset()
            ]
        )
        suite.run(reference_data=ref_data, current_data=curr_data)

        return suite.as_dict()

    ref_data = get_ref_data()
    curr_data = get_curr_data()

    reports = generate_reports(ref_data=ref_data, curr_data=curr_data)

    send_report = EmptyOperator(task_id="send_alert")
    # send_report = SlackAPIPostOperator(
    #     slack_conn_id="slack_default",
    #     task_id="send_alert",
    #     text="""
    #     *Evidently Test Suite results:*
    #     ```{report}```
    #     """.format(
    #         report="{{ ti.xcom_pull(task_ids='generate_reports') }}"
    #     ),
    #     channel="#integrations",
    # )

    @task.short_circuit
    def check_drift(metrics: str):
        status = metrics["tests"][0]["status"]
        logging.info(status)
        if status == "FAIL":
            return True

    send_retrain_alert = EmptyOperator(task_id="send_retrain_alert")
    # send_retrain_alert = SlackAPIPostOperator(
    #     slack_conn_id="slack_default",
    #     task_id="send_retrain_alert",
    #     text="""
    #     *Warning:* Retrain was triggered because of data drift conditions.
    #     {description}
    #     """.format(
    #         description="{{ ti.xcom_pull(task_ids='generate_reports')['tests'][0]['description']
    # }}"
    #     ),
    #     channel="#integrations",
    # )

    trigger_retrain = TriggerDagRunOperator(task_id="trigger_retrain", trigger_dag_id="train_model")

    # cleanup = aql.cleanup()
    chain(
        reports,
        check_drift(metrics="{{ ti.xcom_pull(task_ids='generate_reports') }}"),
        trigger_retrain,
        send_retrain_alert,
    )

    reports >> send_report


feature_monitoring = feature_monitoring()
