"""
### Feature Monitoring with MLflow

Uses Evidently to monitor the features in the feature store after features are added to the feature store by the feature engineering DAG.
If drift is detected, a Slack notification is sent and a DAG is triggered to retrain the model.
"""

import logging

import pandas as pd
from airflow import Dataset
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

# from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from astro import sql as aql
from astro.sql.table import Metadata, Table
from pendulum import datetime


@dag(
    start_date=datetime(2022, 1, 1),
    schedule=[Dataset("prediction_data")],
    tags=["example"],
    default_view="graph",
    catchup=False,
    doc_md=__doc__,
    render_template_as_native_obj=True,
)
def feature_monitoring():
    @aql.dataframe
    def get_ref_data(**context):
        import pandas as pd
        from sklearn.datasets import load_iris

        # Load the data
        iris = load_iris(as_frame=True)
        data = pd.DataFrame(iris.data)
        return data

    @aql.dataframe
    def get_curr_data(**context):
        feature_df = context["ti"].xcom_pull(
            dag_id="feature_eng", task_ids="build_features", include_prior_dates=True
        )
        feature_df.dropna(inplace=True)
        feature_df.drop("target", axis=1, inplace=True)
        return feature_df.to_numpy()

    @aql.dataframe(columns_names_capitalization="lower")
    def generate_reports(ref_data: pd.DataFrame, curr_data: pd.DataFrame):
        from evidently.test_preset import (  # NoTargetPerformanceTestPreset,; DataStabilityTestPreset
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

    ref_table = Table(
        name="iris_ground_truth",
        metadata=Metadata(
            schema="public",
            database="feature_store",
        ),
        conn_id="postgres",
    )

    curr_table = Table(
        name="new_features_predictions",
        metadata=Metadata(
            schema="public",
            database="feature_store",
        ),
        conn_id="postgres",
    )

    ref_data = get_ref_data(input_table=ref_table)
    curr_data = get_curr_data(input_table=curr_table)

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

    trigger_retrain = TriggerDagRunOperator(task_id="trigger_retrain", trigger_dag_id="train")

    cleanup = aql.cleanup()
    chain(
        reports,
        check_drift(metrics="{{ ti.xcom_pull(task_ids='generate_reports') }}"),
        trigger_retrain,
        send_retrain_alert,
    )

    reports >> [send_report, cleanup]


feature_monitoring = feature_monitoring()
