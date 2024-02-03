from __future__ import annotations

import logging
import os
from typing import Any

import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
import seaborn as sns
from airflow import Dataset
from airflow.decorators import dag
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.utils.dates import days_ago
from astro import sql as aql
from astro.dataframes.pandas import DataFrame
from astro.files import File
from astro.sql.table import Metadata
from astro.sql.table import Table
from imblearn.under_sampling import RandomUnderSampler
from mlflow_provider.hooks.client import MLflowClientHook
from sklearn.compose import make_column_transformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder

from src.eda import DefinirSO
from src.eda import Map_Var_DF
from utils.constants import default_args
from utils.constants import dict_regiao
from utils.constants import rc_params

log = logging.getLogger(__name__)
log.setLevel(os.getenv("AIRFLOW__LOGGING__FAB_LOGGING_LEVEL", "INFO"))

FILE_PATH = "data.parquet"

# AWS S3 parameters
AWS_CONN_ID = "conn_minio"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"

# MLFlow parameters
MLFLOW_CONN_ID = "conn_mlflow"
EXPERIMENT_NAME = "Default"
MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS = 1000

XCOM_BUCKET = "localxcom"


@dag(
    dag_id="feaure_engineering",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule=[Dataset("astro://conn_postgres@?table=risk_data&schema=public&database=postgres")],
    # schedule_interval="@once",
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "feature engineering"],
)
def feature_eng() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        outlets=[Dataset("s3://" + DATA_BUCKET_NAME + "/temp/" + FILE_PATH)],
    )

    create_buckets_if_not_exists = S3CreateBucketOperator.partial(
        task_id="create_buckets_if_not_exists",
        aws_conn_id=AWS_CONN_ID,
    ).expand(bucket_name=[DATA_BUCKET_NAME, MLFLOW_ARTIFACT_BUCKET, XCOM_BUCKET])

    @task_group
    def prepare_mlflow_experiment() -> str:
        @task
        def list_existing_experiments(max_results: int = 1000) -> Any:
            "Get information about existing MLFlow experiments."

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
            existing_experiments_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/search",
                request_params={"max_results": max_results},
            ).json()

            return existing_experiments_information

        @task.branch
        def check_if_experiment_exists(
            experiment_name: str,
            existing_experiments_information: dict[str, list[dict[str, str]]],
        ) -> Any:
            "Check if the specified experiment already exists."

            if existing_experiments_information:
                existing_experiment_names = [
                    experiment["name"]
                    for experiment in existing_experiments_information["experiments"]
                ]
                if experiment_name in existing_experiment_names:
                    return "prepare_mlflow_experiment.experiment_exists"
                else:
                    return "prepare_mlflow_experiment.create_experiment"
            else:
                return "prepare_mlflow_experiment.create_experiment"

        @task
        def create_experiment(experiment_name: str, artifact_bucket: str) -> Any:
            """Create a new MLFlow experiment with a specified name.
            Save artifacts to the specified S3 bucket."""

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
            experiments_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/search",
                request_params={"max_results": 1000},
            ).json()
            num = -1000
            for experiment in experiments_information["experiments"]:
                if num < int(experiment["experiment_id"]):
                    num = int(experiment["experiment_id"])

            new_experiment_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/create",
                request_params={
                    "name": experiment_name,
                    "artifact_location": f"s3://{artifact_bucket}/{num+1}",
                },
            ).json()

            return new_experiment_information

        experiment_already_exists = EmptyOperator(task_id="experiment_exists")

        @task(trigger_rule="none_failed")
        def get_current_experiment_id(experiment_name: str, max_results: int = 1000) -> Any:
            "Get the ID of the specified MLFlow experiment."

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
            experiments_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/search",
                request_params={"max_results": max_results},
            ).json()

            for experiment in experiments_information["experiments"]:
                if experiment["name"] == experiment_name:
                    return experiment["experiment_id"]

            raise ValueError(f"{experiment_name} not found in MLFlow experiments.")

        experiment_id = get_current_experiment_id(
            experiment_name=EXPERIMENT_NAME,
            max_results=MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS,
        )

        (
            check_if_experiment_exists(
                experiment_name=EXPERIMENT_NAME,
                existing_experiments_information=list_existing_experiments(
                    max_results=MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS
                ),
            )
            >> [
                experiment_already_exists,
                create_experiment(
                    experiment_name=EXPERIMENT_NAME,
                    artifact_bucket=MLFLOW_ARTIFACT_BUCKET,
                ),
            ]
            >> experiment_id
        )

        return experiment_id

    @aql.transform
    def extract_data(input_table: Table) -> Any:
        return """
            SELECT
                target_default,
                score_1,
                score_2,
                score_3,
                score_4,
                score_5,
                score_6,
                risk_rate,
                credit_limit,
                reason,
                income,
                facebook_profile,
                state,
                real_state,
                n_bankruptcies,
                n_defaulted_loans,
                n_accounts,
                n_issues,
                application_time_applied,
                application_time_in_funnel,
                email,
                external_data_provider_credit_checks_last_month,
                external_data_provider_credit_checks_last_year,
                external_data_provider_email_seen_before,
                external_data_provider_fraud_score,
                marketing_channel,
                reported_income,
                shipping_state,
                profile_tags,
                user_agent
                FROM {{input_table}}
        """

    @aql.dataframe(columns_names_capitalization="original")
    def data_clean(df: pd.DataFrame, experiment_id: str, name: str) -> pd.DataFrame:
        mlflow.sklearn.autolog()

        log.info(f"----------------------------\n{df}\n--------------------------")

        df.head()

        df.columns = df.columns.str.lower()

        # Copiando o dataset
        df_clean = df.copy()

        # Exluindo os valores ausentes da variável alvo
        df_clean.dropna(subset=["target_default"], inplace=True)

        # Substintuindo os valores negativos por NaN
        df_clean["external_data_provider_email_seen_before"] = df_clean[
            "external_data_provider_email_seen_before"
        ].apply(lambda x: np.nan if x < 0 else x)
        df_clean["reported_income"] = df_clean["reported_income"].apply(
            lambda x: np.nan if x == np.inf else x
        )
        df_clean["credit_limit"] = df_clean["credit_limit"].apply(lambda x: np.nan if x == 0 else x)
        # Corrigindo as descrições do hotmail e gmail
        df_clean["email"] = df_clean["email"].apply(
            lambda x: "hotmail.com" if x == "hotmaill.com" else x
        )
        df_clean["email"] = df_clean["email"].apply(
            lambda x: "gmail.com" if x == "gmaill.com" else x
        )

        # Armazenando as features do dataframe
        features_clean = df_clean.columns.to_list()

        # Replicando a função
        var_df_clean = Map_Var_DF(features=features_clean, df=df_clean)

        # Excluindo a variável credit_limit
        df_clean.drop(labels=["credit_limit"], axis=1, inplace=True)

        # Armazenando as variáveis a serem tratadas
        cat_df = var_df_clean["feature"].loc[(var_df_clean["Categórico"] == 1)].to_list()
        num_df = (
            var_df_clean["feature"]
            .loc[(var_df_clean["Categórico"] == 0) & (var_df_clean["feature"] != "credit_limit")]
            .to_list()
        )

        # variáveis numéricas
        imputer_num = SimpleImputer(missing_values=np.nan, strategy="median")
        imputer_num = imputer_num.fit(df_clean[num_df])
        df_clean[num_df] = imputer_num.transform(df_clean[num_df])

        # variáveis categóricas
        imputer_cat = SimpleImputer(missing_values=np.nan, strategy="most_frequent")
        imputer_cat = imputer_cat.fit(df_clean[cat_df])
        df_clean[cat_df] = imputer_cat.transform(df_clean[cat_df])

        # Armazenando as features do dataframe
        features_clean = df_clean.columns.to_list()

        # Replicando a função
        var_df_clean = Map_Var_DF(features=features_clean, df=df_clean)

        with mlflow.start_run(experiment_id=experiment_id, run_name=f"eda-{name}"):
            mlflow.sklearn.log_model(imputer_num, artifact_path="SimpleImputerNumeric")
            mlflow.sklearn.log_model(imputer_cat, artifact_path="SimpleImputerCategorical")
            mlflow.log_table(var_df_clean, artifact_file="EDA")

        return df_clean

    @aql.dataframe
    def balancer(df: pd.DataFrame, experiment_id: str, name: str) -> None:
        # Definindo os parâmetros de style adicionais para o matplotlib
        rc_params["axes.titlepad"] = 12

        # setando os parâmetros no matplotlib
        with plt.rc_context(rc_params):
            # Instanciando a Figure e Axes
            fig, ax = plt.subplots()

            # Criando o gráfico de countplot
            sns.countplot(x="target_default", data=df, ax=ax, palette="Pastel2")

            # Exibindo o título
            ax.set_title("Quantidade de default e não default")

            # Armazena o % da amostra
            percentual_default = round(
                (df["target_default"].value_counts()[1] / df.shape[0]) * 100, 2
            )

            # Criar uma informação no gráfico
            ax.annotate(
                str(percentual_default) + "% da amostra",
                xy=(1, df["target_default"].value_counts()[1]),
                xytext=(25, 25),
                color="#787878",
                weight="bold",
                textcoords="offset points",
                arrowprops=dict(color="#787878", shrink=0.05, width=0.01, headwidth=7),
            )

            os.makedirs(os.path.dirname("include/plots/"), exist_ok=True)
            # Save the plot as a PNG file
            plt.savefig("include/plots/target_default.png")
            plt.close()
            with mlflow.start_run(experiment_id=experiment_id, run_name=f"balancer-{name}"):
                mlflow.log_artifact("include/plots/target_default.png", "target_default-plots")

    @aql.dataframe
    def outliers(df: pd.DataFrame) -> DataFrame:
        # Copiando o dataset
        df_clean = df.copy()

        # armazenando as features para utilizar o método IQR Score
        feature_IQR = ["income", "reported_income", "n_accounts", "n_issues"]

        # Loop em relação as features
        for feature in feature_IQR:
            Q1 = df_clean[feature].quantile(q=0.25)  # Definindo o primeiro quartil
            Q3 = df_clean[feature].quantile(q=0.75)  # Definindo o segundo quartil
            IQR = Q3 - Q1  # Definindo o interquartil
            Limite_Superior = Q3 + IQR  # Definindo o Limite Superior
            Limite_Inferior = Q1 - IQR  # Definindo o Limite Inferior

            # Eliminando os outliers acima do limite superior
            df_clean.drop(
                df_clean.loc[df_clean[feature] > Limite_Superior].index, axis=0, inplace=True
            )

            # Eliminando os outliers abaixo do limite inferior
            df_clean.drop(
                df_clean.loc[df_clean[feature] < Limite_Inferior].index, axis=0, inplace=True
            )

        return df_clean

    @aql.dataframe(multiple_outputs=True)
    def feature_eng(df: pd.DataFrame, experiment_id: str, name: str) -> Any:
        mlflow.sklearn.autolog()

        # Copiando o dataset
        df_clean = df.copy()

        # Considerando apenas a hora e convertendo para inteiro
        df_clean["application_time_applied"] = (
            df_clean["application_time_applied"].str[:2].astype("int64")
        )

        # Retirando o padrão dicionário
        df_clean["profile_tags"] = df_clean["profile_tags"].str.extract(r"\[(.*)\]")

        # Retirando as aspas e vírgulas
        df_clean["profile_tags"] = (
            df_clean["profile_tags"].str.replace("'", "").str.replace(",", "").str.replace("+", "")
        )

        # Setando o algoritmo
        bag_of_words = CountVectorizer(binary=True, analyzer="word")

        # Treinando o algoritmo
        words = bag_of_words.fit_transform(df_clean["profile_tags"]).todense()

        # Criando um dataframe a partir da matriz criada
        df_tag = pd.DataFrame(words, columns=bag_of_words.get_feature_names(), index=df_clean.index)

        # unindo os dataframes
        df_clean_novo = pd.concat([df_clean, df_tag], axis=1)

        # Excluindo a variável profile_tags
        df_clean_novo.drop(labels=["profile_tags"], axis=1, inplace=True)

        # Armazenando os % em um dicionário
        dict_reason = (df_clean_novo["reason"].value_counts() / df_clean_novo.shape[0]).to_dict()

        # Criando uma nova variável
        df_clean_novo["mais_freq"] = df_clean_novo["reason"].apply(
            lambda x: 1 if dict_reason[x] > 0.02 else 0
        )

        # Excluindo a variável reason do dataset
        df_clean_novo.drop(labels=["reason"], axis=1, inplace=True)

        # Criando a variável
        df_clean_novo["SO"] = df_clean_novo["user_agent"].apply(DefinirSO)

        # Excluindo a variável
        df_clean_novo.drop(labels=["user_agent"], axis=1, inplace=True)

        # Criando uma variável por região
        df_clean_novo["regiao"] = df_clean_novo["shipping_state"].apply(lambda x: dict_regiao[x])

        # Eliminando a variável shipping_state
        df_clean_novo.drop(labels=["shipping_state"], axis=1, inplace=True)

        # Lista de variáveis binárias (LabelEncoder)
        var_bin = ["target_default", "facebook_profile"]

        # Lista de variáveis categóricas (getdummies)
        var_cat = [
            "score_1",
            "score_2",
            "state",
            "real_state",
            "email",
            "marketing_channel",
            "SO",
            "regiao",
        ]

        # Vamos copiar o dataframe
        df_trat = df_clean_novo.copy()

        # Instanciando o objeto labelEncoder
        le = LabelEncoder()

        # Criando um loop para codificar as variáveis binárias
        for feature in var_bin:
            # transformando os valores binários em numéricos
            df_trat[feature] = le.fit_transform(df_trat[feature])

        # Aplicando o OneHotEncoder nas variáveis categóricas
        one_hot_enc = make_column_transformer(
            (OneHotEncoder(handle_unknown="ignore"), [var_cat]), remainder="passthrough"
        )

        df_trat = one_hot_enc.fit_transform(df_trat)
        df_trat = pd.DataFrame(df_trat, columns=one_hot_enc.get_feature_names_out())

        # df_trat = pd.get_dummies(data=df_trat, columns=var_cat)

        # Definindo os valores de X e y
        X = df_trat.drop(["target_default"], axis=1)
        y = df_trat["target_default"]

        # Dividindo o dataset em treino e test
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, shuffle=True, stratify=y, random_state=42
        )

        # Instanciando o objeto RandomUnderSampler()
        rus = RandomUnderSampler(random_state=0)

        # Balanceando a amostra
        X_train_rus, y_train_rus = rus.fit_sample(X_train, y_train)

        # Transformando o array X em dataframe e y em série
        X_train_rus = pd.DataFrame(X_train_rus, columns=X_train.columns)
        y_train_rus = pd.Series(y_train_rus)

        with mlflow.start_run(experiment_id=experiment_id, run_name=f"feature-engine-{name}"):
            mlflow.sklearn.log_model(bag_of_words, artifact_path="CountVectorizer")
            mlflow.sklearn.log_model(one_hot_enc, artifact_path="OneHotEncoder")
            mlflow.sklearn.log_model(le, artifact_path="LabelEncoder")
            mlflow.sklearn.log_model(rus, artifact_path="RandomUnderSampler")

        return {"X_train": X_train_rus, "X_test": X_test, "y_train": y_train_rus, "y_test": y_test}

    input_table = Table(
        name="risk_data",
        metadata=Metadata(
            schema="public",
            database="postgres",
        ),
        conn_id="conn_postgres",
    )

    extracted_df = extract_data(input_table=input_table)

    save_data_to_other_s3 = aql.export_file(
        task_id="save_data_to_s3",
        input_data=extracted_df,
        output_file=File(
            path=os.path.join("s3://", DATA_BUCKET_NAME, FILE_PATH), conn_id=AWS_CONN_ID
        ),
        if_exists="replace",
    )

    exp_id = prepare_mlflow_experiment()

    data = data_clean(
        df=extracted_df,
        experiment_id=exp_id,
        name="data-clean-{{ ts_nodash }}",
    )

    balance = balancer(
        df=input_table,
        experiment_id=exp_id,
        name="data-clean-{{ ts_nodash }}",
    )

    (
        start
        >> create_buckets_if_not_exists
        >> exp_id
        >> data
        >> [
            feature_eng(
                df=outliers(data),
                experiment_id="{{ ti.xcom_pull(task_ids='prepare_mlflow_experiment.get_current_experiment_id') }}",  # noqa: E501
                name="{{ ts_nodash }}",
            ),
            balance,
        ]
        # >> aql.cleanup()
        >> end
    )

    start >> extracted_df >> save_data_to_other_s3 >> end


feature_eng()
