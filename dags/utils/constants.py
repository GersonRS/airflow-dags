"""
Constantes
"""
import os
import airflow
from datetime import timedelta
from kubernetes.client import models as k8s

default_args = {
    "owner": "Gerson_S",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email": ["gersonrodriguessantos8@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(1),
}

LANDING_ZONE = os.getenv("LANDING_ZONE", "landing")
PROCESSING_ZONE = os.getenv("PROCESSING_ZONE", "processing")
CURATED_ZONE = os.getenv("CURATED_ZONE", "curated")

etl_config = {
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(labels={"purpose": "load_bq_data"}),
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        limits={"cpu": 1, "memory": "8Gi"}, requests={"cpu": 0.5, "memory": "5Gi"}
                    ),
                )
            ]
        ),
    )
}

modeling_config = {
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(labels={"purpose": "modeling"}),
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        limits={"cpu": 2, "memory": "8Gi"}, requests={"cpu": 1, "memory": "5Gi"}
                    ),
                )
            ]
        ),
    )
}
