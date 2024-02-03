"""
Constantes
"""
from __future__ import annotations

import os
from datetime import timedelta

from kubernetes.client import models as k8s

default_args = {
    "owner": "Gerson_S",
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
                        limits={"cpu": 1, "memory": "8Gi"},
                        requests={"cpu": 0.5, "memory": "5Gi"},
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
                        limits={"cpu": 2, "memory": "8Gi"},
                        requests={"cpu": 1, "memory": "5Gi"},
                    ),
                )
            ]
        ),
    )
}


# Dicionário de estado por região
dict_regiao = {
    "BR-RS": "Sul",
    "BR-RR": "Norte",
    "BR-MT": "Centro-Oeste",
    "BR-SP": "Sudeste",
    "BR-AC": "Norte",
    "BR-MS": "Centro-Oeste",
    "BR-PE": "Nordeste",
    "BR-AM": "Norte",
    "BR-CE": "Nordeste",
    "BR-AP": "Norte",
    "BR-MA": "Nosdeste",
    "BR-BA": "Nordeste",
    "BR-TO": "Norte",
    "BR-RO": "Norte",
    "BR-GO": "Centro-Oeste",
    "BR-SE": "Nordeste",
    "BR-RN": "Nordeste",
    "BR-MG": "Sudeste",
    "BR-ES": "Sudeste",
    "BR-PR": "Sul",
    "BR-DF": "Centro-Oeste",
    "BR-SC": "Sul",
    "BR-PA": "Norte",
    "BR-AL": "Nordeste",
    "BR-PB": "Nordeste",
}


# Definindo os parâmetros de style para o matplotlib
rc_params = {
    "axes.edgecolor": "#787878",
    "axes.titlecolor": "#787878",
    "axes.labelcolor": "#787878",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.color": "#787878",
    "ytick.color": "#787878",
    "axes.titleweight": "bold",
    "axes.titlesize": 12,
}
