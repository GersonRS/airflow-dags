import io
import os
import shutil

import pandas as pd
import pytest
from minio import Minio
from pytest_docker_tools import container, fetch, volume

from dags.utils.constants import CURATED_ZONE, PROCESSING_ZONE

os.environ["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "False"  # Don't want anything to "magically" work
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"  # Don't want anything to "magically" work
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"  # Set default test settings, skip certain actions, etc.
os.environ["AIRFLOW_HOME"] = os.path.dirname(os.path.dirname(__file__))  # Hardcode AIRFLOW_HOME to root of this project


minio_image = fetch(repository="minio/minio:latest")

minio = container(
    image="{minio_image.id}",
    command=["server", "/data"],
    environment={
        "MINIO_ACCESS_KEY": "minio",
        "MINIO_SECRET_KEY": "minio123",
    },
    ports={"9000/tcp": "9000"},
)


@pytest.fixture(autouse=True, scope="session")
def reset_db():
    """Reset the Airflow metastore for every test session."""
    from airflow.utils import db

    db.resetdb()
    yield

    # Cleanup temp files generated during tests
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "unittests.cfg"))
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "unittests.db"))
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "webserver_config.py"))
    shutil.rmtree(os.path.join(os.environ["AIRFLOW_HOME"], "logs"))


@pytest.fixture()
def client(minio, data_json):
    # set up connectivity with minio storage
    client = Minio(
        f"{minio.ips.primary}:{minio.ports['9000/tcp'][0]}",
        "minio",
        "minio123",
        secure=False,
    )
    # create buckets
    client.make_bucket(PROCESSING_ZONE)
    client.make_bucket(CURATED_ZONE)
    
    # Upload data.
    df_business = pd.DataFrame.from_records(data_json)
    json_bytes = df_business.to_json(orient="records").encode("utf-8")
    json_buffer = io.BytesIO(json_bytes)
    client.put_object(
        "processing",
        "example.json",
        data=json_buffer,
        length=len(json_bytes),
        content_type="application/csv",
    )

    return client

@pytest.fixture()
def data_json():
    return [
        {
            "id": 1534,
            "user_id": 3183,
            "title": "Audio curvo vulgaris ulterius suggero traho adiuvo.",
            "body": "Cubicularis capillus repellendus. Blandior ceno creo. Arto spoliatio circumvenio. Ventito cribro aspicio. Dolorem aggredior tredecim. Omnis acsi patria. Acer culpa stillicidium. Ubi arca volubilis. Eos claustrum creator. Consequuntur copiose capto. Ceno odit attero. Curatio pecco expedita. Talus verbum defessus. Voluptas decens aut. Vulgivagus catena curo. Vitium callide verbera. Somniculosus dolor maiores. Sapiente tribuo solutio. Tametsi stabilis cinis.",
        },
        {
            "id": 1532,
            "user_id": 3178,
            "title": "Cervus auxilium vorax abscido cavus urbs arcesso nemo venio campana vulgo.",
            "body": "Capitulus ab vel. Armo utor cetera. Solvo dicta sono. Vester dedecor commodi. Tepidus ventosus velit. Nesciunt aperte subito. Adopto cum suasoria. Neque accendo turba. Cotidie amitto astrum. Tumultus thalassinus tabula. Cauda rem aliquam. Illum studio autem. Autem turpe campana. Omnis optio terminatio. Spectaculum chirographum apparatus.",
        },
    ]
