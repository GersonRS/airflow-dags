import os
from datetime import datetime
from io import BytesIO

import pandas as pd
from dags.utils.constants import CURATED_ZONE, LANDING_ZONE, PROCESSING_ZONE


def read_business_json_data(**kwargs):
    
    file_names = kwargs["files"]

    client = kwargs["client"]

    # try:
    list_name = []
    for file in file_names:
        obj_business = client.get_object(
            PROCESSING_ZONE,
            file,
        )
        
        df_business = pd.read_json(obj_business, orient="records")
        selected_data = df_business[["title", "body"]].head(5)
        selected_data.to_dict("records")

        csv_bytes = selected_data.to_csv(header=True, index=False).encode("utf-8")
        csv_buffer = BytesIO(csv_bytes)
        name = "business/business-"+datetime.now().strftime("%Y-%m-%d_%Hh%Mm%Ss")+".csv"
        client.put_object(
            CURATED_ZONE,
            name,
            data=csv_buffer,
            length=len(csv_bytes),
            content_type="application/csv",
        )
        list_name.append(name)
    return list_name
    # except:
    #     print("deu erro em tudo")
    #     return []
