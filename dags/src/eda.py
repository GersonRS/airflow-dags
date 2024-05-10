from __future__ import annotations

from typing import Any

import pandas as pd


def Map_Var_DF(features: list[str], df: pd.DataFrame) -> pd.DataFrame:
    # Criando um dicionário para receber as variáveis
    dict_var: dict[str, list[Any]] = {
        "feature": [],
        "Tipo": [],
        "Categórico": [],
        "Binário": [],
        "Qtd var unico": [],
        "Min": [],
        "Max": [],
        "% Qtd de Nulos": [],
    }

    # Criando um loop a partir das features
    for feature in features:
        # Armazenando o nome da feature
        dict_var["feature"].append(feature)

        # Armazenando o tipo da variável
        dict_var["Tipo"].append(df[feature].dtypes)

        # Armazenando a quantidade de valores nulos
        dict_var["% Qtd de Nulos"].append(round(df[feature].isnull().sum() / df.shape[0], 4))

        if df[feature].dtype == "O":
            # Atribuindo o valor 1 se a variável for categórica
            dict_var["Categórico"].append(1)

            # Armazenando a quantidade de valores únicos
            dict_var["Qtd var unico"].append(df[feature].nunique())

            # Armazenando os valores mínimos
            dict_var["Min"].append("N/A")

            # Armazenando os valores máximos
            dict_var["Max"].append("N/A")

            if df[feature].nunique() == 2:
                # Atribuindo o valor 1 se a variável for binária
                dict_var["Binário"].append(1)

            else:
                # Atribuindo o valor 0 se a variável não for binária
                dict_var["Binário"].append(0)

        else:
            # Atribuindo o valor 0 se a variável não for categórica
            dict_var["Categórico"].append(0)

            # Armazenando a quantidade de valores únicos
            dict_var["Qtd var unico"].append(df[feature].nunique())

            # Atribuindo o valor 0 se a variável não for binária
            dict_var["Binário"].append(0)

            # Armazenando os valores mínimos
            dict_var["Min"].append(df[feature].min())

            # Armazenando os valores máximos
            dict_var["Max"].append(df[feature].max())

    # Transformando o dicionário em dataframe
    df_var = pd.DataFrame.from_dict(data=dict_var)

    # retorna o dataframe
    return df_var


def DefinirSO(string: str) -> str:
    # Lista de sistema para buscar
    Lista_SO = ["android", "iphone", "ubuntu", "windows", "macintosh"]

    # Variável para armazenar o resultado
    result = ""

    # passando a string para minúsculo
    string_lower = string.lower()

    # Roda um loop em relação a lista
    for so in Lista_SO:
        # Verifica se o sistema está contido no conjunto de caracteres
        if so in string_lower:
            # Armazena o sistema
            result = so

    # Retorna o resultado da busca
    if result == "":
        return "Outro"
    else:
        return result
