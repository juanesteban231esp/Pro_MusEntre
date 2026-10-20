from pyspark.sql import functions as F


def model(dbt, session):

    dbt.config(materialized="table")

    # Resultado de tu modelo SQL anterior
    df = dbt.ref("SQLchild_bronz_sil")

    # Diccionario de correcciones
    city_mapping = {
        "Bengaluruu": "Bengaluru",
        "Bengalore": "Bengaluru",
        "Hyderabadd": "Hyderabad",
        "NewDelhi": "New Delhi",
        "NewDelhii": "New Delhi"
    }

    # Convertimos el diccionario en un pequeño DataFrame Spark
    mapping_df = session.createDataFrame(
        list(city_mapping.items()),
        ["wrong_city", "correct_city"]
    )

    # Broadcast join porque mapping_df es diminuto
    df = (
        df
        .join(
            F.broadcast(mapping_df),
            df["city"] == mapping_df["wrong_city"],
            "left"
        )
        .withColumn(
            "city",
            F.coalesce(F.col("correct_city"), F.col("city"))
        )
        .drop("wrong_city", "correct_city")
    )

    return df