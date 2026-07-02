"""EXTRACT : lecture des 4 fichiers CSV sources.

"""

from pyspark.sql import DataFrame, SparkSession

RESOURCES_DIR = "resources"


def _read_csv(spark: SparkSession, name: str) -> DataFrame:
    """Lit un CSV avec les options adaptees aux sources.

 
    """
    return (
        spark.read
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", False)
        .csv(f"{RESOURCES_DIR}/{name}.csv")
    )


def read_patients(spark: SparkSession) -> DataFrame:
    """Lit patients.csv et supprime la colonne fantome.

    L'en-tete du fichier se termine par une virgule, ce qui cree
    une colonne vide sans nom.
    """
    df = _read_csv(spark, "patients")
    ghost_cols = [c for c in df.columns if c.strip() == "" or c.startswith("_c")]
    return df.drop(*ghost_cols)


def read_identifiants(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "identifiants_ipp")


def read_adresses(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "adresses")


def read_opposition(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "opposition_recherche")



def extract(spark: SparkSession) -> dict[str, DataFrame]:
    """Point d'entree : lit les 4 sources et les retourne dans un dict."""
    return {
        "patients": read_patients(spark),
        "identifiants": read_identifiants(spark),
        "adresses": read_adresses(spark),
        "opposition": read_opposition(spark),
    } 