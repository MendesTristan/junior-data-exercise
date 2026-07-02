"""LOAD : construction des ressources Patient FHIR R4 et ecriture.

Sorties :
  output/patients_fhir_ndjson/ : 1 ressource Patient JSON par ligne
                                 (directement consommable par une API FHIR)
  output/patients_fhir.parquet/ : la meme table en format requetable
  output/rejects/               : enregistrements ecartes, avec motif
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

OUTPUT_DIR = "output"

# Systemes d'identification (URI fictives, a adapter au referentiel AP-HP)
IPP_SYSTEM = "urn:aphp:ipp"
OPPOSITION_EXTENSION_URL = "https://aphp.fr/fhir/StructureDefinition/opposition-recherche"


def _identifiers() -> Column:
    """identifier[] : l'IPP actif (use=official) + les anciens (use=old)."""
    officiel = F.array(F.struct(
        F.lit("official").alias("use"),
        F.lit(IPP_SYSTEM).alias("system"),
        F.col("ipp").alias("value"),
    ))
    anciens = F.transform(
        F.coalesce(F.col("ipp_anciens"), F.array()),
        lambda x: F.struct(
            F.lit("old").alias("use"),
            F.lit(IPP_SYSTEM).alias("system"),
            x.alias("value"),
        ),
    )
    return F.concat(officiel, anciens)


def _names() -> Column:
    """name[] : nom de naissance (official) + nom d'usage (usual) si present."""
    officiel = F.struct(
        F.lit("official").alias("use"),
        F.col("nom_naissance").alias("family"),
        F.col("prenoms").alias("given"),
    )
    usuel = F.struct(
        F.lit("usual").alias("use"),
        F.col("nom_usuel").alias("family"),
        F.col("prenoms").alias("given"),
    )
    return F.when(
        F.col("nom_usuel").isNotNull(), F.array(officiel, usuel)
    ).otherwise(F.array(officiel))


def _addresses() -> Column:
    """address[] : actuelle/domicile -> use=home, ancienne -> use=old."""
    return F.transform(
        F.coalesce(F.col("adresses"), F.array()),
        lambda a: F.struct(
            F.when(a["type_adresse"] == "ancienne", "old")
             .otherwise("home").alias("use"),
            F.array(a["ligne_adresse"]).alias("line"),
            a["code_postal"].alias("postalCode"),
            a["ville"].alias("city"),
            a["pays"].alias("country"),
            F.struct(
                a["date_debut"].alias("start"),
                a["date_fin"].alias("end"),
            ).alias("period"),
        ),
    )


def _opposition_extension() -> Column:
    """extension[] : opposition a la recherche, seulement si connue.

    Pas de champ natif dans Patient pour le consentement ; FHIR le
    modelise normalement dans une ressource Consent (voir NOTES.md).
    """
    return F.when(
        F.col("opposition").isNotNull(),
        F.array(F.struct(
            F.lit(OPPOSITION_EXTENSION_URL).alias("url"),
            F.col("opposition").alias("valueBoolean"),
        )),
    )


def to_fhir(df: DataFrame) -> DataFrame:
    """Construit la ressource Patient FHIR R4 pour chaque patient."""
    fhir = F.struct(
        F.lit("Patient").alias("resourceType"),
        F.col("ipp").alias("id"),
        _identifiers().alias("identifier"),
        (F.col("date_fin_validite").isNull()
         | (F.col("date_fin_validite") > F.current_date())).alias("active"),
        _names().alias("name"),
        F.col("sexe").alias("gender"),
        F.date_format("date_naissance", "yyyy-MM-dd").alias("birthDate"),
        F.date_format("date_deces", "yyyy-MM-dd").alias("deceasedDateTime"),
        _addresses().alias("address"),
        _opposition_extension().alias("extension"),
    )
    # to_json omet les champs null -> JSON FHIR propre sans cles vides
    return df.select(F.col("ipp"), F.to_json(fhir).alias("fhir_json"), fhir.alias("fhir"))


def load(patients: DataFrame, rejects: DataFrame) -> None:
    """Ecrit les 3 sorties. Mode overwrite -> pipeline rejouable."""
    fhir = to_fhir(patients)

    (fhir.select("fhir_json").coalesce(1)
         .write.mode("overwrite").text(f"{OUTPUT_DIR}/patients_fhir_ndjson"))

    (fhir.select("ipp", "fhir.*")
         .write.mode("overwrite").parquet(f"{OUTPUT_DIR}/patients_fhir.parquet"))

    (rejects.coalesce(1)
            .write.mode("overwrite").option("header", True)
            .csv(f"{OUTPUT_DIR}/rejects"))