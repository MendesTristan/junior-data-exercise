"""TRANSFORM : nettoyage des valeurs, resolution des IPP, consolidation.

Partie 1 : fonctions de nettoyage unitaires (une colonne sale entre,
une colonne propre sort). Les mappings sont en constantes pour etre
lisibles et testables.
Partie 2 (plus bas) : logique de consolidation du pipeline.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Constantes de mapping
# ---------------------------------------------------------------------------

# Valeurs rencontrees dans les sources -> codes FHIR (male / female)
SEXE_MAPPING = {
    "m": "male", "1": "male", "homme": "male", "male": "male",
    "f": "female", "2": "female", "femme": "female", "female": "female",
}

# Valeurs d'opposition a la recherche -> booleen.
# Toute valeur absente ou inconnue -> null (consentement inconnu,
# a ne pas confondre avec "non oppose").
OPPOSITION_OUI = ["o", "oui", "true", "1", "oppose"]
OPPOSITION_NON = ["n", "non", "false", "0"]

# Formats de dates rencontres dans les sources
DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MM-yyyy", "yyyy/MM/dd"]


# ---------------------------------------------------------------------------
# Fonctions de nettoyage
# ---------------------------------------------------------------------------

def clean_date(col: Column) -> Column:
    """Essaie chaque format de date connu, retourne null si aucun ne marche."""
    attempts = [F.to_date(F.trim(col), fmt) for fmt in DATE_FORMATS]
    return F.coalesce(*attempts)


def clean_sexe(col: Column) -> Column:
    """Normalise le sexe vers les codes FHIR : male / female / unknown."""
    normalized = F.lower(F.trim(col))
    mapping = F.create_map(*[F.lit(x) for kv in SEXE_MAPPING.items() for x in kv])
    return F.coalesce(mapping[normalized], F.lit("unknown"))


def clean_opposition(col: Column) -> Column:
    """Normalise l'opposition en booleen ; null si valeur absente ou inconnue."""
    # 'Opposé' -> 'oppose' : on retire les accents avant comparaison
    normalized = F.translate(F.lower(F.trim(col)), "éèêë", "eeee")
    return (
        F.when(normalized.isin(OPPOSITION_OUI), F.lit(True))
        .when(normalized.isin(OPPOSITION_NON), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def clean_nom(col: Column) -> Column:
    """Nettoie un nom de famille : trim + majuscules ; null si vide."""
    cleaned = F.upper(F.trim(col))
    return F.when(cleaned == "", None).otherwise(cleaned)


def clean_prenoms(col: Column) -> Column:
    """Parse la liste JSON de prenoms et nettoie chaque element.

    
    """
    parsed = F.from_json(col, "array<string>")
    return F.transform(parsed, lambda x: F.initcap(F.trim(x)))


def clean_statut(col: Column) -> Column:
    """Normalise le statut IPP : gere la casse et les accents.

    'actif' -> ACTIF ; 'DÉPRÉCIÉ' -> DEPRECIE
    """
    return F.translate(F.upper(F.trim(col)), "ÉÈÊË", "EEEE")

# ---------------------------------------------------------------------------
# Partie 2 : consolidation
# ---------------------------------------------------------------------------

def build_ipp_mapping(identifiants: DataFrame) -> DataFrame:
    """Table de correspondance : chaque ipp -> son ipp de reference.

    Un IPP actif pointe vers lui-meme, un deprecie vers son principal.
    Les deprecies sans principal (orphelins) sont exclus ici et
    traces dans les rejets.
    """
    ids = identifiants.select(
        F.trim("ipp").alias("ipp"),
        clean_statut(F.col("statut")).alias("statut"),
        F.trim("ipp_principal").alias("ipp_principal"),
    )
    return ids.filter(
        (F.col("statut") == "ACTIF")
        | ((F.col("statut") == "DEPRECIE") & F.col("ipp_principal").isNotNull())
    ).select(
        "ipp",
        F.coalesce("ipp_principal", "ipp").alias("ipp_ref"),
        "statut",
    )


def clean_patients(patients: DataFrame) -> DataFrame:
    """Nettoie et type toutes les colonnes de patients."""
    return patients.select(
        F.trim("ipp").alias("ipp"),
        clean_nom(F.col("nom_naissance")).alias("nom_naissance"),
        clean_nom(F.col("nom_usuel")).alias("nom_usuel"),
        clean_prenoms(F.col("prenoms")).alias("prenoms"),
        clean_date(F.col("date_naissance")).alias("date_naissance"),
        clean_sexe(F.col("sexe")).alias("sexe"),
        clean_date(F.col("date_deces")).alias("date_deces"),
        clean_date(F.col("date_fin_validite")).alias("date_fin_validite"),
    ).dropDuplicates(["ipp"])  # doublon pur 800000124 : lignes identiques apres nettoyage


def remap_to_principal(df: DataFrame, mapping: DataFrame) -> DataFrame:
    """Remplace l'ipp d'un DataFrame par l'ipp de reference.

    Sert a rattacher les adresses et oppositions des IPP deprecies
    a leur IPP principal.
    """
    return (
        df.withColumn("ipp", F.trim("ipp"))
        .join(mapping.select("ipp", "ipp_ref"), on="ipp", how="inner")
        .drop("ipp")
        .withColumnRenamed("ipp_ref", "ipp")
    )


def consolidate_adresses(adresses: DataFrame, mapping: DataFrame) -> DataFrame:
    """Nettoie les adresses, les rattache a l'IPP principal, et les
    regroupe en une liste triee par date de debut pour chaque patient."""
    cleaned = adresses.select(
        F.trim("ipp").alias("ipp"),
        F.trim("ligne_adresse").alias("ligne_adresse"),
        F.trim("code_postal").alias("code_postal"),
        F.initcap(F.trim("ville")).alias("ville"),
        F.initcap(F.trim("pays")).alias("pays"),
        F.lower(F.trim("type_adresse")).alias("type_adresse"),
        clean_date(F.col("date_debut")).alias("date_debut"),
        clean_date(F.col("date_fin")).alias("date_fin"),
    )
    remapped = remap_to_principal(cleaned, mapping)
    # Doublons d'adresse (meme adresse saisie deux fois avec des casses
    # differentes) : on compare sur une cle insensible a la casse
    # et on garde la saisie la plus recente.
    deduped = (
        remapped
        .withColumn("cle", F.lower(F.regexp_replace("ligne_adresse", r"\s+", " ")))
        .withColumn(
            "rang",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.partitionBy("ipp", "cle")
                .orderBy(F.desc_nulls_last("date_debut"))
            ),
        )
        .filter(F.col("rang") == 1)
        .drop("cle", "rang")
    )
    return deduped.groupBy("ipp").agg(
        F.sort_array(
            F.collect_list(F.struct("date_debut", "date_fin", "ligne_adresse",
                                    "code_postal", "ville", "pays", "type_adresse"))
        ).alias("adresses")
    )


def consolidate_opposition(opposition: DataFrame, mapping: DataFrame) -> DataFrame:
    """Nettoie l'opposition, la rattache a l'IPP principal, et garde
    le recueil le plus recent si un patient en a plusieurs."""
    cleaned = opposition.select(
        F.trim("ipp").alias("ipp"),
        clean_opposition(F.col("opposition")).alias("opposition"),
        clean_date(F.col("date_recueil")).alias("date_recueil"),
    )
    remapped = remap_to_principal(cleaned, mapping)
    from pyspark.sql.window import Window
    w = Window.partitionBy("ipp").orderBy(F.desc_nulls_last("date_recueil"))
    return (
        remapped.withColumn("rang", F.row_number().over(w))
        .filter(F.col("rang") == 1)
        .select("ipp", "opposition", "date_recueil")
    )


def collect_rejects(patients: DataFrame, identifiants: DataFrame,
                    opposition: DataFrame) -> DataFrame:
    """Trace les enregistrements non rattachables, avec un motif."""
    ids = identifiants.select(
        F.trim("ipp").alias("ipp"),
        clean_statut(F.col("statut")).alias("statut"),
        F.trim("ipp_principal").alias("ipp_principal"),
    )
    orphan_ids = ids.filter(
        (F.col("statut") == "DEPRECIE") & F.col("ipp_principal").isNull()
    ).select("ipp", F.lit("IPP deprecie sans IPP principal").alias("motif"))

    known = ids.select("ipp")
    orphan_oppo = (
        opposition.select(F.trim("ipp").alias("ipp"))
        .join(known, on="ipp", how="left_anti")
        .select("ipp", F.lit("opposition sur IPP inconnu du referentiel").alias("motif"))
    )
    return orphan_ids.union(orphan_oppo)


def transform(dfs: dict[str, DataFrame]) -> tuple[DataFrame, DataFrame]:
    """Enchaine la consolidation complete. Retourne (patients, rejets)."""
    mapping = build_ipp_mapping(dfs["identifiants"])

    patients = clean_patients(dfs["patients"])
    # Option A : seules les fiches des IPP actifs font foi.
    actifs = mapping.filter(F.col("statut") == "ACTIF").select("ipp")
    patients = patients.join(actifs, on="ipp", how="inner")

    # Les anciens IPP sont conserves comme identifiants secondaires (FHIR use=old)
    anciens = (
        mapping.filter(F.col("statut") == "DEPRECIE")
        .groupBy("ipp_ref").agg(F.collect_list("ipp").alias("ipp_anciens"))
        .withColumnRenamed("ipp_ref", "ipp")
    )

    adresses = consolidate_adresses(dfs["adresses"], mapping)
    oppo = consolidate_opposition(dfs["opposition"], mapping)

    result = (
        patients
        .join(anciens, on="ipp", how="left")
        .join(adresses, on="ipp", how="left")
        .join(oppo, on="ipp", how="left")
    )
    rejects = collect_rejects(dfs["patients"], dfs["identifiants"], dfs["opposition"])
    return result, rejects