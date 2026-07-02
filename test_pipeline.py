"""Tests des fonctions de nettoyage.

Je ne teste pas tout le pipeline de bout en bout, mais les fonctions
de nettoyage unitaires .
"""

import sys

import pytest
from pyspark.sql import SparkSession, functions as F

# Le code du pipeline est dans src/, on l'ajoute au path pour l'importer
sys.path.insert(0, "src")
from transform import clean_date, clean_opposition, clean_prenoms, clean_sexe


@pytest.fixture(scope="session")
def spark():
    # Une seule session Spark pour toute la suite de tests :
    # le demarrage de Spark est lent (~30s), pas question d'en
    # relancer une par test.
    s = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield s
    s.stop()


def apply_fn(spark, fn, value):
    """Applique une fonction de nettoyage a une seule valeur.

    Petit utilitaire pour ne pas repeter la creation d'un DataFrame
    d'une ligne dans chaque test. Le schema est declare explicitement
    ("c string") : avec une valeur None, Spark ne peut pas inferer
    le type et leve une erreur CANNOT_DETERMINE_TYPE.
    """
    df = spark.createDataFrame([(value,)], "c string")
    return df.select(fn(F.col("c")).alias("r")).first()["r"]


# Les 4 formats de dates trouves dans les sources, plus les cas
# d'erreur. Une date illisible doit donner null, pas planter.
@pytest.mark.parametrize("raw,expected", [
    ("1985-03-12", "1985-03-12"),   # format ISO, deja propre
    ("12/03/1985", "1985-03-12"),   # format francais (le plus courant)
    ("28-02-1978", "1978-02-28"),   # jour-mois-annee avec tirets
    ("1965/09/30", "1965-09-30"),   # annee en tete mais avec slashes
    ("pas une date", None),          # valeur illisible -> null, pas de crash
    (None, None),                    # valeur absente -> null
])
def test_clean_date(spark, raw, expected):
    result = apply_fn(spark, clean_date, raw)
    # clean_date retourne un objet date, on compare en string
    assert (str(result) if result else None) == expected


# Le sexe est encode de 7 facons differentes dans patients.csv.
# Tout doit converger vers les 3 codes FHIR : male / female / unknown.
@pytest.mark.parametrize("raw,expected", [
    ("M", "male"), ("1", "male"), ("Homme", "male"), ("male", "male"),
    ("F", "female"), ("2", "female"), ("Femme", "female"),
    ("", "unknown"),      # le patient 800000130 a un sexe vide
    (None, "unknown"),
    ("autre", "unknown"), # valeur inconnue -> unknown, on n'invente pas
])
def test_clean_sexe(spark, raw, expected):
    assert apply_fn(spark, clean_sexe, raw) == expected


# Le test le plus important du fichier. L'opposition a la recherche
# est une donnee sensible : une valeur vide ou illisible doit rester
# null (consentement inconnu) et surtout ne JAMAIS devenir False,
# sinon on traiterait un patient au statut inconnu comme non-oppose.
@pytest.mark.parametrize("raw,expected", [
    ("O", True), ("oui", True), ("true", True),
    ("oui ", True),      # espace en trop, vu sur le patient 800000130
    ("Opposé", True),    # avec majuscule et accent, vu sur 800000131
    ("N", False), ("non", False), ("false", False), ("0", False),
    ("", None),          # valeur vide -> inconnu, pas False
    (None, None),        # valeur absente -> inconnu, pas False
])
def test_clean_opposition(spark, raw, expected):
    # "is" et pas "==" : en Python, 0 == False vaut True, ce qui
    # masquerait une erreur. "is" verifie qu'on a exactement
    # True, False ou None.
    assert apply_fn(spark, clean_opposition, raw) is expected


def test_clean_prenoms(spark):
    # Les prenoms arrivent en pseudo-JSON avec des espaces parasites
    # et une casse aleatoire. On verifie le parsing + le nettoyage.
    assert apply_fn(spark, clean_prenoms, '["jean "]') == ["Jean"]          
    assert apply_fn(spark, clean_prenoms, '["Marie","Claire"]') == ["Marie", "Claire"]
    assert apply_fn(spark, clean_prenoms, '[" FATIMA"]') == ["Fatima"]      