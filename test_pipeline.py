"""Tests des fonctions de nettoyage sur les cas limites des sources."""

import sys

import pytest
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, "src")
from transform import clean_date, clean_opposition, clean_prenoms, clean_sexe


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield s
    s.stop()


def apply_fn(spark, fn, value):
    """Applique une fonction de nettoyage a une valeur unique."""
    df = spark.createDataFrame([(value,)], "c string")
    return df.select(fn(F.col("c")).alias("r")).first()["r"]


@pytest.mark.parametrize("raw,expected", [
    ("1985-03-12", "1985-03-12"),   # ISO
    ("12/03/1985", "1985-03-12"),   # francais
    ("28-02-1978", "1978-02-28"),   # tirets
    ("1965/09/30", "1965-09-30"),   # slashes inverses
    ("pas une date", None),          # illisible -> null
    (None, None),
])
def test_clean_date(spark, raw, expected):
    result = apply_fn(spark, clean_date, raw)
    assert (str(result) if result else None) == expected


@pytest.mark.parametrize("raw,expected", [
    ("M", "male"), ("1", "male"), ("Homme", "male"), ("male", "male"),
    ("F", "female"), ("2", "female"), ("Femme", "female"),
    ("", "unknown"), (None, "unknown"), ("autre", "unknown"),
])
def test_clean_sexe(spark, raw, expected):
    assert apply_fn(spark, clean_sexe, raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("O", True), ("oui", True), ("oui ", True), ("true", True), ("Opposé", True),
    ("N", False), ("non", False), ("false", False), ("0", False),
    ("", None), (None, None),   # inconnu -> null, jamais False
])
def test_clean_opposition(spark, raw, expected):
    assert apply_fn(spark, clean_opposition, raw) is expected


def test_clean_prenoms(spark):
    assert apply_fn(spark, clean_prenoms, '["jean "]') == ["Jean"]
    assert apply_fn(spark, clean_prenoms, '["Marie","Claire"]') == ["Marie", "Claire"]
    assert apply_fn(spark, clean_prenoms, '[" FATIMA"]') == ["Fatima"]