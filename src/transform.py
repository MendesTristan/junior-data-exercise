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


