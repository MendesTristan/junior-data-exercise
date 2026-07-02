# Exercice technique — Data Engineer Junior

## Contexte

Un établissement de santé souhaite exposer, via une **API FHIR R4**, un référentiel patient
**propre et fiable**. Aujourd'hui, l'information patient est dispersée dans
plusieurs extractions issues de systèmes différents (admissions, gestion des
identités, recueil du consentement). Ces fichiers ne sont pas alignés, se
recoupent partiellement et contiennent des imperfections typiques de données de
production.

Votre mission : à partir de ces extractions, **construire un pipeline qui
alimente une table `Patient` au format [FHIR R4](https://hl7.org/fhir/R4/patient.html)**,
directement exploitable par une API.

## Données sources

Les fichiers se trouvent dans [`resources/`](./resources). Ce sont des CSV
(séparateur `,`, en-tête sur la première ligne, encodage UTF-8).

| Fichier                    | Contenu                                                                                                                                                                                                                                                                               |
|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `patients.csv`             | Identité administrative : identifiant patient (**IPP**), nom de naissance, nom usuel, prénom(s) — un patient peut en avoir plusieurs —, date de naissance, sexe, date de décès éventuelle, et une date de fin de validité de l'enregistrement. |
| `identifiants_ipp.csv`     | Cycle de vie des IPP : statut (actif / déprécié) et, le cas échéant, l'IPP vers lequel un IPP déprécié a été rattaché.                                                                                                                                                                |
| `adresses.csv`             | Adresses des patients, actuelles et anciennes, avec période de validité.                                                                                                                                                                                                              |
| `opposition_recherche.csv` | Indique si un patient s'est opposé à l'utilisation de ses données à des fins de recherche.                                                                                                                                                                                            |

(*️ Attention les données sont volontairement imparfaites) 

## Objectif

Produire une table consolidée de **ressources `Patient` FHIR R4**, où :

- chaque patient « réel » apparaît **une seule fois** ;
- l'information dispersée dans les différentes sources est **réconciliée** ;
- le résultat est **directement consommable par une API** (sérialisable en FHIR
  valide, requêtable, sans retraitement supplémentaire).

Le format de sortie est **à votre
appréciation** : justifiez votre choix au regard de l'usage « API ».

## Attentes techniques

- Le traitement doit être réalisé avec **Apache Spark**.
- **Scala est préféré** (PySpark accepté si vous êtes nettement plus à l'aise —
  précisez-le).
- Le code doit être **structuré, lisible et rejouable** : on doit pouvoir
  relancer le job sans corrompre le résultat.

## Ce que nous regardons

L'énoncé est **volontairement ouvert**. Il n'y a pas une seule bonne réponse :
nous nous intéressons surtout à **votre raisonnement et à vos arbitrages**.

## Livrables attendus

1. Le **code source** du ou des job(s) Spark.
2. Un moyen **simple de lancer** le pipeline (instructions de build/exécution,
   `spark-submit`, ou équivalent).
3. Un **court document** (un `NOTES.md` suffit) décrivant :
    - vos hypothèses et arbitrages,
    - les anomalies détectées et leur traitement,
    - ce que vous feriez avec plus de temps.
4. Un **échantillon de la table `Patient`** produite.

## Indications pratiques

- Le volume est volontairement petit : l'enjeu est la **justesse et la
  démarche**, pas la performance à grande échelle (mais nous apprécions que
  vous sachiez quand celle-ci deviendrait un sujet).
- Privilégiez un périmètre **terminé et cohérent** plutôt qu'un périmètre large
  et inachevé.
- Comptez environ **une demi-journée** de travail. Si vous manquez de temps,
  documentez ce que vous auriez fait.

Bon courage 🙂




# Section Indication de lancement de la pipeline


## Implémentation

Pipeline PySpark structuré en ETL : `src/extract.py` (lecture),
`src/transform.py` (nettoyage et consolidation), `src/load.py`
(mapping FHIR R4 et écriture), `src/main.py` (orchestration).

### Lancer le pipeline

Dans un GitHub Codespace (l'environnement s'installe seul via le
devcontainer) ou tout environnement avec Python 3.11+ et Java 17 :

    pip install -r requirements.txt
    python src/main.py

### Lancer les tests

    pytest test_pipeline.py -v

### Sorties (dossier output/)

- `patients_fhir_ndjson/` : une ressource Patient FHIR R4 par ligne
- `patients_fhir.parquet/` : la même table en format requêtable
- `rejects/` : enregistrements écartés, avec motif

Les hypothèses, anomalies repérées et arbitrages sont détaillés
dans [NOTES.md](NOTES.md).