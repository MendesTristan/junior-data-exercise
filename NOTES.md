# Notes de démarche

Pipeline PySpark qui consolide les quatre extractions CSV en une table
`Patient` FHIR R4, rejouable et directement exploitable par une API.

## Comment j'ai travaillé

J'ai commencé par l'environnement. Étant sous Windows, où Spark pose souvent
des problèmes (winutils, variables d'environnement), j'ai monté le projet sur
GitHub Codespaces avec un devcontainer : Python 3.11, Java 17 et les
dépendances s'installent automatiquement. N'importe qui peut ouvrir le dépôt
dans un Codespace et lancer le pipeline sans rien configurer.

J'ai choisi PySpark plutôt que Scala : je suis aujourd'hui nettement plus
productif en Python, et la logique Spark (API DataFrame) reste identique dans
les deux langages. 

Ensuite, avant d'écrire la moindre transformation, j'ai lu les quatre fichiers
ligne par ligne. C'est ce qui m'a permis de comprendre le modèle :
`identifiants_ipp.csv` est le référentiel des identités. Un patient peut
exister sous deux IPP une fiche dépréciée qui pointe vers la fiche active
via `ipp_principal` et les adresses comme les oppositions sont parfois
rattachées à l'ancien IPP. Tout l'exercice tient dans une phrase : résoudre
les identités avant de joindre quoi que ce soit.

## Le pipeline

Une structure ETL simple, un fichier par étape :

- `src/extract.py` lecture des quatre CSV, tout en chaînes de caractères
  (pas d'inférence de schéma : avec quatre formats de dates, elle serait
  imprévisible), et suppression de la colonne fantôme créée par la virgule
  en trop dans l'en-tête de `patients.csv`.
- `src/transform.py` d'abord les fonctions de nettoyage, puis la
  consolidation : table de correspondance des IPP, filtrage sur les fiches
  actives, réaffectation des adresses et oppositions.
- `src/load.py` montage des ressources `Patient` avec des structs Spark
  imbriqués, puis écriture en NDJSON (une ressource par ligne, le format
  d'échange naturel de FHIR, directement ingérable par une API), en Parquet
  (pour le requêtage analytique), et la table des rejets en CSV.
- `src/main.py`l'orchestration 

   

Le volume est petit et le pipeline linéaire, donc je n'ai pas voulu sur-découper. Ce découpage-là garde le code lisible : chaque fichier a un rôle clair et main.py se lit comme le plan du pipeline.

  ## Lancer le pipeline

```bash
pip install -r requirements.txt
python src/main.py          # le pipeline
pytest test_pipeline.py -v  # les tests
```


## Anomalies et traitement

**Les valeurs sales.** Quatre formats de dates, que je convertis en essayant
chaque format connu (`try_to_date` renvoie null si le format ne colle pas,
`coalesce` garde le premier essai réussi). Sept encodages du sexe (`M`, `1`,
`Homme`, `male`...), ramenés aux codes FHIR `male` / `female` / `unknown` par
une table de correspondance. Des prénoms stockés en JSON dans une chaîne, avec
espaces parasites et casse aléatoire, parsés puis nettoyés élément par
élément. Des statuts d'IPP avec accents et casse variables, normalisés avant
toute comparaison.

**Les doublons.** Trois patients existent en double, sous un IPP actif et un
IPP déprécié. S'y ajoutent un doublon pur (`800000124`, deux lignes identiques
à un espace près, dédupliquées après nettoyage) et une même adresse saisie
deux fois avec des casses différentes (`800000127`), dédupliquée par une clé
insensible à la casse en gardant la saisie la plus récente.

**L'opposition à la recherche.** Neuf façons d'écrire oui ou non, une valeur
vide, des patients absents du fichier.

**Les orphelins.** Un IPP déprécié sans cible (`700000099`) et une opposition
rattachée à un IPP inconnu (`800000199`). Plutôt que de les écarter en
silence, je les trace dans `output/rejects/` avec un motif.

## Hypothèses

**Les fiches dépréciées n'apportent rien de plus.** J'ai comparé les paires :
les fiches dépréciées ne contiennent aucune information absente des fiches
actives — les mêmes données, en moins propre. C'est ce constat qui fonde
l'arbitrage « la fiche active fait foi » ci-dessous.

**Un consentement non recueilli n'est pas un consentement.** Le confondre
avec « non opposé » reviendrait à utiliser pour la recherche les données d'un
patient dont on ignore le choix.

**Les valeurs invérifiables restent telles quelles.** Le code postal invalide
(`6900` pour Lyon), l'adresse à Londres : conservés tels quels. Je ne corrige
pas une donnée que je ne peux pas vérifier. Même logique pour les champs
manquants (une date de naissance absente) : simplement omis de la ressource
FHIR, jamais inventés.

## Arbitrages

**La fiche active fait foi.** J'ai écarté la fusion champ par champ (de la
complexité sans gain, avec le risque de réintroduire des valeurs sales) au
profit d'une règle simple : la fiche active fait foi. Rien n'est perdu : les
adresses et l'opposition de l'ancien IPP sont réaffectées au patient, et
l'ancien IPP reste dans `identifier[]` avec `use: old`on peut toujours
retrouver le patient par son ancien identifiant.

**L'opposition illisible reste inconnue.**  tout ce qui n'est pas
clairement lisible reste `null`. Quand un patient fusionné a deux recueils,
le plus récent fait foi. Faute de champ natif dans `Patient`, l'information
est portée par une extension FHIR l'alternative propre serait une ressource
`Consent` séparée, je la mentionne en perspective.

**Les orphelins sont tracés, jamais supprimés.** En contexte hospitalier, une
donnée ne disparaît pas sans trace, d'autant que l'un des deux est une
opposition à la recherche, peut-être mal saisie, qui mérite une enquête côté
source.

## Vérification

28 tests unitaires (pytest) couvrent les fonctions de nettoyage, avec un cas
par valeur anormale réellement observée dans les sources : les quatre formats
de dates, chaque encodage du sexe, « Opposé », « oui » avec un espace... Le
test auquel je tiens le plus vérifie qu'un consentement vide ou illisible ne
devient jamais `False`.

Au final : 18 lignes patients en entrée (dont 3 fiches dépréciées et
1 doublon), 14 patients uniques en sortie, 2 rejets tracés. L'échantillon
complet est dans `output/`.



## Avec plus de temps

- Valider chaque ressource produite contre le profil officiel `Patient`
  (serveur HAPI FHIR ou bibliothèque `fhir.resources`), plutôt que de se
  fier à la seule structure.
- Modéliser l'opposition dans une ressource `Consent` dédiée.
- Automatiser les contrôles de qualité en entrée (type Great Expectations)
  et suivre le volume de rejets comme indicateur de santé du pipeline.
- Normaliser les adresses via la Base Adresse Nationale, pour corriger les
  codes postaux invalides au lieu de seulement les conserver.

