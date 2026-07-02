# Notes de démarche



J'ai choisi de travailler sur GitHub Codespaces plutôt qu'en local.
Deux raisons : je suis sur Windows et Spark y pose souvent des problèmes
(winutils, variables d'environnement), et je voulais un environnement
reproductible que les relecteurs peuvent relancer à l'identique.

J'ai donc forké le dépôt, ouvert un Codespace dessus, et défini un
devcontainer avec Python 3.11, Java 17 (requis par Spark) et git-lfs
(le dépôt l'utilise, sans lui le push échoue). Avec ce fichier,
n'importe qui peut ouvrir le projet et tout s'installe seul.

J'ai choisi PySpark plutôt que Scala : je suis plus productif en Python,
et la logique Spark (DataFrame API) reste la même dans les deux langages.

## Choix de la structure

J'ai hésité entre un fichier unique et un découpage en modules.
J'ai finalement retenu une structure ETL simple, qui suit les trois
étapes du pipeline :

- src/extract.py : lecture des CSV
- src/transform.py : nettoyage et consolidation
- src/load.py : écriture de la sortie
- src/main.py : orchestration

Le volume est petit et le pipeline linéaire, donc je n'ai pas voulu
sur-découper. Ce découpage-là garde le code lisible : chaque fichier
a un rôle clair et main.py se lit comme le plan du pipeline.

## Compréhension des données avant de coder

Avant d'écrire la moindre transformation, j'ai ouvert les quatre
fichiers et je les ai lus ligne par ligne. Sur un volume pareil, c'est
faisable à l'œil, et ça m'a permis de repérer les pièges plutôt que de
les découvrir en plein développement.

Ce que j'ai compris du modèle : identifiants_ipp.csv est le référentiel.
Un patient peut avoir reçu deux IPP au fil du temps (doublon de fiche à
l'accueil, par exemple). Dans ce cas, l'ancien IPP est marqué DEPRECIE
et pointe vers le bon via la colonne ipp_principal. Les autres fichiers
(patients, adresses, opposition) sont rattachés à l'IPP, y compris
parfois à un IPP déprécié. Tout l'exercice consiste donc à résoudre
les identités avant de joindre quoi que ce soit.


## Arbitrage : que faire des fiches en double ?

Trois patients ont deux fiches dans patients.csv : une sous leur IPP
actif, une sous leur IPP déprécié (Martin, El Amrani, Dubois).

J'avais deux options :

1. Garder uniquement la fiche de l'IPP actif.
2. Fusionner les deux fiches champ par champ (la fiche active en
   priorité, la fiche dépréciée pour combler les trous).

J'ai comparé les fiches paire par paire. Constat : les fiches dépréciées
ne contiennent aucune information absente de la fiche active. Ce sont
les mêmes données, en moins propre (dates dans un autre format, casse
incohérente, espaces en trop, sexe codé en chiffre).

J'ai donc choisi l'option 1 : la fiche de l'IPP actif fait foi, la
fiche dépréciée est écartée. La fusion champ par champ aurait ajouté
de la complexité sans rien apporter ici, avec même le risque de
réintroduire des valeurs sales dans une fiche propre.

Deux précisions importantes sur ce choix :

- Écarter la fiche dépréciée ne veut pas dire perdre ses données
  liées : les adresses et l'opposition enregistrées sous l'ancien IPP
  sont réaffectées à l'IPP principal. L'historique d'adresses de
  Jean Martin contient donc bien son ancienne adresse de Lyon.
- L'ancien IPP n'est pas effacé de la sortie : il apparaît dans la
  ressource FHIR comme identifiant avec use = "old". Quelqu'un qui
  chercherait le patient par son ancien identifiant le retrouverait.

Si les données évoluaient (fiches dépréciées portant des informations
uniques), je passerais à la fusion champ par champ,je le note comme
limite en fin de document.