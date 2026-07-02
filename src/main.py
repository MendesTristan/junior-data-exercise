"""Point d'entree du pipeline. Lancement : python src/main.py"""

from pyspark.sql import SparkSession

from extract import extract
from transform import transform
from load import load


def main() -> None:
    spark = (SparkSession.builder
             .appName("aphp-patients-fhir")
             .master("local[*]")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    sources = extract(spark)
    patients, rejects = transform(sources)
    load(patients, rejects)

    print(f"\nPipeline termine : {patients.count()} patients, "
          f"{rejects.count()} rejets -> dossier output/")
    spark.stop()


if __name__ == "__main__":
    main()