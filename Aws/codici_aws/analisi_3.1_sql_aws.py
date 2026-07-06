#!/usr/bin/env python3
import time
import argparse
from pyspark.sql import SparkSession

# 1. PARSER DEGLI ARGOMENTI
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True, help="Percorso S3 del file CSV")
parser.add_argument("--output_path", type=str, required=True, help="Percorso S3 della cartella di output")
args = parser.parse_args()

# 2. INIZIALIZZAZIONE SPARK
spark = SparkSession.builder \
    .appName("Analisi_SQL_Definitiva") \
    .getOrCreate()

# 3. LETTURA E VISTA
df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input_path)
df.createOrReplaceTempView("voli")

# 4. QUERY ANALITICA (Inclusa la logica per i Mesi e la struttura Dati_Tratte)
start_time = time.time()

query_sql = """
    WITH metriche_tratta AS (
        SELECT 
            op_unique_carrier AS Compagnia,
            CONCAT(origin, ' -> ', dest) AS Tratta,
            COUNT(*) AS Voli,
            ROUND(MIN(CASE WHEN cancelled = 0 THEN dep_delay END), 2) AS Min,
            ROUND(MAX(CASE WHEN cancelled = 0 THEN dep_delay END), 2) AS Max,
            ROUND(AVG(CASE WHEN cancelled = 0 THEN dep_delay END), 2) AS Medio,
            ROUND(AVG(CAST(cancelled AS FLOAT)), 4) AS Cancellazioni,
            array_sort(collect_set(month)) AS Mesi
        FROM voli
        WHERE origin IS NOT NULL AND dest IS NOT NULL AND op_unique_carrier IS NOT NULL
        GROUP BY op_unique_carrier, origin, dest
    )
    SELECT 
        Compagnia,
        collect_list(
            named_struct(
                'Tratta', Tratta,
                'Voli', Voli,
                'Min', COALESCE(Min, 0.0),
                'Max', COALESCE(Max, 0.0),
                'Medio', COALESCE(Medio, 0.0),
                'Cancellazioni', Cancellazioni,
                'Mesi', Mesi
            )
        ) AS Dati_Tratte
    FROM metriche_tratta
    GROUP BY Compagnia
"""

risultato_sql = spark.sql(query_sql)

# 5. SALVATAGGIO IN JSON
# JSON è obbligatorio qui perché hai usato 'named_struct' e 'collect_list'
risultato_sql.write.mode("overwrite").json(args.output_path)

end_time = time.time()
print(f"Analisi SQL completata in {end_time - start_time:.2f} secondi.")
print(f"Output salvato in formato JSON in: {args.output_path}")

spark.stop()