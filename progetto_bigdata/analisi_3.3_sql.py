#!/usr/bin/env python3
"""spark application"""

import time
import argparse
from pyspark.sql import SparkSession

# 1. PARSER DEGLI ARGOMENTI DA TERMINALE
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True, help="Input file path")
parser.add_argument("--output_path", type=str, required=True, help="Output folder path") # Aggiungi
args = parser.parse_args()
input_filepath = args.input_path
output_folder = args.output_path
# 2. INIZIALIZZAZIONE SPARK SESSION
spark = SparkSession \
    .builder \
    .appName("ProgettoBigData_Analisi3_SQL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# AVVIAMO IL CRONOMETRO
start_time = time.time()

# 3. IMPORT DEL FILE CSV
df = spark.read.csv(input_filepath, header=True, inferSchema=True).cache()

# 4. REGISTRAZIONE DELLA VISTA TEMPORANEA
df.createOrReplaceTempView("voli")

# 5. QUERY ANALITICA CON CORREZIONE MATEMATICA DELLA MEDIA PESATA
query_sql = """
    WITH metriche_coppia AS (
        SELECT 
            origin AS Aeroporto,
            op_unique_carrier AS Compagnia,
            COUNT(*) AS Voli_Operati,
            SUM(CASE WHEN cancelled = 0 THEN dep_delay END) AS Somma_Ritardo_Compagnia,
            COUNT(CASE WHEN cancelled = 0 THEN dep_delay END) AS Voli_Validi_Compagnia,
            ROUND(AVG(CASE WHEN cancelled = 0 THEN dep_delay END), 2) AS Ritardo_Medio_Partenza,
            ROUND(AVG(CASE WHEN cancelled = 0 THEN arr_delay END), 2) AS Ritardo_Medio_Arrivo,
            ROUND(AVG(cancelled), 4) AS Tasso_Cancellazione
        FROM voli
        WHERE origin IS NOT NULL AND op_unique_carrier IS NOT NULL
        GROUP BY origin, op_unique_carrier
    ),
    metriche_con_confronto AS (
        SELECT 
            Aeroporto,
            Compagnia,
            Voli_Operati,
            Ritardo_Medio_Partenza,
            Ritardo_Medio_Arrivo,
            Tasso_Cancellazione,
            -- Calcolo della VERA media globale dell'aeroporto (Totale Minuti / Totale Voli dell'APT)
            ROUND(
                Ritardo_Medio_Partenza - (
                    SUM(Somma_Ritardo_Compagnia) OVER(PARTITION BY Aeroporto) / 
                    SUM(Voli_Validi_Compagnia) OVER(PARTITION BY Aeroporto)
                ), 2
            ) AS Differenza_Dalla_Media_APT,
            DENSE_RANK() OVER(PARTITION BY Aeroporto ORDER BY Ritardo_Medio_Partenza ASC) AS Posizione_Classifica
        FROM metriche_coppia
    ),
    struttura_aggregata AS (
        SELECT 
            Aeroporto,
            collect_list(
                named_struct(
                    'Posizione_Classifica', Posizione_Classifica,
                    'Compagnia', Compagnia,
                    'Voli_Operati', Voli_Operati,
                    'Ritardo_Medio_Partenza', Ritardo_Medio_Partenza,
                    'Ritardo_Medio_Arrivo', Ritardo_Medio_Arrivo,
                    'Tasso_Cancellazione', Tasso_Cancellazione,
                    'Differenza_Dalla_Media_APT', Differenza_Dalla_Media_APT
                )
            ) AS Performance_Unsorted
        FROM metriche_con_confronto
        GROUP BY Aeroporto
    )
    SELECT 
        Aeroporto,
        array_sort(Performance_Unsorted, (left, right) -> case 
            when left.Posizione_Classifica < right.Posizione_Classifica then -1 
            when left.Posizione_Classifica > right.Posizione_Classifica then 1 
            else 0 
        end) AS Performance_Compagnie
    FROM struttura_aggregata
"""

risultato_sql = spark.sql(query_sql)

# 6. VISUALIZZAZIONE OUTPUT
print("\n--- RISULTATI ORDINATI PER CLASSIFICA (SPARK SQL - ANALISI 3.3 COERENTE) ---")
risultato_sql.show(10, truncate=120)
risultato_sql.write.mode("overwrite").json(output_folder)
print(f"✅ Risultati SQL 3.3 salvati in: {output_folder}")
# FINE CRONOMETRO
end_time = time.time()
print(f"\n⏱️ Tempo totale di esecuzione (SQL): {end_time - start_time:.2f} secondi\n")

spark.stop()
