#!/usr/bin/env python3
import time
import argparse
from pyspark.sql import SparkSession

# 1. PARSER DEGLI ARGOMENTI
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True)
parser.add_argument("--output_path", type=str, required=True)
args = parser.parse_args()

# 2. INIZIALIZZAZIONE SPARK (Senza configurazioni locali)
spark = SparkSession.builder \
    .appName("Analisi3_Core_AWS") \
    .getOrCreate()
sc = spark.sparkContext

# 3. INGESTIONE E PULIZIA
# Leggiamo direttamente da S3
df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input_path).dropDuplicates()

# Conversione in RDD
rdd_voli = df.rdd.map(lambda row: (
    str(row["origin"]), 
    str(row["op_unique_carrier"]), 
    float(row["dep_delay"] or 0.0), 
    float(row["arr_delay"] or 0.0), 
    float(row["cancelled"] or 0.0)
))

# 4. AGGREGAZIONE E CALCOLI (Logica RDD invariata)
rdd_coppie = rdd_voli.map(
    lambda x: ((x[0], x[1]), (x[2], x[3], x[4], 1)) 
).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))

# Media Globale
rdd_medie_globali = rdd_coppie.map(
    lambda x: (x[0][0], (x[1][0], x[1][3] - x[1][2])) 
).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

diz_medie_apt = sc.broadcast(
    rdd_medie_globali.mapValues(lambda x: round(x[0]/x[1], 2) if x[1] > 0 else 0.0).collectAsMap()
)

def calcola_stats(chiave, valori):
    apt, comp = chiave
    somma_dep, somma_arr, somma_canc, voli_tot = valori
    voli_validi = voli_tot - somma_canc
    media_dep = round(somma_dep / voli_validi, 2) if voli_validi > 0 else 0.0
    media_arr = round(somma_arr / voli_validi, 2) if voli_validi > 0 else 0.0
    tasso_canc = round(somma_canc / voli_tot, 4) if voli_tot > 0 else 0.0
    scostamento = round(media_dep - diz_medie_apt.value.get(apt, 0.0), 2)
    return ((apt, float(media_dep)), (comp, voli_tot, media_arr, tasso_canc, scostamento))

rdd_stats = rdd_coppie.map(lambda x: calcola_stats(x[0], x[1]))
rdd_ordinato = rdd_stats.sortByKey()

# 5. FORMATTAZIONE E SALVATAGGIO
def formatta_ranking(iteratore):
    aeroporto_corrente = None
    posizione = 1
    for ((apt, media_dep), (comp, voli, med_arr, tasso, scostamento)) in iteratore:
        if aeroporto_corrente != apt:
            aeroporto_corrente = apt
            posizione = 1
        yield f"{apt}\t{posizione};{comp};{int(voli)};{media_dep};{med_arr};{tasso};{scostamento}"
        posizione += 1

rdd_finale = rdd_ordinato.mapPartitions(formatta_ranking)
rdd_finale.saveAsTextFile(args.output_path)

spark.stop()