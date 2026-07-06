#!/usr/bin/env python3
"""Spark Application - Analisi 3.3 Core (Versione Corretta e Allineata)"""

import time
import argparse
import sys
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# SETUP AMBIENTE
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configurazione argomenti
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True)
parser.add_argument("--output_path", type=str, required=True)
args = parser.parse_args()

# Inizializzazione Spark
conf = SparkConf().setAppName("ProgettoBigData_Analisi3_Core_Corretto")
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

start_time = time.time()

# 1. INGESTIONE E PULIZIA
# dropDuplicates assicura l'univocità dei record[cite: 10]
df = spark.read.csv(args.input_path, header=True, inferSchema=True).dropDuplicates()

# Conversione in RDD: (Origin, Carrier, DepDelay, ArrDelay, Cancelled)
rdd_voli = df.rdd.map(lambda row: (
    str(row["origin"]), 
    str(row["op_unique_carrier"]), 
    float(row["dep_delay"] or 0.0), 
    float(row["arr_delay"] or 0.0), 
    float(row["cancelled"] or 0.0)
))

# 2. AGGREGAZIONE PER COPPIA (Aeroporto, Compagnia)
rdd_coppie = rdd_voli.map(
    lambda x: ((x[0], x[1]), (x[2], x[3], x[4], 1)) 
).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))

# 3. CALCOLO MEDIE GLOBALI PER AEROPORTO (Ponderate)
# Media calcolata come (Totale Ritardi / Totale Voli Validi)[cite: 6, 10]
rdd_medie_globali = rdd_coppie.map(
    lambda x: (x[0][0], (x[1][0], x[1][3] - x[1][2])) 
).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

diz_medie_apt = sc.broadcast(
    rdd_medie_globali.mapValues(lambda x: round(x[0]/x[1], 2) if x[1] > 0 else 0.0).collectAsMap()
)

# 4. CALCOLO STATISTICHE
def calcola_stats(chiave, valori):
    apt, comp = chiave
    somma_dep, somma_arr, somma_canc, voli_tot = valori
    voli_validi = voli_tot - somma_canc
    
    media_dep = round(somma_dep / voli_validi, 2) if voli_validi > 0 else 0.0
    media_arr = round(somma_arr / voli_validi, 2) if voli_validi > 0 else 0.0
    tasso_canc = round(somma_canc / voli_tot, 4) if voli_tot > 0 else 0.0
    scostamento = round(media_dep - diz_medie_apt.value.get(apt, 0.0), 2)
    
    # Restituiamo una chiave composta (APT, float(media_dep)) per ordinamento numerico[cite: 10]
    return ((apt, float(media_dep)), (comp, voli_tot, media_arr, tasso_canc, scostamento))

rdd_stats = rdd_coppie.map(lambda x: calcola_stats(x[0], x[1]))

# 5. ORDINAMENTO FORZATO (Numerico)
rdd_ordinato = rdd_stats.sortByKey()

def formatta_ranking(iteratore):
    aeroporto_corrente = None
    posizione = 1
    for ((apt, media_dep), (comp, voli, med_arr, tasso, scostamento)) in iteratore:
        if aeroporto_corrente != apt:
            aeroporto_corrente = apt
            posizione = 1
        riga = f"{apt}\t{posizione};{comp};{int(voli)};{media_dep};{med_arr};{tasso};{scostamento}"
        posizione += 1
        yield riga

rdd_finale = rdd_ordinato.mapPartitions(formatta_ranking)

# 6. SALVATAGGIO
rdd_finale.saveAsTextFile(args.output_path)
spark.stop()
