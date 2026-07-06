#!/usr/bin/env python3
import time
import csv
import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# =========================================================================
# 0. PARSER DEGLI ARGOMENTI DA TERMINALE (DINAMICO)
# =========================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True, help="Input file path (es. s3://...)")
parser.add_argument("--output_path", type=str, required=True, help="Output folder path (es. s3://...)")
args = parser.parse_args()
path_file = args.input_path
output_file = args.output_path

# =========================================================================
# 1. CONFIGURAZIONE E INIZIALIZZAZIONE SPARK PER CLUSTER YARN (AWS)
# =========================================================================
conf = SparkConf() \
    .setAppName("ProgettoBigData_Analisi1_SparkCore_RDD_AWS")
    # NOTA: .setMaster("local[*]") RIMOSSO. Sarà gestito dinamicamente da yarn in AWS EMR.

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

# AVVIAMO IL CRONOMETRO DI ESECUZIONE
start_time = time.time()

# =========================================================================
# 2. CARICAMENTO DATI E COSTRUZIONE LOGICA MAPREDUCE
# =========================================================================
rdd_linee = sc.textFile(path_file)
header = rdd_linee.first()

def parser_riga(linea):
    if linea == header or not linea:
        return None
    try:
        campi = next(csv.reader([linea]))
        
        mese = int(campi[1])
        compagnia = campi[5].strip()
        origine = campi[7].strip()
        destinazione = campi[10].strip()
        ritardo = campi[22].strip()
        cancellato = int(float(campi[23]))
        
        if not compagnia or not origine or not destinazione:
            return None
            
        chiave = (compagnia, f"{origine} -> {destinazione}")
        
        if cancellato == 1:
            return (chiave, (1, float('inf'), float('-inf'), 0.0, 0, 1, {mese}))
        else:
            r_val = float(ritardo) if ritardo and ritardo != "None" else None
            if r_val is not None:
                return (chiave, (1, r_val, r_val, r_val, 1, 0, {mese}))
            else:
                return (chiave, (1, float('inf'), float('-inf'), 0.0, 0, 0, {mese}))
    except:
        return None

rdd_filtrato = rdd_linee.map(parser_riga).filter(lambda x: x is not None)

# =========================================================================
# 3. FASE DI RIDUZIONE (REDUCE)
# =========================================================================
def riduci_metriche(v1, v2):
    voli_tot = v1[0] + v2[0]
    r_min = min(v1[1], v2[1])
    r_max = max(v1[2], v2[2])
    somma_rit = v1[3] + v2[3]
    count_rit = v1[4] + v2[4]
    voli_canc = v1[5] + v2[5]
    mesi = v1[6].union(v2[6])
    return (voli_tot, r_min, r_max, somma_rit, count_rit, voli_canc, mesi)

rdd_aggregato = rdd_filtrato.reduceByKey(riduci_metriche)

# =========================================================================
# 4. STRUTTURAZIONE FINALE ED EMISSIONE FORMATTATA
# =========================================================================
def mappa_struttura_finale(record):
    (compagnia, tratta), (voli_tot, r_min, r_max, somma_rit, count_rit, voli_canc, mesi) = record
    
    rit_medio = round(somma_rit / count_rit, 2) if count_rit > 0 else "NULL"
    tasso_canc = round(voli_canc / voli_tot, 4)
    min_val = r_min if r_min != float('inf') else "NULL"
    max_val = r_max if r_max != float('-inf') else "NULL"
    
    # Formattiamo come stringa leggibile per poterla salvare come file di testo
    mesi_str = ",".join(map(str, sorted(list(mesi))))
    return f"{compagnia}\t{tratta};{voli_tot};{min_val};{max_val};{rit_medio};{tasso_canc};[{mesi_str}]"

rdd_stringhe_finali = rdd_aggregato.map(mappa_struttura_finale)

# =========================================================================
# 5. AZIONE DI OUTPUT SU S3 E STAMPA CRONOMETRO
# =========================================================================
# Salviamo l'intero risultato nel bucket S3 specificato da terminale
rdd_stringhe_finali.saveAsTextFile(output_file)

end_time = time.time()
print(f"\n⏱️ Tempo totale di esecuzione RDD (Spark Core) su AWS: {end_time - start_time:.2f} secondi\n")

spark.stop()