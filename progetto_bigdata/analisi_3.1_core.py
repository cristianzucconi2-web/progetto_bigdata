#!/usr/bin/env python3
import shutil
import time
import csv
import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Forziamo PySpark a usare lo stesso identico eseguibile Python corrente
# Questo risolve l'errore "Python non è stato trovato" dovuto agli alias di Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# =========================================================================
# 0. PARSER DEGLI ARGOMENTI DA TERMINALE (DINAMICO)
# =========================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True, help="Input file path")
parser.add_argument("--output_path", type=str, required=True, help="Output folder path")
args = parser.parse_args()

path_file = args.input_path
output_folder = args.output_path

# =========================================================================
# 1. CONFIGURAZIONE E INIZIALIZZAZIONE SPARK (CON BLINDATURA MEMORIA PYTHON)
# =========================================================================
conf = SparkConf() \
    .setAppName("ProgettoBigData_Analisi1_SparkCore_RDD") \
    .setMaster("local[*]") \
    .set("spark.driver.memory", "4g") \
    .set("spark.executor.memory", "4g") \
    .set("spark.python.worker.memory", "2g") \
    .set("spark.python.worker.reuse", "true") \
    .set("spark.python.worker.timeout", "600") \
    .set("spark.network.timeout", "800")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

# AVVIAMO IL CRONOMETRO DI ESECUZIONE
start_time = time.time()

# =========================================================================
# 2. CARICAMENTO DATI E COSTRUZIONE LOGICA MAPREDUCE
# =========================================================================
rdd_linee = sc.textFile(path_file)

# Estraiamo l'header per poterlo scartare durante il mapping delle righe
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

# Mapping e Filtraggio iniziale dei record Nulli
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
# 4. STRUTTURAZIONE FINALE AD ALBERO (GERARCHICA)
# =========================================================================
def mappa_struttura_finale(record):
    (compagnia, tratta), (voli_tot, r_min, r_max, somma_rit, count_rit, voli_canc, mesi) = record
    
    rit_medio = round(somma_rit / count_rit, 2) if count_rit > 0 else "NULL"
    tasso_canc = round(voli_canc / voli_tot, 4)
    min_val = r_min if r_min != float('inf') else "NULL"
    max_val = r_max if r_max != float('-inf') else "NULL"
    
    dati_tratta = {
        "Tratta": tratta,
        "Voli": voli_tot,
        "Min": min_val,
        "Max": max_val,
        "Medio": rit_medio,
        "Cancellazioni": tasso_canc,
        "Mesi": sorted(list(mesi))
    }
    return (compagnia, dati_tratta)

rdd_finale = rdd_aggregato.map(mappa_struttura_finale).groupByKey().mapValues(list)

# =========================================================================
# 5. AZIONE DI OUTPUT E CRONOMETRO
# =========================================================================
print("\n--- RISULTATI SPARK CORE (PARADIGMA RDD PURISSIMO) ---")
for compagnia, lista_tratte in rdd_finale.take(10):
    estratto = lista_tratte[:1]
    print(f"{compagnia}\t[{estratto[0]}, ... ]")

if os.path.exists(output_folder):
    shutil.rmtree(output_folder)

# Salviamo il risultato (convertito in stringhe per la scrittura su file)
rdd_finale.map(lambda x: str(x)).saveAsTextFile(output_folder)
print(f"✅ Risultati salvati in: {output_folder}")

end_time = time.time()
print(f"\n⏱️ Tempo totale di esecuzione RDD (Spark Core): {end_time - start_time:.2f} secondi\n")

spark.stop()
