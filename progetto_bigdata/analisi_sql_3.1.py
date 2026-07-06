import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. PARSER DEGLI ARGOMENTI
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True)
parser.add_argument("--output_path", type=str, required=True)
args = parser.parse_args()

# 2. INIZIALIZZAZIONE SPARK
spark = SparkSession.builder.appName("Analisi_Finale").master("local[*]").getOrCreate()

# 3. LETTURA E PULIZIA
df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input_path)

df_pulito = df.filter(
    (col("op_unique_carrier") != "op_unique_carrier") & 
    (col("origin").isNotNull()) & 
    (col("dest").isNotNull())
)
df_pulito.createOrReplaceTempView("voli")

# 4. QUERY ANALITICA
# Utilizziamo CONCAT_WS per trasformare l'array dei mesi in una stringa compatibile con CSV
query_sql = """
    SELECT 
        op_unique_carrier AS Compagnia,
        CONCAT(origin, ' -> ', dest) AS Tratta,
        COUNT(*) AS Voli,
        MIN(CASE WHEN cancelled = 0 AND dep_delay IS NOT NULL THEN dep_delay END) AS Min,
        MAX(CASE WHEN cancelled = 0 AND dep_delay IS NOT NULL THEN dep_delay END) AS Max,
        ROUND(AVG(CASE WHEN cancelled = 0 AND dep_delay IS NOT NULL THEN dep_delay END), 2) AS Medio,
        ROUND(AVG(CAST(cancelled AS FLOAT)), 4) AS Cancellazioni,
        CONCAT_WS(',', array_sort(collect_set(CAST(month AS STRING)))) AS Mesi
    FROM voli
    GROUP BY op_unique_carrier, origin, dest
"""

risultato = spark.sql(query_sql)

# 5. SALVATAGGIO
risultato.write.mode("overwrite").csv(args.output_path, header=True)

print(f"Analisi completata con successo in: {args.output_path}")
spark.stop()
