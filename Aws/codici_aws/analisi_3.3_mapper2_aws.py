#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)
next(reader, None) # Salta intestazione

for colonne in reader:
    try:
        aeroporto = colonne[7].strip()
        compagnia = colonne[5].strip()
        cancelled = float(colonne[23]) if colonne[23] else 0.0
        dep_delay = float(colonne[15]) if colonne[15] else 0.0
        arr_delay = float(colonne[22]) if colonne[22] else 0.0
        
        # Emette chiave aeroporto per lo shuffle & sort
        print(f"{aeroporto}\t{compagnia};{dep_delay};{arr_delay};{cancelled}")
    except: continue