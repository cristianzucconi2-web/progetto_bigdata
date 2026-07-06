#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)
intestazione = next(reader, None)

# Estrazione dinamica degli indici dalle colonne
if intestazione and "origin" in [c.lower() for c in intestazione]:
    intestazione = [c.lower().strip() for c in intestazione]
    idx_origin = intestazione.index("origin")
    idx_carrier = intestazione.index("op_unique_carrier")
    idx_dep = intestazione.index("dep_delay")
    idx_arr = intestazione.index("arr_delay")
    idx_canc = intestazione.index("cancelled")
else:
    # Mappatura corretta basata sul posizionamento reale del dataset (0-indexed)
    idx_origin, idx_carrier, idx_dep, idx_arr, idx_canc = 7, 5, 15, 22, 23

for colonne in reader:
    try:
        if not colonne or len(colonne) <= max(idx_origin, idx_carrier, idx_dep, idx_arr, idx_canc):
            continue
            
        aeroporto = colonne[idx_origin].strip()
        compagnia = colonne[idx_carrier].strip()
        cancelled = float(colonne[idx_canc]) if colonne[idx_canc] else 0.0
        
        dep_delay = 0.0
        arr_delay = 0.0
        if cancelled == 0.0:
            dep_delay = float(colonne[idx_dep]) if colonne[idx_dep] else 0.0
            arr_delay = float(colonne[idx_arr]) if colonne[idx_arr] else 0.0
            
        if not aeroporto or not compagnia:
            continue
            
        # CORREZIONE: Emettiamo l'aeroporto come CHIAVE per lo shuffle distribuito
        print(f"{aeroporto}\t{compagnia};{dep_delay};{arr_delay};{cancelled}")
    except:
        continue