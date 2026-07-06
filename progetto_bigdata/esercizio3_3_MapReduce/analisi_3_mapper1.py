#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)
intestazione = next(reader, None)

if intestazione and "origin" in [c.lower() for c in intestazione]:
    intestazione = [c.lower().strip() for c in intestazione]
    idx_origin = intestazione.index("origin")
    idx_dep = intestazione.index("dep_delay")
    idx_canc = intestazione.index("cancelled")
else:
    idx_origin, idx_dep, idx_canc = 14, 20, 35

for colonne in reader:
    try:
        if not colonne or len(colonne) <= max(idx_origin, idx_dep, idx_canc):
            continue
        aeroporto = colonne[idx_origin].strip()
        cancelled = float(colonne[idx_canc]) if colonne[idx_canc] else 0.0
        
        if cancelled == 0.0:
            dep_delay = float(colonne[idx_dep]) if colonne[idx_dep] else 0.0
            # Emette: AEROPORTO \t RITARDO
            print(f"{aeroporto}\t{dep_delay}")
    except:
        continue