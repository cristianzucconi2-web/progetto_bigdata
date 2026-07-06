#!/usr/bin/env python3
import sys
import csv
import json

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue
        
    # CORREZIONE BUG 1: Salto l'header verificando il contenuto, non la posizione
    if "month" in linea.lower() or "op_unique_carrier" in linea.lower():
        continue
        
    try:
        campi = next(csv.reader([linea]))
        
        mese = int(campi[1])
        compagnia = campi[5].strip()
        origine = campi[7].strip()
        destinazione = campi[10].strip()
        ritardo = campi[22].strip()
        cancellato = int(float(campi[23]))
        
        if not compagnia or not origine or not destinazione:
            continue
            
        chiave = json.dumps([compagnia, f"{origine} -> {destinazione}"])
        
        # OTTIMIZZAZIONE 2: Il mese viene passato come intero singolo, non come lista [mese]
        if cancellato == 1:
            valori = [1, float('inf'), float('-inf'), 0.0, 0, 1, mese]
        else:
            r_val = float(ritardo) if ritardo and ritardo != "None" else None
            if r_val is not None:
                valori = [1, r_val, r_val, r_val, 1, 0, mese]
            else:
                valori = [1, float('inf'), float('-inf'), 0.0, 0, 0, mese]
                
        print(f"{chiave}\t{json.dumps(valori)}")
        
    except Exception as e:
        continue