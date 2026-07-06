#!/usr/bin/env python3
import sys
import json

current_chiave = None

# Strutture di accumulo locali (O(1) Memory Space)
voli_tot = 0
r_min = float('inf')
r_max = float('-inf')
somma_rit = 0.0
count_rit = 0
voli_canc = 0
mesi_set = set()

def emetti_aggregazione(chiave_comp_tratta, v_tot, min_r, max_r, s_rit, c_rit, v_canc, mesi):
    try:
        compagnia, tratta = json.loads(chiave_comp_tratta)
        
        rit_medio = round(s_rit / c_rit, 2) if c_rit > 0 else "NULL"
        tasso_canc = round(v_canc / v_tot, 4)
        min_val = min_r if min_r != float('inf') else "NULL"
        max_val = max_r if max_r != float('-inf') else "NULL"
        
        output_strutturato = {
            "Tratta": tratta,
            "Voli": v_tot,
            "Min": min_val,
            "Max": max_val,
            "Medio": rit_medio,
            "Cancellazioni": tasso_canc,
            "Mesi": sorted(list(mesi))
        }
        
        print(f"{compagnia}\t{json.dumps(output_strutturato)}")
    except Exception as e:
        pass

for riga_flusso in sys.stdin:
    riga_flusso = riga_flusso.strip()
    if not riga_flusso:
        continue
        
    try:
        chiave_str, valori_json = riga_flusso.split('\t', 1)
        valori = json.loads(valori_json)
        
        p_voli_tot = valori[0]
        p_r_min = valori[1]
        p_r_max = valori[2]
        p_somma_rit = valori[3]
        p_count_rit = valori[4]
        p_voli_canc = valori[5]
        p_mese = valori[6] # Riceve l'intero puro ottimizzato dal Mapper
        
        if current_chiave == chiave_str:
            voli_tot += p_voli_tot
            r_min = min(r_min, p_r_min)
            r_max = max(r_max, p_r_max)
            somma_rit += p_somma_rit
            count_rit += p_count_rit
            voli_canc += p_voli_canc
            mesi_set.add(p_mese) # .add() per inserire l'intero nel set locale
        else:
            if current_chiave is not None:
                emetti_aggregazione(current_chiave, voli_tot, r_min, r_max, somma_rit, count_rit, voli_canc, mesi_set)
            
            current_chiave = chiave_str
            voli_tot = p_voli_tot
            r_min = p_r_min
            r_max = p_r_max
            somma_rit = p_somma_rit
            count_rit = p_count_rit
            voli_canc = p_voli_canc
            mesi_set = {p_mese} # Inizializza il set con il primo intero

    except Exception as e:
        continue

if current_chiave is not None:
    emetti_aggregazione(current_chiave, voli_tot, r_min, r_max, somma_rit, count_rit, voli_canc, mesi_set)