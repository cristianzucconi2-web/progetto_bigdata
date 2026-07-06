#!/usr/bin/env python3
import sys
import os

medie_globali_apt = {}
if os.path.exists("medie_output.txt"):
    with open("medie_output.txt", "r") as f:
        for line in f:
            apt, med = line.strip().split("\t")
            medie_globali_apt[apt] = float(med)

def elabora_e_emetti_ranking(aeroporto, diz):
    med_g = medie_globali_apt.get(aeroporto, 0.0)
    lista = []
    for comp, m in diz.items():
        s_dep, s_arr, s_canc, t_voli, v_val = m
        rit_dep = round(s_dep / v_val, 2) if v_val > 0 else 0.0
        rit_arr = round(s_arr / v_val, 2) if v_val > 0 else 0.0
        tasso = round(s_canc / t_voli, 4) if t_voli > 0 else 0.0
        # Calcolo allineato allo scostamento di Spark
        scostamento = round(rit_dep - med_g, 2)
        lista.append((comp, t_voli, rit_dep, rit_arr, tasso, scostamento))
    
    lista.sort(key=lambda x: x[2]) # Ordine basato su ritardo partenza
    for pos, i in enumerate(lista, 1):
        print(f"{aeroporto}\t{pos};{i[0]};{i[1]};{i[2]};{i[3]};{i[4]};{i[5]}")

current_apt = None
dati = {}
for line in sys.stdin:
    apt, val = line.strip().split("\t")
    comp, dep, arr, canc = val.split(";")
    if current_apt != apt:
        if current_apt: elabora_e_emetti_ranking(current_apt, dati)
        current_apt, dati = apt, {}
    if comp not in dati: dati[comp] = [0.0, 0.0, 0.0, 0, 0]
    m = dati[comp]
    m[0]+=float(dep); m[1]+=float(arr); m[2]+=float(canc); m[3]+=1; m[4]+=1 if float(canc)==0.0 else 0
if current_apt: elabora_e_emetti_ranking(current_apt, dati)