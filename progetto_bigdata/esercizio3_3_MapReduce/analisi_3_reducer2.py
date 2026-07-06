#!/usr/bin/env python3
import sys
import os

# 1. CARICAMENTO SIDE TABLE (Distributed Cache / Hadoop -files)
medie_globali_apt = {}
if os.path.exists("medie_output.txt"):
    with open("medie_output.txt", "r") as f:
        for line in f:
            line = line.strip()
            if line:
                apt, med = line.split("\t")
                medie_globali_apt[apt] = float(med)

# Variabili di stato per lo streaming contiguo di Hadoop
current_aeroporto = None
dati_compagnie_apt = {}  # Contiene SOLO le compagnie dell'aeroporto corrente

def elabora_e_emetti_ranking(aeroporto, dizionario_compagnie):
    """Calcola scostamenti, ordina le performance e stampa il report completo."""
    media_globale_apt = medie_globali_apt.get(aeroporto, 0.0)
    lista_report = []
    
    for comp, metriche in dizionario_compagnie.items():
        s_dep, s_arr, s_canc, t_voli, v_val = metriche
        rit_dep = round(s_dep / v_val, 2) if v_val > 0 else 0.0
        rit_arr = round(s_arr / v_val, 2) if v_val > 0 else 0.0
        tasso_canc = round(s_canc / t_voli, 4)
        differenza = round(rit_dep - media_globale_apt, 2)
        
        lista_report.append({
            "Compagnia": comp,
            "Voli_Operati": t_voli,
            "Ritardo_Medio_Partenza": rit_dep,
            "Ritardo_Medio_Arrivo": rit_arr,
            "Tasso_Cancellazione": tasso_canc,
            "Differenza_Dalla_Media_APT": differenza
        })
    
    # Ordinamento dal ritardo minore (migliore) al maggiore (peggiore)
    lista_ordinata = sorted(lista_report, key=lambda x: x["Ritardo_Medio_Partenza"])
    
    # Assegnazione della posizione in classifica ed emissione dell'output completo
    for posizione, info in enumerate(lista_ordinata, start=1):
        # Output formattato: Aeroporto \t Posizione;Compagnia;Voli;RitDep;RitArr;TassoCanc;Scostamento
        print(f"{aeroporto}\t{posizione};{info['Compagnia']};{info['Voli_Operati']};"
              f"{info['Ritardo_Medio_Partenza']};{info['Ritardo_Medio_Arrivo']};"
              f"{info['Tasso_Cancellazione']};{info['Differenza_Dalla_Media_APT']}")

# 2. LETTURA DELLO STREAMING DA HADOOP STDIN
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
        
    try:
        aeroporto, valore = line.split("\t")
        compagnia, dep_delay, arr_delay, cancelled = valore.split(";")
        dep_delay = float(dep_delay)
        arr_delay = float(arr_delay)
        cancelled = float(cancelled)
    except:
        continue
        
    v_valido = 1 if cancelled == 0.0 else 0
    
    # Logica di aggregazione sequenziale per chiave contigua
    if current_aeroporto == aeroporto:
        if compagnia not in dati_compagnie_apt:
            dati_compagnie_apt[compagnia] = [0.0, 0.0, 0.0, 0, 0]
        dati_compagnie_apt[compagnia][0] += dep_delay
        dati_compagnie_apt[compagnia][1] += arr_delay
        dati_compagnie_apt[compagnia][2] += cancelled
        dati_compagnie_apt[compagnia][3] += 1
        dati_compagnie_apt[compagnia][4] += v_valido
    else:
        # Se cambia l'aeroporto, elaboriamo il ranking di quello appena concluso
        if current_aeroporto is not None:
            elabora_e_emetti_ranking(current_aeroporto, dati_compagnie_apt)
            
        # Reset delle strutture dati per il nuovo aeroporto
        current_aeroporto = aeroporto
        dati_compagnie_apt = {compagnia: [dep_delay, arr_delay, cancelled, 1, v_valido]}

# Elaborazione dell'ultimo blocco rimasto in coda
if current_aeroporto is not None:
    elabora_e_emetti_ranking(current_aeroporto, dati_compagnie_apt)