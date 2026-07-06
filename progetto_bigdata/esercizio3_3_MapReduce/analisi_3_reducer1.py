#!/usr/bin/env python3
import sys

totale_ritardi = {}
conteggio_voli = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    aeroporto, ritardo = line.split("\t")
    try:
        ritardo = float(ritardo)
    except ValueError:
        continue

    if aeroporto not in totale_ritardi:
        totale_ritardi[aeroporto] = 0.0
        conteggio_voli[aeroporto] = 0

    totale_ritardi[aeroporto] += ritardo
    conteggio_voli[aeroporto] += 1

for aeroporto in totale_ritardi:
    media = round(totale_ritardi[aeroporto] / conteggio_voli[aeroporto], 2)
    print(f"{aeroporto}\t{media}")