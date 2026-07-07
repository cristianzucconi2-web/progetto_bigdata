Nella cartella progetto_bigdata sono presenti i file python di implemenazione per spark core, spark sql e mapreduce locale, 
sono presenti nella stessa cartella anche gli output delle esecuzioni in locale

Nella cartella Aws sono presenti i file per l'esecuzione su cluster, simili a quelli locale, con logica analoga, adattati per aws
sono presenti nella stessa cartella anche gli output delle esecuzioni in locale

E' presente un file con i comandi per le esecuzioni locali e degli screenshot per i tempi di esecuzione di aws

E' infine presente la relazione di quanto affrontato nel progetto

Il file .gitattributes da il link del datset originale (https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024) 

Nella cartella dataset è stato inserito il file originale (flight_data_2024.csv), non quello duplicato (superando i 2.6 GB, supera il limite massimo consentito per i singoli file) e quello dimezzato, entrambi possono essere caricati ma nel primo caso il file andrebbe diviso in due parti
