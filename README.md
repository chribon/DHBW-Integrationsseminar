# DHBW-Integrationsseminar

## Speicherung von MES-Daten (MPDV Hydra) der "Digitalen Fabrik" in dem HDP (Hortonworks Data Platform)-Cluster

* Sichten der Hydra/MIP REST-API (Welche Daten stehen an der Schnittstelle zur Verfügung?)
* Art der Datenspeicherung in dem HDP-Cluster eruieren und anschließend Konzeption einer geeigneten Datenablage
* Implementierung einer Lösung zur Datenerfassung und Datenablage in dem HDP-Cluster
* Performancetests der Datenablage



## Aufbau des Repos
### API_Inspection
Dieser Ordner enthält ein Jupyter Notebook, das während der Sichtung der MES Hydra API erstellt wurde und in dem z. B. die Anzahlen der Domains ermittelt wurden und aufgelistet sind. Zudem sind die einzelnen Übersichten von Domains und Services im JSON-Format abgelegt.
### HDFS
Abgelegt sind hier das für den Performancetest genutzte Python-Skript und ein Skript, das alle API-Endpunkte abfragt. Zusätzlich ist eine Auflistung aller zu installierenden Python-Module vorhanden.
### Hive
Neben einer Datei mit den zu installierenden Python-Modulen ist das Python-Skript zur Abfrage und Ablage aller API-Endpunkte enthalten und das für die Performancetests verwendete Skript. Hierbei handelt es sich je um Variante drei, die die Zwischenspeicherung im HDFS nutzt. Die unter Kapitel 4.4 Bereich Python Eigenentwicklung beschriebenen Varianten eins und zwei sind im Unterordner 'variants1and2' abgelegt.
### Dokumentation
Hier ist das ausgearbeitete Dokument zu finden.


## Projektmitglieder
|Name|GitHub-Name|
|----------|--------|
|Christian Bonfert|[chribon](https://github.com/chribon)|
|Katharina Ehrmann|[kela4](https://github.com/kela4)|
|Maria Fladung|[gramoerpsel](https://github.com/gramoerpsel)|
|Lukas Habermann|[lh-alcatraz](https://github.com/lh-alcatraz)|
|Roman Nolde| [Cherzad](https://github.com/Cherzad)|
|Tom Schmelzer|[schmelto](https://github.com/schmelto)|



