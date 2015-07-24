
## Collect Usage Information of Spark Cluster

### Run

#### Generate Usage Data

    python spark_info_parser.py -f [file of master urls] -s [sleep interval in seconds] -o [output file prefix] -n [running times]

#### Analyse Usage Data

    python spark_info_analyser.py -d [path to pickle files]
