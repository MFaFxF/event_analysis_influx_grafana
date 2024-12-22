# Ochsnersport Event KPI Dashboard

## Installation
### Grafana
`brew install grafana`
--> UI can be found at localhost:3000 (default)

### Influx
`brew install influx`
-> UI can be found at localhost:8086

#### config
ggf config file

## Setup
run `influxd` or `influxd run`
to start the DB.

create bucket (do this by command)

execute load_events.py to load data.

open grafana
new dashboard
