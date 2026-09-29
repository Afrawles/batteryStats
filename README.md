# batteryStats
Predictive Battery Health &amp; Maintenance Intelligence for EV Fleets

## Data & Credits

The initial development & validation uses public lithium-ion battery pack cycling data from Universitat Politècnica de Catalunya,
used here under its CC BY 4.0 license

**Citation:**
> de la Vega Hernández, Joaquín, Ortega Redondo, Juan Antonio & Riba Ruiz, Jordi Roger. 
> “Lithium-Ion Battery Pack Cycling Dataset with CC-CV Charging and WLTP/Constant Discharge Profiles”, 1.0, 
> Universitat Politècnica de Catalunya, 2025. DOI: https://doi.org/10.34810/data2395


**License** [**License details**](https://creativecommons.org/licenses/by/4.0/)

### how dataset was used

```
    Parquet files → Scoring → InfluxDB → NATS alerts
```

### Real EV driving + charging data (Audi e-tron)

One Audi e-tron over one year, driving and charging, from the paper "Analysis and key findings from
real-world electric vehicle field data" (Joule, 2023), doi:10.17632/7vdkzpnjgj.2. Pack-level signals only
(current, voltage, SOC, one temperature) — no per-cell values, so the imbalance flag does not apply.

```sh
pip install numpy h5py pyarrow scipy

python3 scripts/audi_ingest.py "/path/Real-world electric vehicle data driving and charging" -o datasets/sim/audi.parquet
```
