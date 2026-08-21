
## Customization

### Adding metrics

There are two ways to get metrics for a raw file into AlphaKraken. Both store them under the
metrics type configured in the settings, so they show up as `<metrics_type>__<metric_name>`
columns in the webapp.

#### 1. Let the quanting software report them (no code change)

Have the software write a `metrics.csv` into its output folder, with one header row of metric
names and one row of values:

```
proteins,precursors,my_own_metric
8123,95012,0.42
```

AlphaKraken reads that file after the job finished. Set the metrics type of the settings entry to
whatever labels the metrics best, e.g. `msqc` for an msqc extractor image, or `custom` if
AlphaKraken should calculate nothing at all on its own. See
[deployment.md](deployment.md#metrics-reported-by-the-quanting-software) for the details.

Prefer this way: it needs no deployment of new AlphaKraken code, and it keeps the knowledge about
a software's output inside that software.

#### 2. Add a calculation to the codebase

Use this when the software cannot be changed, e.g. for a third-party search engine that only
leaves its own output files behind. Look for "dummy code for adding new metrics" and code along the example.

1. Add a module in `airflow_src/plugins/metrics/metrics/`, using
   `example_metrics.py` as the starting point and `msqc.py` as a full example.

2. Add the metrics type to `MetricsTypes` in `shared/keys.py`, and wire the new function into the
   dispatch in `calc_metrics()` in `airflow_src/plugins/metrics/metrics_calculator.py`.


Notes:
- Metrics are stored in a `DynamicDocument`, so no database migration is needed for new metrics.
- New metrics show up in the webapp table on their own, appended after the configured columns. Add
  them to `webapp/columns_config.yaml` to control their position and to give them a color gradient
  and/or a plot.
