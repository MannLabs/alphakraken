## Add a new instrument
Each instrument is identified by a unique `<INSTRUMENT_ID>`,
which should be lowercase and contain only letters and numbers but is otherwise arbitrary (e.g. "newinst1").
Note that some parts of the system rely on convention, so make sure to use exactly
`<INSTRUMENT_ID>` (case-sensitive!) in the below steps.

1. On the instrument, create a folder named `Backup` (capital B!) in the folder where the acquired files are saved.

2. Add the following block to the end of the `instruments` section in `envs/alphakraken.$ENV.yaml` (mind the correct indentation!):
```bash
   <INSTRUMENT_ID>:
    type: <INSTRUMENT_TYPE>
    username: <username for instrument>
    mount_src: //<ip address of instrument>/<INSTRUMENT_ID>
    mount_target: instruments/<INSTRUMENT_ID>
```
Here, `<INSTRUMENT_TYPE>` is one of the keys defined in the `InstrumentKeys` class (`thermo`, `bruker`, `sciex`) and determines
what output is expected from the instrument:
`thermo` -> one `.raw` file, `bruker` -> one `.d` folder, `sciex` -> one `.wiff` file plus more files with the same stem.

3. In `docker-compose.yaml`, add a new worker service, by copying an existing one and adapting it like:
```
  airflow-worker-<INSTRUMENT_ID>:
    <<: *airflow-worker
    command: celery worker -q kraken_queue_<INSTRUMENT_ID>
    # there might be additional keys here, just copy them
    volumes:
      - ${MOUNTS_PATH:?error}/airflow_logs:/opt/airflow/logs:rw
      - ${MOUNTS_PATH:?error}/output:/opt/airflow/mounts/output:ro
      - ${MOUNTS_PATH:?error}/instruments/<INSTRUMENT_ID>:/opt/airflow/mounts/instruments/<INSTRUMENT_ID>:ro
      - ${MOUNTS_PATH:?error}/backup/<INSTRUMENT_ID>:/opt/airflow/mounts/backup/<INSTRUMENT_ID>:rw
```
Make sure to replace each instance of `<INSTRUMENT_ID>` with the correct instrument ID
and to not accidentally drop the `ro` and `rw` flags as they limit file access rights.

4. Transfer the changes in `2.` and `3.` to all AlphaKraken PCs/VMs.

5. On the PC hosting the workers execute
```
./mount.sh <INSTRUMENT_ID>
```
and then start the new container `airflow-worker-<INSTRUMENT_ID>`  (cf. [above](maintenance.md/#restart-of-pcvm-hosting-the-workers)).

6. Restart all relevant infrastructure containers (`scheduler`, `file_mover` and `file_remover`) with the `--build` flag (cf. [above](maintenance.md/#restart-of-pcvm-hosting-the-airflow-infrastructure)).

7. Open the airflow UI and unpause the new `*.<INSTRUMENT_ID>` DAGs. It might be wise to do this one after another,
(`instrument_watcher` -> `acquisition_handler` -> `acquisition_processor`.) and to check the logs for errors before starting the next one.
