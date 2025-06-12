
## A note on 'state', fallback & catchup
AlphaKraken is designed to be robust against interruptions, and built to be distributed easily across several machines.
One main design principle is that all relevant state of AlphaKraken is stored in the MongoDB database.
Together with the raw data on the instrument and in the backup, this defines the state of the system.
All other components can, in principle, be shut down, restarted and/or deleted any time without loss of information,
in particular:

- The Airflow scheduler and webserver, and the Webapp can be restarted at any point.

- The Airflow workers can be restarted at any point, with a little caveat: if this happens while the `copy_file`
task is being run, the file copy operation can be interrupted mid-way. Once the worker is restarted, the task will
then report a hashsum mismatch, which needs to be resolved (e.g. using the `backup_overwrite_file_id` variable).

- The Airflow DB can in principle be deleted and rebuilt from scratch, as all information is stored in the MongoDB database.
Under certain circumstances, files will stay in non-final states, which can be detected easily in the Webapp.

### Fallback & catchup
Suppose a given AlphaKraken instance `A`, with a certain state in its `MongoDB`, becomes temporarily unavailable.
To continue operation, one a completely independent new instance `B` can be set up
(with the same configuration, mounts, quanting settings),
which will take care of the file backup and processing tasks while `A` is down. As `B` would be starting from a completely
clean MongoDB instance, the comparison to past data will of course be limited.
Also, the `debug_max_file_age_in_hours` Airflow variable can be used to avoid re-processing older files
in case the instrument acquisition folder contains many old files.
Note that, in order to allow `A` to pick up from where it stopped, the `file_move` task should be disabled on `B`
as the files in the acquisition folders are the entrypoint for the whole processing.

Once `A` is available again, shut down `B` and restart `A` with the Airflow variables `output_exists_mode=associate`
and `consider_old_files_acquired=True`. This will allow `A` to quickly recover by leveraging
- that most of the files are already present on the backup and
- that most of the quanting is already done.

Note that during a catchup phase, it might be required to increase the number of scheduler instances and/or workers,
and to monitor the system closely.

## Airflow Variables
These variables are set in the Airflow UI under "Admin" -> "Variables". They steer the behavior of the whole system,
so be careful when changing them. If in doubt, pause all DAGs that are not part of the current problem before changing them.

### consider_old_files_acquired (default: False)
If this is set to `True`, the acquisition monitor will use an additional check to decide whether acquisition is done:
it will compare the creation date of the currently monitored file with that of the youngest file.
If this is more than a certain threshold (currently 5 hours), the acquisition is considered done.
This is useful to speed up the `acquisition_handler` DAG in case there a many 'old' files,
e.g. when processing was halted for some time.

### backup_overwrite_file_id (default: None)
In case the `file_copy` task is interrupted (e.g. manually) while a file is being copied,
simply restarting it will not help, as the partially copied file will not be overwritten
due to a security mechanism.
In this case, set the `backup_overwrite_file_id` variable to the file _id_ (not: file _name_), i.e. including
potential collision flags, and restart the `file_copy` task. The overwrite protection
will be deactivated just for this file id and the copying should succeed.


### output_exists_mode (default: raise)
Convenience switch to avoid manual handling of output files
in case something went wrong with the quanting.

If set to `raise`, processing of the file will stop in case the output folder exists.
If set to `overwrite`, the system will start a new job, overwriting the existing output files.
If set to `associate`, the system will try to read off the Slurm job id from the existing AlphaDIA `log.txt`
(from the first line containing "slurm_job_id: ", taking the first word after that)
and associate the current task to the slurm job with that id.

Recommended setting in production: 'raise' (default)



### min_free_space_gb (default: -1)
If set to a positive number (unit: GB), the `file_remover` will remove files (oldest first) from the
instrument backup folder until the free space is above this threshold.
The value of `min_file_age_to_remove_in_days` (see below) is taken into account when
selection files to remove.
Can be overruled per instrument by setting the `min_free_space_gb` variable in the `alphakraken.yaml` file.

Recommended setting in production: `300` (big enough to avoid running out of space over a weekend
(in the worst case scenario that the file_remover stops running on Friday).

### min_file_age_to_remove_in_days (default: 14)
The minimum file age in days for files to be removed by the file_remover.

Recommended setting in production: `14` (default)


### debug_no_cluster_ssh (default: False)
`debug_no_cluster_ssh` If set to `True`, the system will not connect to the Slurm cluster. This is useful for
testing, debugging and to avoid flooding the cluster at the initial setup.

Recommended setting in production: False (default)

### debug_max_file_age_in_hours (default: -1)
If set, files that are older will not be processed by the acquisition handler, i.e. not be backed up nor quanted.
They will nevertheless be added to the DB, with status="ignored".
Needs to be a valid float, "-1" to deactivate.

Recommended setting in production: -1 (default)


## Troubleshooting

### Problem: worker does not start

A worker fails to start up with the error
```
Error response from daemon: Mounts denied:
The path /home/kraken-user/alphakraken/sandbox/mounts/.... is not shared from the host and is not known to Docker.
```

#### Solution
Check that the mounting has been done correctly. If the instrument is currently unavailable,
you can either ignore the error or temporarily comment out the corresponding worker definition in `docker-compose.yaml`.
Once the instrument is available again, uncomment the worker definition and restart the container.

Sometimes, substituting `--build` with `--build --force-recreate` in the `docker compose` command helps
resolve mounting problems.


### Problem: a lot of tasks are in the 'scheduled' state
This can be seen in the Airflow UI -> Admin -> Pools.
If "Scheduled Slots" is large, but "Running Slots" reasonable small (less than 32 per scheduler)
then it might help to give the scheduler a fresh start on the PC/VM hosting the infrastructure:
```bash
./compose.sh up airflow-scheduler --build --force-recreate -d
```

### Problem: a `copy_raw_file` task is stuck
Symptom: the `copy_raw_file` task gets stuck when checking whether the file
is already present on the pool backup. In addition, `ls` from the
PC hosting the worker on pool backup folder containing the file does not
return (from the cluster, it does).

#### Solution
Most likely, this is because the file copying got interrupted, e.g. due to manual restart of the worker.
To resolve, move the file from the backup pool folder and restart the `copy_raw_file` task.


### Problem: Tasks fail with "task instance .. finished with state failed"
More specific, the Airflow logs show
`ERROR - The executor reported that the task instance <TaskInstance: instrument_watcher.test1.wait_for_raw_file_creation ...> finished with state failed, but the task instance's state attribute is queued.`

#### Solution
Most likely, the code that the task should run is broken (e.g. due to an import or configuration error). Look at the
respective worker's container logs to find out the root cause.


## Useful commands

### Some useful MongoDB commands
Log into the DB as admin user:
```
./compose.sh exec mongodb-service mongosh -u <MONGO_INITDB_ROOT_USERNAME> -p <MONGO_INITDB_ROOT_PASSWORD>
```
using the credentials from `envs/.env-mongo`. Then one can execute e.g. the commands in `init-mongo.sh`.


Query to find all files for a given instrument with a given status that are younger than a given date
```
{ $and: [{status:"error"}, {instrument_id:"test1"}, {created_at_: {$gte: new ISODate("2024-06-27")}}]}
```

### Some useful Docker commands

See state of containers
```bash
docker ps
```

See state of mounts
```bash
df -h
```

Instead of interacting with multiple services (by using `--profile`), you can also interact only with individual services,
e.g.
```bash
./compose.sh up airflow-worker-test1 --build -d
./compose.sh down airflow-worker-test1
```

If you encounter problems with mounting or any sorts of caching issues, try to replace
`--build` with `--build --force-recreate`.

Bring down all containers
```
./compose.sh --profile "*" down
```

Sometimes, a container would refuse to stop ("Error while Stopping"):
to force kill it, get its `ID` from the above command and kill it manually
```bash
ID=<ID of container, e.g. 8fb6a5985>
sudo kill -9 $(ps ax | grep $ID | grep -v grep | awk '{print $1}')
```

Watch logs for a given service (don't confuse with the Airflow logs)
```bash
./compose.sh logs airflow-worker-test1 -f
```

Start bash in a given service container
```bash
./compose.sh exec airflow-worker-test1 bash
```

Clean up all containers, volumes, and images
```bash
./compose.sh down --volumes  --remove-orphans --rmi
```

As a last resort, try restarting Docker (this will kill all running containers!)
```
systemctl restart docker
```
or the whole machine:
```
sudo reboot now
```

## Cluster load management
For each acquired file, a processing job on the Slurm cluster will be scheduled. If, for any reason, the number of
concurrently submitted jobs should be limited, set the size of the `cluster_slots_pool`
(in the Airflow UI under "Admin" -> "Pools") accordingly. Note that this setting does not affect
jobs that have already been submitted and thus may take a while to take effect.


## Upgrading Airflow
Every once in a while, the Airflow version should be updated.

1. Create a backup copy of the `mongodb_data_${ENV}` and `airflowdb_data_${ENV}` folders (on the machine that hosts the DBs).
2. Locate the current version in the Airflow Dockerfile, line `ARG AIRFLOW_VERSION=2.10.5`
3. Check for breaking changes between the current and the new version [here](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)
and adapt the code if necessary.
4. Search and replace the old version string (e.g. `2.10.5`) with the new version throughout the code.
5. Shutdown all workers and infrastructure (up to the databases), generally following the instructions on how to deploy new code versions [here](deployment.md#deploying-new-code-versions).
6. Migrate the Airflow DB: `./compose.sh run airflow-cli db migrate`
7. Spin up workers and infrastructure again.
