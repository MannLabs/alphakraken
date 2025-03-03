## Actions to take after a machine reboot
Currently, there is some action needed after a reboot of the local worker PC. This could be
avoided by using permanent mounts.

#### Restart of PC/VM hosting the airflow infrastructure
0. `ssh` into the PC/VM, `cd` to the alphakraken source directory, and set `export ENV=sandbox` (`production`).

1. Start the docker service
```bash
sudo systemctl start docker
```

2. Remount the `airflow_logs` folder (using the `kraken-read` user):
```bash
./mount.sh logs
```

3. Run the airflow infrastructure and MongoDB services
```bash
./compose.sh --profile infrastructure up --build -d
```
and check container health using `sudo docker ps`.

Then, Airflow UI is accessible at http://hostname:8080/ and the Streamlit webapp at http://hostname:8501/.


#### Restart of PC/VM hosting the workers
0. `ssh` into the PC/VM, `cd` to the alphakraken source directory, and set `export ENV=sandbox` (`production`).

1. Start the docker service
```bash
sudo systemctl start docker
```

2. Set up all mounts for all instruments (`test2`, ..) and the other folders:
```bash
for entity in test2 test3 backup output logs; do
  ./mount.sh $entity
done
```
You will be prompted for the password for each mount.
When mounting `backup` you must use the `kraken-write` user with full write access (can also be used for `output` and `logs`).

3. Run the worker containers (sandbox/production version)
```bash
./compose.sh --profile workers up --build -d
```
which spins up on worker service for each instrument,
and check container health using `sudo docker ps`.




## Airflow Variables
These variables are set in the Airflow UI under "Admin" -> "Variables". They steer the behavior of the whole system,
so be careful when changing them. If in doubt, pause all DAGs that are not part of the current problem before changing them.

### min_free_space_gb (default: -1)
If set to a positive number (unit: GB), the `file_remover` will remove files (oldest first) from the
instrument backup folder until the free space is above this threshold.
The value of `min_file_age_to_remove_in_days` (see below) is taken into account when
selection files to remove.

Recommended setting in production: `300` (big enough to avoid running out of space over a weekend
(in the worst case scenario that the file_remover stops running on Friday).

### min_file_age_to_remove_in_days (default: 14)
The minimum file age in days for files to be removed by the file_remover.

Recommended setting in production: `14` (default)

### allow_output_overwrite (default: False)
If set to `True`, the system will overwrite existing output files. Convenience switch to avoid manual deletion of output files
in case something went wrong with the quanting.

Recommended setting in production: False (default)

### backup_overwrite_file_id (default: None)
In case the `file_copy` task is interrupted (e.g. manually) while a file is being copied,
simply restarting it will not help, as the partially copied file will not be overwritten
due to a security mechanism.
In this case, set the `backup_overwrite_file_id` variable to the file _id_ (not: file _name_), i.e. including
potential collision flags, and restart the `file_copy` task. The overwrite protection
will be deactivated just for this file id and the copying should succeed.


### debug_no_cluster_ssh (default: False)
`debug_no_cluster_ssh` If set to `True`, the system will not connect to the SLURM cluster. This is useful for
testing, debugging and to avoid flooding the cluster at the initial setup.

Recommended setting in production: False (default)

### debug_max_file_age_in_hours (default: -1)
If set, files that are older will not be processed by the acquisition handler, i.e. not be backed up nor quanted.
They will nevertheless be added to the DB, with status="ignored".
Needs to be a valid float, "-1" to deactivate.

Recommended setting in production: -1 (default)


## Troubleshooting

See also  .

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



## Useful commands

### Some useful MongoDB commands
Find all files for a given instrument with a given status that are younger than a given date
```
{ $and: [{status:"error"}, {instrument_id:"test2"}, {created_at_: {$gte: new ISODate("2024-06-27")}}]}
```

### Some useful Docker commands
Instead of interacting with multiple services (by using `--profile`), you can also interact only with individual services,
e.g.
```bash
./compose.sh down airflow-worker-test2
./compose.sh up airflow-worker-test2 --build -d
```

See state of containers
```bash
docker ps
```

Sometimes, a container would refuse to stop ("Error while Stopping"):
to force kill it, get its `ID` from the above command and kill it manually
```bash
ID=<ID of container, e.g. 8fb6a5985>
sudo kill -9 $(ps ax | grep $ID | grep -v grep | awk '{print $1}')
```

See state of mounts
```bash
df
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

If you encounter problems with mounting or any sorts of caching issues, try to replace
`--build` with `--build --force-recreate`.

As a last resort, try restarting Docker (this will kill all running containers!)
```
systemctl restart docker
```
or the whole machine:
```
sudo reboot now
```

### Cluster load management
For each acquired file, a processing job on the SLURM cluster will be scheduled. If, for any reason, the number of
concurrently submitted jobs should be limited, set the size of the `cluster_slots_pool`
(in the Airflow UI under "Admin" -> "Pools") accordingly. Note that this setting does not affect
jobs that have already been submitted and thus may take a while to take effect.
