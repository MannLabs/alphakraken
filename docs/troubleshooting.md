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
