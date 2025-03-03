
## Deployment
This guide is both valid for a local setup (without connection to pool or cluster), and for sandbox/production setups.
Upfront, set an environment variable `ENV`, which is either `local`, `sandbox`, or `production`, e.g.
```bash
ENV=local && export ENV=$ENV
```
This will use the environment variables defined in `envs/${ENV}.env` and point shell scripts to the correct configuration.

### Deployment workflow: 'local' vs. 'sandbox' vs. 'production'
All features should be tested on `local` before deploying them to the `sandbox` environment
for further testing. `sandbox` is technically equivalent to `production`, but it does not contain any valuable
data and therefore it's perfectly fine to break and/or wipe it.
There, depending on the scope of the feature, and of the likeliness of breaking something,
another test with real data might be necessary.

Use common sense when deciding the scope of testing:
For instance, if you correct a typo in the webapp, you might well skip the sandbox testing.
In contrast, a new feature that changes the way data is processed should definitely be tested in the sandbox environment.

Only a well-tested feature should be deployed to production. Make sure a pull request is always self-contained
and 'shippable', i.e. deployment to production should be possible at any time.

### Initial deployment
All commands in this Readme assume you are in the root folder of the repository.
Please note that running and developing the alphakraken is only tested for MacOS and Linux
(the UI can be accessed from any OS, of course).

#### One-time initializations
The following steps are required for both the `local` and the `sandbox`/`production` deployments.
For the latter, additional steps are required, see [here](#additional-steps-required-for-initial-sandboxproduction-deployment).

1. Install [Docker](https://docs.docker.com/engine/install/ubuntu/) and `python3`.

2. Clone the repository into a folder and `cd` into it.

3. Set the current user as the user within the airflow containers and get the correct permissions on the "logs"
directory (otherwise, `root` would be used)
```bash
echo -e "AIRFLOW_UID=$(id -u)" > envs/.env-airflow
```

4. On the PC that will host the internal airflow database (not the MongoDB!) run
```bash
./compose.sh --profile infrastructure up airflow-init
```
Note: depending on your operating system and configuration, you might need to run `docker compose` command with `sudo`.

Now run the containers (see below).
The following initialization steps need to be done once the containers are up:

5. In the Airflow UI, set up the SSH connection to the cluster (see [below](#setup-ssh-connection)).
If you don't want to connect to the cluster, just create the connection of type
"ssh" and name "cluster_ssh_connection" with some dummy values for host, username, and password.
In this case, make sure to set the Airflow variable `debug_no_cluster_ssh=True` (see below).

6. In the Airflow UI, set up the required Pools (see [below](#setup-required-pools)).

#### Run the containers (local version)
Start all docker containers required for local testing with
```bash
./compose.sh --profile local up --build -d
```
After startup, the airflow webserver runs on http://localhost:8080/ (default credentials: `airflow`/`airflow`), the Streamlit webapp on http://localhost:8501/ .

To spin all containers down again, use
```bash
./compose.sh --profile local down
```
A more graceful 'warm shutdown' can be achieved by
```bash
./compose.sh --profile local stop
```

See below for [some useful Docker commands](#some-useful-docker-commands).


### Additional steps required for initial sandbox/production deployment

The main differences between the `local` and the `sandbox`/`production` deployments are:
- `local` has all services running on the same machine within the same docker-compose network,
whereas `sandbox`/`production` is per default distributed over two machines
- `sandbox`/`production` needs additional steps to configure the cluster and the network bind mounts

The different services can be distributed over several machines. The only important thing is that there
it exactly one instance of each of the 'central components': `postgres-service`, `redis-service`, and `mongodb-service`.
One reasonable setup is to have the airflow infrastructure and the MongoDB service on one machine,
and all workers on another. This is the current setup in the docker-compose, which is reflected by the
profiles `infrastructure` and `workers`, respectively. If you move one of the central components
to another machine, you might need to adjust the `*_HOST` variables in the
`./env/${ENV}.env` files (see comments there). Of course, one machine could also host them all.

Make sure that the time is in sync between all machines, e.g. by using the same time server.

For production: set strong passwords for `AIRFLOW_PASSWORD`, `MONGO_PASSWORD`, and `POSTGRES_PASSWORD`
in `./env/production.env` and `MONGO_INITDB_ROOT_PASSWORD` in `./env/.env-mongo`.
Make sure they don't contain weird characters like '\' or '#' as they might interfere with name resolution in `docker-compose.yaml`.

#### Required users
Two different users are recommended for the deployment:
one ("`kraken-write`") that has write access to the `backup` pool folder,
and one  ("`kraken-read`") that has only _read_ access.
Both users should be able to write to the `logs` and `output` directories, and to read from `settings`.

#### On all machines
1. Disable the automatic restart of the Docker service
([cf. here](https://docs.docker.com/engine/install/linux-postinstall/#configure-docker-to-start-on-boot-with-systemd))
```bash
sudo systemctl disable docker.service
sudo systemctl disable docker.socket
sudo systemctl disable containerd.service
```
This is currently required, as manual work is needed anyway after a machine reboot
(see [below](#things-to-do-after-a-machine-reboot))
and thus the automated start of containers is not desired.

#### On the PC (VM) hosting the airflow infrastructure
0. `ssh` into the PC/VM, `cd` to the alphakraken source directory, and set `export ENV=sandbox` (`production`).

1. Set up the [pool bind mounts](#set-up-pool-bind-mounts) for `airflow_logs` only. Here, the logs of the individual task runs will be stored
for display in the Airflow UI.

2. Follow the steps for after a restart [below](#restart-of-pcvm-hosting-the-airflow-infrastructure).

#### On the PC (VM) hosting the workers
0. `ssh` into the PC/VM, `cd` to the alphakraken source directory, and set `export ENV=sandbox` (`production`).

1. Set up the [pool bind mounts](#set-up-pool-bind-mounts) for all instruments and `logs`, `backup` and `output`.

2. Follow the steps for after a restart [below](#restart-of-pcvm-hosting-the-workers).


#### On the cluster
1. Log into the cluster using the `kraken-write` user.
2. Create this directory
```bash
mkdir -p ~/slurm/jobs
```
3. Copy the cluster run script `submit_job.sh` to `~/slurm`.
Make sure to update also this file when deploying a new version of the AlphaKraken.

4. Set up AlphaDIA  (see [below](#setup-alphadia)).

### General note on how Kraken gets to know the data

Each worker needs two 'views' on the pool backup and output data.

The first view enables read/write access,
by mounting on the Kraken host PC a specific (network) folder (e.g. `\\pool-backup\pool-backup` or `\\pool-output\pool-output`)
using `cifs` mounts (wrapped by `mount.sh`)
to a target folder and then mapping this target folder to a worker container
in `docker-compose.yaml`.

The second view is the location of the data as seen from the cluster
(e.g. `/fs/pool/pool-backup` or `/fs/pool/pool-output`),
which is required to set the paths for the cluster jobs correctly.

For instruments, only the first type of view is required, as the cluster does not access the instruments directly.


### Set up pool bind mounts
The workers need bind mounts set up to the pool filesystems for backup and reading AlphaDIA output data.
All airflow components (webserver, scheduler and workers) need a bind mount to a pool folder to read and write airflow logs.
Additionally, workers need one bind mount per instrument PC is needed (cf. section below).

0. (on demand) Install the `cifs-utils` package (otherwise you might get errors like
`CIFS: VFS: cifs_mount failed w/return code = -13`)
```bash
sudo apt install cifs-utils
```

1. Create folders `settings`, `output`, and `airflow_logs` in the desired pool location(s), e.g. under `/fs/pool/pool-alphakraken`.

2. Make sure the variables `MOUNTS_PATH`, `BACKUP_POOL_FOLDER` `QUANTING_POOL_FOLDER` in the `envs/${ENV}.env` file are set correctly.
Set them also in the `envs/alphakraken.$ENV.yaml` file (section: `locations`).

3. Mount the backup, output and logs folder. You will be asked for passwords.
```bash
./mount.sh backup
./mount.sh output
./mount.sh logs
```

Note: for now, user `kraken-write` should only have read access to the backup pool folder, but needs `read/write` on the `output`
folder. If you need to remount one of the folders, add the `umount` option, e.g.
`./mount.sh output umount`.

IMPORTANT NOTE: it is absolutely crucial that the mounts are set correctly (as provided by the `envs/alphakraken.$ENV.yaml` file)
as the workers operate only on docker-internal paths and cannot verify the correctness of the mounts.

### Setup SSH connection
This connection is required to interact with the SLURM cluster.

1. Open the Airflow UI, navigate to "Admin" -> "Connections" and click the "+" button.
2. Fill in the following fields:
    - Connection Id: `cluster_ssh_connection`
    - Conn Type: `SSH`
    - Host: `<cluster_head_node_ip>`  # the IP address of a cluster head node, in this case `<cluster_head_node>`
    - Username: `<user name of user kraken-read>`
    - Password: `<password of user kraken-read>`
3. (optional) Click "Test" to verify the connection.
4. Click "Save".
Note: make sure to use the `kraken-read` user with read-only access to the backup pool folder.

### Setup required pools
Pools are used to limit the number of parallel tasks for certain operations. They are managed via the Airflow UI
and need to be created manually once.
1. Open the Airflow UI, navigate to "Admin" -> "Pools".
2. For each pool defined in `settings.py:Pools`, create a new pool with a sensible value (see suggestions in the `Pools` class).

### Setup AlphaDIA on the cluster
For details on how to install AlphaDIA on the SLURM cluster, follow the AlphaDIA
[https://github.com/MannLabs/alphadia/blob/main/docs/installation.md#slurm-cluster-installation](Readme).

In a nutshell, to install a certain version, e.g. `VERSION=1.7.0`:

1. Log in (make sure to use the same user as configured in the [SSH connection](#setup-ssh-connection)!)
```bash
ssh kraken-read@<cluster_head_node>
```
2. create a new conda environment and activate it
```bash
conda create --name alphadia-${VERSION} python=3.11 -y && conda activate alphadia-${VERSION}
```
3. Install desired version
```bash
pip install "alphadia[stable]==${VERSION}"
```
Make sure the environment is named `alphadia-${VERSION}`, as this is the scheme that is expected by the module starting
the AlphaDIA jobs.
Also, don't forget to install `mono` (cf. AlphaDIA Readme).


### Deploying new code versions
These steps need to be done on all machines that run alphakraken services.
Make sure the code is always consistent across all machines!
0. If in doubt, create a  backup copy of the `mongodb_data_$ENV` and `airflowdb_data_$ENV` folders (on the machine that hosts the DBs).
1. On each machine, pull the most recent version of the code from the repository using `git pull` and a personal access token.
2. Check if there are any special changes to be done (e.g. updating `submit_job.sh` on the cluster,
new mounts, new environment variables, manual database interventions, ..) and apply them.
3. (when deploying workers) To avoid copying processes being interrupted, in the Airflow UI set the size of the `file_copy_pool` to 0 and wait until all `copy_raw_file` tasks are finished.
4. Stop all docker compose services that need to be updated across all machines using the `./compose.sh --profile <some profile> stop` command.
5. Start all docker compose services again, first the `infrastructure` services, then the `workers` services.
6. Set the size of `file_copy_pool` to the number it was before.
7. Normal operation should be resumed after about 5 minutes. Depending on when they were shut down, some tasks
could be in an `error` state though. Check after a few hours if some files are stuck and resolve the issues with the Airflow UI.


### Things to do after a machine reboot
Currently, there is some action needed after a reboot. This could be
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
