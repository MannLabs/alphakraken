# AlphaKraken

A fully automated data processing and analysis system for mass spectrometry experiments:
- monitors acquisitions on mass spectrometers
- copies raw data to a backup location
- runs AlphaDIA on every sample and provides metrics in a web application

<img src="docs/overview.jpg" alt="overview" style="max-width: 600px;"/>

## Important note for users

To not interfere with the automated processing, please stick to the following simple rule:

> Do not touch the acquisition folders*!

*i.e. the folders where the acquisition software writes the raw files to. In particular,
do not **delete**, **rename** or **open** files in these folders and do not **copy** or **move** files *from* or *to* these folders!

Regular users should find all required documentation in the [AlphaKraken WebApp](http://<kraken_url>).
The rest of this documentation is relevant only for developers and administrators.


## System Requirements

The following architecture is required to run AlphaKraken: a local PC, a compute cluster, and a shared file system.

**Local PC**: at least one local PC with a recent linux distribution, 16GB+ RAM and 4+ CPU cores,
with network access to the acquisition PCs, to the shared file system and to the compute cluster.

**Distributed compute environment**: compute cluster with SLURM workload manager that can be accessed via SSH.

**Shared File System**: a file system that can be accessed from the local PC and the compute cluster.


## Quick start
For the impatient:

```bash
git clone https://github.com/MannLabs/alphakraken.git && cd alphakraken
echo -e "AIRFLOW_UID=$(id -u)" > envs/.env-airflow
ENV=local && export ENV=$ENV
./compose.sh --profile infrastructure up airflow-init
./compose.sh --profile local up --build -d
```

After startup, the airflow webserver runs on http://localhost:8080/ (default credentials: airflow/airflow), the Streamlit webapp on http://localhost:8501/ .

See the [deployment.md](docs/deployment.md) for detailed instructions and [development.md](docs/development.md) for
instructions on how to try the system locally.



## Documentation Structure

For detailed information, please refer to the following documentation files:

- [deployment.md](docs/deployment.md) - Setup and deployment instructions
- [instruments.md](docs/instruments.md) - Adding and configuring instruments
- [development.md](docs/development.md) - Development setup and testing procedures
- [maintenance.md](docs/maintenance.md) - Maintenance procedures and post-reboot instructions, common issues and solutions

---

## About

An open-source Python package of the AlphaX ecosystem from the [Mann Labs at the Max Planck Institute of Biochemistry](https://www.biochem.mpg.de/mann).

---

## License

AlphaDIA was developed by the [Mann Labs at the Max Planck Institute of Biochemistry](https://www.biochem.mpg.de/mann) and is freely available with an [Apache License](LICENSE.txt).
External Python packages have their own licenses, which can be consulted on their respective websites.

---
