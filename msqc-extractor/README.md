# MS Quality Control Metrics Extractor

Extract performance metrics and TIC data from Thermo (.raw), Bruker timsTOF (.d) and SCIEX (.wiff)
mass spectrometry files.

## Usage

```bash
python main.py <raw_file_path> <output_path> <num_threads>
```

`num_threads` is only used for Bruker files.

## Usage in a container

The container is how AlphaKraken runs this tool in standalone deployments (`docker` job engine,
cf. `airflow_src/plugins/jobs/docker_job_handler.py`). Build it from the repo root:

```bash
./compose.sh --profile msqc build
```

The entrypoint takes its arguments from the environment, like `misc/software/run_msqc.sh` does:

```bash
docker run --rm --network none \
    -e RAW_FILE_PATH=/data/in/<raw_file> -e OUTPUT_PATH=/data/out -e NUM_THREADS=2 \
    -v <host input folder>:/data/in:ro -v <host output folder>:/data/out:rw \
    alphakraken-msqc:latest
```

The image reads Thermo files via the `coreclr` .NET runtime, which means it needs no `mono`
installation but also **cannot read SCIEX .wiff files** — those require `mono` or Windows.

## Outputs

- `msqc_results.tsv`: Performance metrics (median injection times, scan counts, median TIC values)
- `msqc_tic.tsv`: Total Ion Current data per spectrum

## Supported Formats

- Thermo: `.raw`
- Bruker timsTOF: `.d`
- SCIEX: `.wiff` (not in the container, see above)

## Dependencies

Install with: `pip install -r requirements.txt`
