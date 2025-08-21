# MS Quality Control Metrics Extractor

Extract performance metrics and TIC data from Thermo (.raw) and Bruker timsTOF (.d) mass spectrometry files.

## Usage

```bash
python main.py <raw_file_path> <output_path>
```

## Outputs

- `msqc_results.tsv`: Performance metrics (median injection times, scan counts, median TIC values)
- `msqc_tic.tsv`: Total Ion Current data per spectrum

## Supported Formats

- Thermo `.raw` files
- Bruker timsTOF `.d` directories

## Dependencies

Install with: `pip install -r requirements.txt`
