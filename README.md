# Lake Sandbox

A sandbox and example for timeseries data generation, reorganization, and validation with Delta Lake integration.

## Features

- **Timeseries Generation**: Generate synthetic timeseries data for multiple UTM tiles with parcel IDs, NDVI, spectral bands, and meteorological data
- **Data Reorganization**: Convert date-partitioned data to parcel-chunk-partitioned format with hash-based distribution
- **Delta Lake Integration**: Convert reorganized data to a single partitioned Delta Lake table for ACID transactions and time travel
- **Data Validation**: Comprehensive validation of raw, organized, and Delta Lake data with completeness checks
- **Data Analysis**: Query individual parcels, sample random data, and analyze partitioning performance
- **Pipeline Orchestration**: End-to-end pipeline using DLT for execution tracking and monitoring
- **Performance Monitoring**: Built-in performance monitoring with memory usage and execution time tracking

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd lake-sandbox

# Install with uv (recommended)
uv sync
```

## Quick Start

### Full Pipeline (Recommended)

Run the complete data pipeline from generation to validation:

```bash
# Default pipeline (500k parcels per tile, 2 tiles, 106 days)
uv run lake-sandbox pipeline

# Custom configuration
uv run lake-sandbox pipeline \
  --num-parcels 1000000 \
  --utm-tiles "32TNR,32TPR,32TNS" \
  --start-date 2024-01-01 \
  --end-date 2024-06-30 \
  --chunk-size 10000
```

### Validation Only

Run validation on existing data without regenerating:

```bash
uv run lake-sandbox pipeline --validate-only
```

## Individual Commands

### 1. Timeseries Generation

Generate synthetic timeseries data partitioned by UTM tile, year, and date:

```bash
# Basic generation
uv run lake-sandbox generate-timeseries

# Custom parameters
uv run lake-sandbox generate-timeseries \
  --output-dir ./data/raw \
  --utm-tiles "32TNR,32TPR,32TNS" \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --num-parcels 250000
```

**Output Structure:**
```
./output/timeseries-raw/
├── utm_tile=32TNR/
│   └── year=2024/
│       ├── date=2024-01-01/
│       │   └── data.parquet
│       ├── date=2024-01-02/
│       │   └── data.parquet
│       └── ...
└── utm_tile=32TPR/
    └── year=2024/
        └── ...
```

### 2. Data Reorganization

Convert from date-partitioned to parcel-chunk-partitioned format and Delta Lake:

```bash
# Full reorganization (reorganize + delta conversion + optimization)
uv run lake-sandbox reorg

# Individual phases
uv run lake-sandbox reorg --phase reorg      # Only reorganization
uv run lake-sandbox reorg --phase delta      # Only Delta conversion  
uv run lake-sandbox reorg --phase optimize   # Only optimization

# Custom directories and chunk size
uv run lake-sandbox reorg \
  --input-dir ./data/raw \
  --output-dir ./data/organized \
  --delta-dir ./data/delta \
  --chunk-size 5000 \
  --force
```

**Output Structure:**
```
./output/timeseries-organized/
├── parcel_chunk=00/
│   └── data.parquet        # ~10k parcels, all dates
├── parcel_chunk=01/
│   └── data.parquet
└── ...

./output/timeseries-delta/
└── parcel_data/            # Single partitioned Delta Lake table
    ├── _delta_log/
    ├── parcel_chunk=00/
    │   └── *.parquet
    ├── parcel_chunk=01/
    │   └── *.parquet
    └── ...
```

### 3. Data Validation

Validate data integrity and completeness:

```bash
# Validate all targets (raw + organized + delta)
uv run lake-sandbox validate --target both

# Validate specific targets
uv run lake-sandbox validate --target raw
uv run lake-sandbox validate --target organized  
uv run lake-sandbox validate --target delta

# With custom parameters
uv run lake-sandbox validate \
  --target organized \
  --raw-dir ./data/raw \
  --organized-dir ./data/organized \
  --expected-dates 106 \
  --verbose
```

**Validation Checks:**
- **Raw**: Correct number of unique parcels across all files
- **Organized**: Data completeness (each parcel has all expected dates), no parcel overlaps between chunks
- **Delta**: Partitioned table integrity, parcel-date combinations completeness, no parcel overlaps between partitions

### 4. Check Status

Monitor reorganization progress:

```bash
uv run lake-sandbox reorg --status
```

## Pipeline Configuration

### Directory Structure

The pipeline uses the following default directory structure:

```
./output/
├── timeseries-raw/          # Generated raw data (date-partitioned)
├── timeseries-organized/    # Reorganized data (parcel-chunk-partitioned)  
└── timeseries-delta/        # Single partitioned Delta Lake table
    └── parcel_data/         # Partitioned by parcel_chunk
```

### Key Parameters

| Parameter         | Default       | Description                           |
|-------------------|---------------|---------------------------------------|
| `--num-parcels`   | 500,000       | Parcels to generate per UTM tile      |
| `--utm-tiles`     | "32TNR,32TPR" | Comma-separated UTM tile identifiers  |
| `--start-date`    | "2024-01-01"  | Start date for timeseries             |
| `--end-date`      | "2024-04-15"  | End date for timeseries (106 days)    |
| `--chunk-size`    | 10,000        | Parcels per reorganized chunk         |
| `--force`         | False         | Force reprocessing existing files     |
| `--skip-existing` | True          | Skip pipeline stages if output exists |

## Data Schema

### Generated Timeseries Data

Each parquet file contains the following columns:

```python
{
    'parcel_id': str,           # Unique parcel identifier
    'date': date,               # Date of observation
    'ndvi': float,              # Normalized Difference Vegetation Index
    'evi': float,               # Enhanced Vegetation Index  
    'red': int,                 # Red band reflectance
    'nir': int,                 # Near-infrared band reflectance
    'blue': int,                # Blue band reflectance
    'green': int,               # Green band reflectance
    'swir1': int,               # Short-wave infrared 1 reflectance
    'swir2': int,               # Short-wave infrared 2 reflectance
    'temperature': float,       # Temperature (°C)
    'precipitation': float,     # Precipitation (mm)
    'cloud_cover': float,       # Cloud cover percentage (0-100)
    'geometry_area': float      # Parcel area (hectares)
}
```

## Advanced Usage

### Custom Validation with Expected Dates

```bash
# Auto-detect expected dates from raw data
uv run lake-sandbox validate --target organized --raw-dir ./output/timeseries-raw

# Manually specify expected dates
uv run lake-sandbox validate --target organized --expected-dates 106
```

### Pipeline with Custom DLT Destination

```bash
# Use filesystem destination instead of DuckDB
uv run lake-sandbox pipeline --destination filesystem

# Use custom pipeline name
uv run lake-sandbox pipeline --pipeline-name my_custom_pipeline
```

### Force Reprocessing

```bash
# Force regeneration of all data
uv run lake-sandbox pipeline --force --no-skip-existing

# Force only specific phases
uv run lake-sandbox reorg --force --phase delta
```

## Performance Monitoring

All commands include built-in performance monitoring that tracks:

- **Execution Time**: Total time for command execution
- **Memory Usage**: Initial, peak, and delta memory consumption
- **CPU Usage**: Average CPU utilization during execution

### Benchmark Results (MacBook Pro M1)

**Test Configuration:**
```bash
uv run lake-sandbox pipeline \
  --num-parcels 1000000 \
  --utm-tiles "32TNR,32TPR,32TNS" \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --chunk-size 10000
```

**Dataset:** 1M parcels × 3 UTM tiles × 365 days = ~159M records after deduplication

| Stage | Duration | Memory Peak | CPU Usage | Output Size |
|-------|----------|-------------|-----------|-------------|
| **Timeseries Generation** | 1.1 min | - | 180% | 15 GB |
| **Phase 1: Reorganization** | 13.5 min | 1.2 GB | 138% | 15 GB |
| **Phase 2: Delta Conversion** | 1.0 min | 1.9 GB | 187% | 5.1 GB |
| **Phase 3: Optimization** | <1 sec | 1.9 GB | 80% | - |
| **Validation** | 15 sec | 2.2 GB | 375% | - |
| **Total Pipeline** | **17.0 min** | **2.2 GB** | **149%** | **20.1 GB** |

**Key Insights:**
- **67% deduplication**: Delta Lake (5.1 GB) vs organized chunks (15 GB) due to removing duplicates
- **Memory efficiency**: Peak usage only 2.2 GB for 159M records
- **CPU utilization**: Excellent multi-core performance (>100% indicates parallel processing)
- **Validation speed**: Cross-validation of 100 partitions in 15 seconds

**Storage Breakdown:**
```
output/timeseries-raw/        15G  (Raw parquet files)
output/timeseries-organized/  15G  (Reorganized chunks with possibleduplicates)  
output/timeseries-delta/     5.1G  (Deduplicated partitioned Delta table)
```

Example detailed output:
```
============================================================
PERFORMANCE SUMMARY: convert_to_delta_lake
============================================================
Execution Time: 62.09 seconds
    Memory Usage:
   • Initial: 1043.0 MB
   • Peak: 1856.8 MB
   • Delta: +813.4 MB
CPU Usage: 187.3%
============================================================
```

## Troubleshooting

### Common Issues

1. **Missing directories**: Ensure parent directories exist or use `--force` to create them
2. **Permission errors**: Check write permissions for output directories
3. **Memory issues**: Reduce `--chunk-size` or `--num-parcels` for large datasets
4. **Validation failures**: Use `--verbose` flag to see detailed validation issues

### Validation Errors

- **"Too many parcels"**: Normal with hash-based partitioning, indicates slightly uneven distribution
- **"Missing dates"**: Parcels missing data for some dates, check raw data completeness
- **"Parcel overlap"**: Parcels appearing in multiple partitions, indicates reorganization issue

### Getting Help

```bash
# Command-specific help
uv run lake-sandbox pipeline --help
uv run lake-sandbox generate-timeseries --help
uv run lake-sandbox reorg --help
uv run lake-sandbox validate --help

# List all available commands
uv run lake-sandbox --help
```

## Data Analysis and Querying

### Query Individual Parcels

Query and display specific parcel timeseries data:

```bash
# Query specific parcel
uv run lake-sandbox query-parcel --parcel-id parcel_00001

# Query random parcel
uv run lake-sandbox query-parcel --random

# Export to CSV
uv run lake-sandbox query-parcel --parcel-id parcel_00001 --export results.csv

# Show detailed summary
uv run lake-sandbox query-parcel --parcel-id parcel_00001 --show-summary
```

### Sample Random Data

Sample random parcels within random time windows (useful for ML training):

```bash
# Sample 10 parcels in 7-day window
uv run lake-sandbox query-sample --num-parcels 10 --window-days 7

# Sample with different parameters
uv run lake-sandbox query-sample --num-parcels 20 --window-days 14 --seed 42

# Use custom delta directory
uv run lake-sandbox query-sample --delta-dir ./custom/delta --num-parcels 5
```

### Analyze Partitioning Performance

Analyze Delta Lake partitioning efficiency and query performance:

```bash
# Basic partitioning analysis
uv run lake-sandbox analyze-partitioning

# Test more parcels for better statistics
uv run lake-sandbox analyze-partitioning --num-test-parcels 20

# Use custom delta directory
uv run lake-sandbox analyze-partitioning --delta-dir ./custom/delta
```

This analysis provides:
- Partition distribution statistics
- Query performance benchmarks for parcel_id lookups
- Partition pruning effectiveness
- Optimization recommendations for different workloads

### Full Data Analysis

For comprehensive data analysis with visualizations:

```bash
# Basic analysis with plots
uv run lake-sandbox analyze

# Custom analysis parameters
uv run lake-sandbox analyze --sample-parcels 20 --output-dir ./plots --show-plots
```

See [examples/README.md](src/lake_sandbox/examples/README.md) for detailed analysis documentation.

## Examples

### Small Test Dataset

```bash
# Generate small test dataset (faster execution)
uv run lake-sandbox pipeline \
  --num-parcels 10000 \
  --utm-tiles "32TNR" \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --chunk-size 1000
```

### Large Production Dataset

```bash
# Generate large production dataset
uv run lake-sandbox pipeline \
  --num-parcels 2000000 \
  --utm-tiles "32TNR,32TPR,32TNS,32TPS" \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --chunk-size 20000
```

### Validation-Only Workflow

```bash
# Run validation on existing data
uv run lake-sandbox pipeline --validate-only --target both --verbose
```

## Architecture

### Data Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Raw Generation │───▶│ Reorganization  │───▶│   Validation    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
│                      │                      │                 │
│ • UTM tile/date      │ • Parcel chunks     │ • Data integrity │
│   partitioned        │ • Hash distribution │ • Completeness   │
│ • 500k parcels/tile  │ • 10k parcels/chunk │ • No overlaps    │
│ • 106 dates          │ • Delta Lake format │ • Expected dates │
└──────────────────────┴─────────────────────┴─────────────────┘
```

### Pipeline Stages

1. **Stage 1: Timeseries Generation**
   - Generates synthetic data for multiple UTM tiles
   - Creates date-partitioned parquet files
   - Each tile gets specified number of parcels

2. **Stage 2: Reorganization**
   - Converts from date-partitioned to parcel-chunk-partitioned
   - Uses hash-based distribution for balanced chunks
   - Converts to single partitioned Delta Lake table for ACID properties and automatic chunking

3. **Stage 3: Validation**
   - Validates partitioned Delta table completeness and integrity
   - Checks for parcel overlaps between partitions
   - Ensures each parcel has all expected dates

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
