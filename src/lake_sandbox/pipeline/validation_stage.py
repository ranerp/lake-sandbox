"""DLT pipeline stage for data validation."""

import dlt
from typing import Iterator, Dict, Any
from pathlib import Path
import typer

from lake_sandbox.validator.validation import validate
from lake_sandbox.utils.performance import monitor_performance


@dlt.resource(name="validation")
@monitor_performance()
def validation_resource(
    target: str = "both",
    organized_dir: str = "./output/timeseries-organized",
    delta_dir: str = "./output/timeseries-delta", 
    raw_dir: str = "./output/timeseries-raw",
    expected_total_parcels: int = 500_000,
    expected_chunk_size: int = 10_000,
    expected_tiles: int = 2,
    expected_dates: int = None
) -> Iterator[Dict[str, Any]]:
    """DLT resource for validating processed data.
    
    Args:
        target: What to validate ('raw', 'organized', 'delta', or 'both')
        organized_dir: Directory with organized parquet chunks
        delta_dir: Directory with Delta Lake tables  
        raw_dir: Directory with raw timeseries data
        expected_total_parcels: Expected total unique parcels
        expected_chunk_size: Expected number of parcels per chunk
        expected_tiles: Expected number of UTM tiles
        expected_dates: Expected number of dates per parcel
    
    Yields:
        Dict with validation results and statistics
    """
    
    typer.echo("=== DLT STAGE: DATA VALIDATION ===")
    
    try:
        # Capture validation results by temporarily modifying the validate function
        # to return results instead of just printing
        from lake_sandbox.validator.raw import validate_raw_timeseries
        from lake_sandbox.validator.organized import validate_organized_chunks
        from lake_sandbox.validator.delta import validate_delta_tables
        
        validation_results = {}
        
        # Run validation based on target
        if target == "raw":
            raw_result = validate_raw_timeseries(
                raw_dir=raw_dir,
                expected_total_parcels=expected_total_parcels,
                verbose=False
            )
            validation_results["raw"] = {
                "valid": raw_result.valid,
                "total_files": raw_result.total_files,
                "total_unique_parcels": raw_result.total_unique_parcels,
                "total_records": raw_result.total_records,
                "issues": raw_result.issues,
                "error": getattr(raw_result, 'error', None)
            }
            
        elif target in ["organized", "both"]:
            organized_result = validate_organized_chunks(
                organized_dir=organized_dir,
                expected_chunk_size=expected_chunk_size,
                expected_tiles=expected_tiles,
                expected_dates=expected_dates,
                raw_dir=raw_dir,
                verbose=False
            )
            validation_results["organized"] = {
                "valid": organized_result.valid,
                "total_chunks": organized_result.total_chunks,
                "total_unique_parcels": organized_result.total_unique_parcels,
                "total_records": organized_result.total_records,
                "issues": organized_result.issues,
                "error": getattr(organized_result, 'error', None)
            }
        
        if target in ["delta", "both"]:
            delta_result = validate_delta_tables(
                delta_dir=delta_dir,
                verbose=False
            )
            validation_results["delta"] = {
                "valid": delta_result.valid,
                "total_tables": delta_result.total_tables,
                "total_unique_parcels": delta_result.total_unique_parcels,
                "total_records": delta_result.total_records,
                "issues": delta_result.issues,
                "error": getattr(delta_result, 'error', None)
            }
        
        # Determine overall validation status
        all_valid = all(result["valid"] for result in validation_results.values())
        total_issues = sum(len(result["issues"]) for result in validation_results.values())
        
        yield {
            "stage": "validation",
            "status": "completed",
            "target": target,
            "overall_valid": all_valid,
            "total_issues": total_issues,
            "results": validation_results,
            "directories": {
                "raw_dir": raw_dir,
                "organized_dir": organized_dir,
                "delta_dir": delta_dir
            },
            "expectations": {
                "total_parcels": expected_total_parcels,
                "chunk_size": expected_chunk_size,
                "tiles": expected_tiles,
                "dates": expected_dates
            }
        }
        
    except Exception as e:
        yield {
            "stage": "validation",
            "status": "failed",
            "error": str(e),
            "target": target,
            "directories": {
                "raw_dir": raw_dir,
                "organized_dir": organized_dir, 
                "delta_dir": delta_dir
            }
        }


def create_validation_pipeline(
    pipeline_name: str = "validation_pipeline",
    destination: str = "duckdb"
) -> dlt.Pipeline:
    """Create a DLT pipeline for data validation.
    
    Args:
        pipeline_name: Name of the pipeline
        destination: DLT destination (parquet, filesystem, etc.)
    
    Returns:
        Configured DLT pipeline
    """
    
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name="lake_sandbox_validation"
    )