import functools
import gc
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import psutil
import typer


@dataclass
class PerformanceMetrics:
    """Performance metrics for monitoring functions."""

    execution_time: float
    peak_memory_mb: float
    initial_memory_mb: float
    memory_delta_mb: float
    cpu_percent: float
    function_name: str


def _print_performance_summary(metrics: PerformanceMetrics) -> None:
    """Print a formatted performance summary."""
    typer.echo("\n" + "=" * 60)
    typer.echo(f"PERFORMANCE SUMMARY: {metrics.function_name}")
    typer.echo("=" * 60)
    typer.echo(f"Execution Time: {metrics.execution_time:.2f} seconds")
    typer.echo("    Memory Usage:")
    typer.echo(f"   • Initial: {metrics.initial_memory_mb:.1f} MB")
    typer.echo(f"   • Peak: {metrics.peak_memory_mb:.1f} MB")
    typer.echo(f"   • Delta: {metrics.memory_delta_mb:+.1f} MB")
    typer.echo(f"CPU Usage: {metrics.cpu_percent:.1f}%")
    typer.echo("=" * 60 + "\n")


class PerformanceMonitor:
    """Monitor CPU and memory usage during function execution."""

    def __init__(self):
        self.process = psutil.Process()

    def get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        return self.process.memory_info().rss / 1024 / 1024

    def get_cpu_percent(self) -> float:
        """Get current CPU usage percentage."""
        return self.process.cpu_percent()

    @contextmanager
    def monitor(self, function_name: str = "unknown"):
        """Context manager to monitor performance during execution."""
        # Force garbage collection before monitoring
        gc.collect()

        # Initial measurements
        initial_memory = self.get_memory_usage_mb()
        start_time = time.perf_counter()

        # Start CPU monitoring
        self.process.cpu_percent()  # Initialize CPU monitoring

        peak_memory = initial_memory

        try:
            yield self

        finally:
            # Final measurements
            end_time = time.perf_counter()
            final_memory = self.get_memory_usage_mb()
            cpu_percent = self.get_cpu_percent()

            # Calculate peak memory during execution
            current_memory = self.get_memory_usage_mb()
            peak_memory = max(peak_memory, current_memory, final_memory)

            # Create metrics
            metrics = PerformanceMetrics(
                execution_time=end_time - start_time,
                peak_memory_mb=peak_memory,
                initial_memory_mb=initial_memory,
                memory_delta_mb=final_memory - initial_memory,
                cpu_percent=cpu_percent,
                function_name=function_name,
            )

            # Print performance summary
            _print_performance_summary(metrics)


def monitor_performance(function_name: str | None = None):
    """Decorator to monitor performance of any function.

    Usage:
        @monitor_performance()
        def my_generator_function():
            # Your code here
            pass

        @monitor_performance("Custom Name")
        def my_function():
            # Your code here
            pass
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            monitor = PerformanceMonitor()
            name = function_name or func.__name__

            with monitor.monitor(name):
                result = func(*args, **kwargs)

            return result

        return wrapper

    return decorator


@contextmanager
def performance_context(name: str = "operation"):
    """Context manager for monitoring performance of code blocks.

    Usage:
        with performance_context("data generation"):
            # Your code here
            generate_data()
    """
    monitor = PerformanceMonitor()
    with monitor.monitor(name):
        yield monitor


def get_system_info() -> dict[str, Any]:
    """Get system information for performance context."""
    return {
        "cpu_count": psutil.cpu_count(),
        "cpu_count_logical": psutil.cpu_count(logical=True),
        "memory_total_gb": psutil.virtual_memory().total / 1024 ** 3,
        "memory_available_gb": psutil.virtual_memory().available / 1024 ** 3,
        "disk_usage_gb": psutil.disk_usage("/").total / 1024 ** 3,
    }


def print_system_info() -> None:
    """Print system information."""
    info = get_system_info()
    typer.echo("\n" + "=" * 60)
    typer.echo("SYSTEM INFORMATION")
    typer.echo("=" * 60)
    typer.echo(
        f"CPU Cores: {info['cpu_count']} physical, {info['cpu_count_logical']} logical")
    typer.echo(
        f"Memory: {info['memory_total_gb']:.1f} GB total, {info['memory_available_gb']:.1f} GB available")
    typer.echo(f"Disk: {info['disk_usage_gb']:.1f} GB total")
    typer.echo("=" * 60 + "\n")
