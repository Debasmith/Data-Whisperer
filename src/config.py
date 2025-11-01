"""
Configuration management for DataWhisperer
Save this as: src/config.py
"""

import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List
from dataclasses import dataclass, field


logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Application configuration"""
    
    # LLM Configuration
    llm_model: str = "gemma3"
    llm_base_url: str = "http://localhost:11434/v1"
    llm_api_key: str = "ollama"
    llm_temperature: float = 0.1
    
    # Server Configuration
    host: str = "localhost"
    port: int = 5007
    auto_open_browser: bool = True
    
    # Query Processing
    max_retries: int = 5
    query_timeout: int = 120  # seconds
    
    # File Upload
    max_file_size_mb: int = 100
    supported_file_types: List[str] = field(default_factory=lambda: [
        '.csv', '.xlsx', '.xls', '.json', '.parquet'
    ])
    
    # Visualization
    default_color_scheme: str = "plotly"  # plotly, d3, tableau
    chart_height: int = 400
    chart_width: int = 600
    
    # Data Processing
    sample_rows_for_llm: int = 5
    max_rows_display: int = 1000
    
    # Spark Configuration
    spark_app_name: str = "DataWhisperer"
    spark_master: str = "local[*]"
    python_executable: str = field(default_factory=lambda: sys.executable)
    spark_driver_host: str = "127.0.0.1"

    def __post_init__(self):
        """Validate configuration after initialization"""
        # Override from environment variables if available
        self.llm_model = os.getenv("DATAWHISPERER_LLM_MODEL", self.llm_model)
        self.llm_base_url = os.getenv("DATAWHISPERER_LLM_URL", self.llm_base_url)
        self.port = int(os.getenv("DATAWHISPERER_PORT", self.port))
        python_env = os.getenv("DATAWHISPERER_PYTHON_EXECUTABLE", "").strip()
        if python_env:
            self.python_executable = python_env

        if not self.python_executable:
            self.python_executable = sys.executable

        python_path = Path(self.python_executable).expanduser()
        if python_path.is_file():
            self.python_executable = str(python_path)
        else:
            # Fallback to current interpreter if provided path is invalid
            self.python_executable = sys.executable
            python_path = Path(self.python_executable)

        if not self._can_import_pyspark(self.python_executable):
            fallback_executable = sys.executable
            if (
                fallback_executable != self.python_executable
                and self._can_import_pyspark(fallback_executable)
            ):
                logger.warning(
                    "Configured python executable '%s' cannot import pyspark. "
                    "Falling back to current interpreter '%s'.",
                    self.python_executable,
                    fallback_executable,
                )
                self.python_executable = fallback_executable
            else:
                raise RuntimeError(
                    "Configured python executable cannot import pyspark. "
                    "Install pyspark in that environment or unset "
                    "DATAWHISPERER_PYTHON_EXECUTABLE."
                )

        self.spark_driver_host = (
            os.getenv("DATAWHISPERER_SPARK_DRIVER_HOST", self.spark_driver_host).strip()
            or "127.0.0.1"
        )

        os.environ["PYSPARK_PYTHON"] = self.python_executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = self.python_executable
        os.environ.setdefault("SPARK_LOCAL_IP", self.spark_driver_host)
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", self.spark_driver_host)

        # Validate file size
        if self.max_file_size_mb <= 0:
            raise ValueError("max_file_size_mb must be positive")

    @staticmethod
    def _can_import_pyspark(python_executable: str) -> bool:
        """Return True if the interpreter can import pyspark."""

        try:
            subprocess.run(
                [python_executable, "-c", "import pyspark"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            return True
        except (FileNotFoundError, subprocess.CalledProcessError):
            return False
    
    @property
    def max_file_size_bytes(self) -> int:
        """Convert max file size to bytes"""
        return self.max_file_size_mb * 1024 * 1024
    
    def get_color_palette(self, n_colors: int = 10) -> List[str]:
        """Get color palette based on scheme"""
        palettes = {
            "plotly": [
                '#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A',
                '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'
            ],
            "d3": [
                '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
            ],
            "tableau": [
                '#4E79A7', '#F28E2B', '#E15759', '#76B7B2', '#59A14F',
                '#EDC948', '#B07AA1', '#FF9DA7', '#9C755F', '#BAB0AC'
            ]
        }
        
        palette = palettes.get(self.default_color_scheme, palettes["plotly"])
        # Repeat palette if needed
        while len(palette) < n_colors:
            palette.extend(palette)
        
        return palette[:n_colors]