"""Logging utility for DataWhisperer with UTF-8 safety."""

import io
import logging
import sys
from datetime import datetime
from pathlib import Path


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Setup logger with consistent formatting"""
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Ensure sys.stdout/sys.stderr can emit Unicode emojis on Windows
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream and hasattr(stream, "reconfigure"):
            try:  # pragma: no cover - platform dependent
                stream.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

    stdout_buffer = getattr(sys.stdout, "buffer", None)
    if stdout_buffer is not None:
        text_stream = io.TextIOWrapper(stdout_buffer, encoding="utf-8", errors="replace")
    else:
        text_stream = sys.stdout

    # Console handler with colors
    console_handler = logging.StreamHandler(text_stream)
    console_handler.setLevel(level)
    
    # Format with colors for different levels
    class ColoredFormatter(logging.Formatter):
        """Custom formatter with colors"""
        
        COLORS = {
            'DEBUG': '\033[36m',    # Cyan
            'INFO': '\033[32m',     # Green
            'WARNING': '\033[33m',  # Yellow
            'ERROR': '\033[31m',    # Red
            'CRITICAL': '\033[35m', # Magenta
        }
        RESET = '\033[0m'
        
        def format(self, record):
            log_color = self.COLORS.get(record.levelname, self.RESET)
            record.levelname = f"{log_color}{record.levelname}{self.RESET}"
            return super().format(record)
    
    formatter = ColoredFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Optional: File handler
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    file_handler = logging.FileHandler(
        log_dir / f"datawhisperer_{datetime.now().strftime('%Y%m%d')}.log",
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    return logger