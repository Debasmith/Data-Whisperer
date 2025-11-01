import os
import sys
from pathlib import Path
import socket
from contextlib import closing
import panel as pn
from src.config import Config
from src.ui.dashboard import DataWhispererDashboard
from src.utils.logger import setup_logger

# Initialize Panel
pn.extension('plotly', 'tabulator', notifications=True, sizing_mode='stretch_width')

logger = setup_logger(__name__)


def _is_port_available(host: str, port: int) -> bool:
    """Return True if the given host:port can be bound."""

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def _get_available_port(host: str, start_port: int, attempts: int = 10) -> int:
    """Find an available port starting from start_port within the attempt limit."""

    port = start_port
    for _ in range(attempts):
        if _is_port_available(host, port):
            return port
        port += 1
    raise RuntimeError(
        f"No available port found near {start_port}. Check running processes or adjust configuration."
    )


def main():
    """Main application entry point"""
    try:
        # Load configuration
        config = Config()

        available_port = _get_available_port(config.host, config.port)
        if available_port != config.port:
            logger.warning(
                "Port %s is in use. Switching to available port %s.",
                config.port,
                available_port,
            )
            config.port = available_port

        logger.info("=" * 80)
        logger.info("Starting DataWhisperer")
        logger.info("=" * 80)
        
        # Create dashboard
        dashboard = DataWhispererDashboard(config)
        
        # Create the application
        app = dashboard.create_app()
        
        # Display startup information
        logger.info(f"\nDashboard Configuration:")
        logger.info(f"   • LLM Model: {config.llm_model}")
        logger.info(f"   • Port: {config.port}")
        logger.info(f"   • Max Retries: {config.max_retries}")
        logger.info(f"   • Supported Formats: {', '.join(config.supported_file_types)}")
        
        logger.info(f"\nStarting web server...")
        logger.info(f"   • URL: http://{config.host}:{config.port}")
        logger.info(f"   • Press Ctrl+C to stop the server\n")
        
        # Serve the application
        app.show(
            port=config.port,
            address=config.host,
            open=config.auto_open_browser,
            threaded=False,
            verbose=True,
            title="DataWhisperer Analytics"
        )
        
    except KeyboardInterrupt:
        logger.info("\n\nShutting down DataWhisperer")
        sys.exit(0)
    except Exception as e:
        logger.error(f"\nFatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()