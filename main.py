import os
import sys
from pathlib import Path
import panel as pn
from src.config import Config
from src.ui.dashboard import DataWhispererDashboard
from src.utils.logger import setup_logger

# Initialize Panel
pn.extension('plotly', 'tabulator', notifications=True, sizing_mode='stretch_width')

logger = setup_logger(__name__)


def main():
    """Main application entry point"""
    try:
        # Load configuration
        config = Config()
        
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