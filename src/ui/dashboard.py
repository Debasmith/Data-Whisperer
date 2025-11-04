import threading
from datetime import datetime
from pathlib import Path

import panel as pn
import param

from src.query.query_processor import QueryProcessor
from src.visualization.viz_engine import VisualizationEngine
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

APPLE_DARK_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;900&display=swap');
    
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    
    body {
        background: #000000;
        color: #f5f5f7;
    }
    
    /* Fix textarea text color - CRITICAL FIX */
    textarea, input[type="text"], .bk-input {
        color: #f5f5f7 !important;
        background: rgba(30, 30, 30, 0.7) !important;
    }
    
    textarea::placeholder, input::placeholder {
        color: rgba(245, 245, 247, 0.4) !important;
    }
    
    /* Glassmorphism effect */
    .glass {
        background: rgba(30, 30, 30, 0.7);
        backdrop-filter: blur(20px) saturate(180%);
        -webkit-backdrop-filter: blur(20px) saturate(180%);
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    /* Smooth animations */
    .smooth-transition {
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    .hover-lift:hover {
        transform: translateY(-4px);
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
    }
    
    /* Hide default panel styling */
    .bk-root {
        background: transparent !important;
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: rgba(255, 255, 255, 0.05);
    }
    
    ::-webkit-scrollbar-thumb {
        background: rgba(255, 255, 255, 0.2);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(255, 255, 255, 0.3);
    }
</style>
"""


class DataWhispererDashboard(param.Parameterized):
    """Apple-inspired dark theme dashboard with auto-layout"""
    
    data_loaded = param.Boolean(default=False)
    processing = param.Boolean(default=False)
    
    def __init__(self, config, **params):
        super().__init__(**params)
        self.config = config
        
        self.query_processor = QueryProcessor(config)
        self.viz_engine = VisualizationEngine(config)
        
        self.file_input = None
        self.query_input = None
        self.submit_button = None
        self.clear_button = None
        self.status_indicator = None
        self.schema_display = None
        self.results_container = None  # Changed from results_grid
        self.stats_row = None
        
        self.query_history = []
        self.result_cards = []
        
        logger.info("dashboard initialized")
    
    def _create_header(self):
        """Create minimalist Apple-style header"""
        
        header_html = """
        <div style="
            background: linear-gradient(135deg, rgba(0, 122, 255, 0.1) 0%, rgba(88, 86, 214, 0.1) 100%);
            padding: 40px 50px;
            border-radius: 20px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(20px);
            margin-bottom: 30px;
            position: relative;
            overflow: hidden;
        ">
            <!-- Animated gradient background -->
            <div style="
                position: absolute;
                top: -50%;
                left: -50%;
                width: 200%;
                height: 200%;
                background: radial-gradient(circle, rgba(0, 122, 255, 0.15) 0%, transparent 70%);
                animation: pulse 8s ease-in-out infinite;
            "></div>
            
            <div style="position: relative; z-index: 1;">
                <div style="display: flex; align-items: center; gap: 20px;">
                    <div style="
                        width: 60px;
                        height: 60px;
                        background: linear-gradient(135deg, #007AFF 0%, #5856D6 100%);
                        border-radius: 16px;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        box-shadow: 0 10px 30px rgba(0, 122, 255, 0.3);
                    ">
                        <svg width="32" height="32" viewBox="0 0 24 24" fill="white">
                            <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>
                        </svg>
                    </div>
                    <div>
                        <h1 style="
                            color: #f5f5f7;
                            margin: 0;
                            font-size: 42px;
                            font-weight: 700;
                            letter-spacing: -1px;
                        ">DataWhisperer</h1>
                        <p style="
                            color: rgba(245, 245, 247, 0.7);
                            margin: 5px 0 0 0;
                            font-size: 17px;
                            font-weight: 400;
                            letter-spacing: -0.2px;
                        ">Ask. Analyze. Visualize. Instantly.</p>
                    </div>
                </div>
            </div>
        </div>
        
        <style>
            @keyframes pulse {
                0%, 100% { transform: translate(0, 0) scale(1); }
                50% { transform: translate(10%, 10%) scale(1.1); }
            }
        </style>
        """
        
        return pn.pane.HTML(header_html, sizing_mode='stretch_width')
    
    def _create_stats_row(self):
        """Create minimal stats cards"""
        
        self.stats_row = pn.Row(
            visible=False,
            sizing_mode='stretch_width',
            margin=(0, 0, 25, 0)
        )
        
        return self.stats_row
    
    def _update_stats(self):
        """Update statistics with glassmorphism cards"""
        
        if not self.query_processor.table_name:
            self.stats_row.visible = False
            return
        
        stats_html = f"""
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 25px;">
            <!-- Dataset Card -->
            <div class="glass smooth-transition hover-lift" style="
                padding: 24px;
                border-radius: 16px;
                text-align: center;
            ">
                <div style="
                    font-size: 13px;
                    color: rgba(245, 245, 247, 0.6);
                    font-weight: 500;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    margin-bottom: 10px;
                ">Dataset</div>
                <div style="
                    font-size: 26px;
                    font-weight: 700;
                    color: #007AFF;
                    letter-spacing: -0.5px;
                ">{self.query_processor.table_name}</div>
            </div>
            
            <!-- Rows Card -->
            <div class="glass smooth-transition hover-lift" style="
                padding: 24px;
                border-radius: 16px;
                text-align: center;
            ">
                <div style="
                    font-size: 13px;
                    color: rgba(245, 245, 247, 0.6);
                    font-weight: 500;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    margin-bottom: 10px;
                ">Rows</div>
                <div style="
                    font-size: 26px;
                    font-weight: 700;
                    color: #30D158;
                    letter-spacing: -0.5px;
                ">{self.query_processor.row_count:,}</div>
            </div>
            
            <!-- Columns Card -->
            <div class="glass smooth-transition hover-lift" style="
                padding: 24px;
                border-radius: 16px;
                text-align: center;
            ">
                <div style="
                    font-size: 13px;
                    color: rgba(245, 245, 247, 0.6);
                    font-weight: 500;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    margin-bottom: 10px;
                ">Columns</div>
                <div style="
                    font-size: 26px;
                    font-weight: 700;
                    color: #FF9F0A;
                    letter-spacing: -0.5px;
                ">{len(self.query_processor.schema_details)}</div>
            </div>
            
            <!-- Queries Card -->
            <div class="glass smooth-transition hover-lift" style="
                padding: 24px;
                border-radius: 16px;
                text-align: center;
            ">
                <div style="
                    font-size: 13px;
                    color: rgba(245, 245, 247, 0.6);
                    font-weight: 500;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    margin-bottom: 10px;
                ">Insights</div>
                <div style="
                    font-size: 26px;
                    font-weight: 700;
                    color: #BF5AF2;
                    letter-spacing: -0.5px;
                ">{len(self.query_history)}</div>
            </div>
        </div>
        """
        
        self.stats_row.clear()
        self.stats_row.append(pn.pane.HTML(stats_html, sizing_mode='stretch_width'))
        self.stats_row.visible = True
    
    def _create_upload_section(self):
        """Create minimal upload interface"""
        
        self.file_input = pn.widgets.FileInput(
            accept=','.join(self.config.supported_file_types),
            sizing_mode='stretch_width',
            height=50,
            styles={
                'background': 'rgba(30, 30, 30, 0.7)',
                'border': '1px solid rgba(255, 255, 255, 0.1)',
                'border-radius': '12px'
            }
        )
        self.file_input.param.watch(self._on_file_upload, 'value')
        
        upload_html = """
        <div class="glass smooth-transition" style="
            border-radius: 20px;
            padding: 50px;
            text-align: center;
            cursor: pointer;
            border: 2px dashed rgba(255, 255, 255, 0.2);
        " onmouseover="this.style.borderColor='rgba(0, 122, 255, 0.5)'; this.style.background='rgba(0, 122, 255, 0.05)';" 
           onmouseout="this.style.borderColor='rgba(255, 255, 255, 0.2)'; this.style.background='rgba(30, 30, 30, 0.7)';">
            <div style="
                width: 64px;
                height: 64px;
                margin: 0 auto 20px;
                background: linear-gradient(135deg, #007AFF 0%, #5856D6 100%);
                border-radius: 16px;
                display: flex;
                align-items: center;
                justify-content: center;
            ">
                <svg width="32" height="32" viewBox="0 0 24 24" fill="white">
                    <path d="M9 16v-6h-4l5-5 5 5h-4v6h-2zm-4 2h14v2h-14z"/>
                </svg>
            </div>
            <h3 style="color: #f5f5f7; margin: 0 0 10px 0; font-weight: 600; font-size: 20px;">
                Drop your data here
            </h3>
            <p style="color: rgba(245, 245, 247, 0.6); margin: 0 0 20px 0; font-size: 15px;">
                or click to browse
            </p>
            <div style="
                display: inline-flex;
                gap: 8px;
                flex-wrap: wrap;
                justify-content: center;
            ">
                <span style="
                    background: rgba(255, 255, 255, 0.1);
                    padding: 6px 12px;
                    border-radius: 8px;
                    font-size: 13px;
                    color: rgba(245, 245, 247, 0.7);
                ">CSV</span>
                <span style="
                    background: rgba(255, 255, 255, 0.1);
                    padding: 6px 12px;
                    border-radius: 8px;
                    font-size: 13px;
                    color: rgba(245, 245, 247, 0.7);
                ">Excel</span>
                <span style="
                    background: rgba(255, 255, 255, 0.1);
                    padding: 6px 12px;
                    border-radius: 8px;
                    font-size: 13px;
                    color: rgba(245, 245, 247, 0.7);
                ">JSON</span>
                <span style="
                    background: rgba(255, 255, 255, 0.1);
                    padding: 6px 12px;
                    border-radius: 8px;
                    font-size: 13px;
                    color: rgba(245, 245, 247, 0.7);
                ">Parquet</span>
            </div>
        </div>
        """
        
        return pn.Column(
            pn.pane.HTML(upload_html, sizing_mode='stretch_width'),
            self.file_input,
            sizing_mode='stretch_width',
            margin=(0, 0, 25, 0)
        )
    
    def _create_query_section(self):
        """Create minimal query interface with FIXED text visibility"""
        
        # FIXED: Properly styled text area with visible text
        self.query_input = pn.widgets.TextAreaInput(
            placeholder='Ask anything about your data... (e.g., "Show me top 10 products by sales")',
            height=120,  # Increased height for better visibility
            disabled=True,
            sizing_mode='stretch_width',
            styles={
                'font-size': '16px',
                'background': 'rgba(30, 30, 30, 0.9)',  # Darker background
                'border': '2px solid rgba(255, 255, 255, 0.15)',
                'border-radius': '16px',
                'padding': '16px',
                'color': '#f5f5f7',  # Light text color
                'backdrop-filter': 'blur(20px)',
                'resize': 'vertical',  # Allow vertical resize
                'min-height': '120px',
                'max-height': '400px'
            }
        )
        
        self.submit_button = pn.widgets.Button(
            name='✨ Analyze',
            button_type='primary',
            width=140,
            height=48,
            disabled=True,
            styles={
                'background': 'linear-gradient(135deg, #007AFF 0%, #5856D6 100%)',
                'border': 'none',
                'font-size': '16px',
                'font-weight': '600',
                'border-radius': '12px',
                'box-shadow': '0 4px 20px rgba(0, 122, 255, 0.4)',
                'color': 'white'
            }
        )
        self.submit_button.on_click(self._on_submit_query)
        
        self.clear_button = pn.widgets.Button(
            name='Clear All',
            button_type='light',
            width=120,
            height=48,
            disabled=True,
            styles={
                'background': 'rgba(255, 255, 255, 0.1)',
                'border': '1px solid rgba(255, 255, 255, 0.2)',
                'border-radius': '12px',
                'font-size': '15px',
                'font-weight': '500',
                'color': '#f5f5f7'
            }
        )
        self.clear_button.on_click(self._on_clear_results)
        
        self.status_indicator = pn.indicators.LoadingSpinner(
            value=False,
            size=32,
            color='primary',
            visible=False
        )
        
        button_row = pn.Row(
            pn.layout.HSpacer(),
            self.submit_button,
            self.clear_button,
            self.status_indicator,
            pn.layout.HSpacer(),
            align='center',
            margin=(15, 0, 0, 0)
        )
        
        query_card = """
        <div class="glass" style="
            padding: 30px;
            border-radius: 20px;
            margin-bottom: 25px;
        ">
            <h3 style="
                color: #f5f5f7;
                margin: 0 0 20px 0;
                font-size: 20px;
                font-weight: 600;
                letter-spacing: -0.3px;
            ">Ask Your Question</h3>
        </div>
        """
        
        return pn.Column(
            pn.pane.HTML(query_card.replace('</div>', ''), sizing_mode='stretch_width'),
            self.query_input,
            button_row,
            pn.pane.HTML('</div>'),
            sizing_mode='stretch_width'
        )
    
    def _create_results_section(self):
        """Create dashboard-style results container (FIXED)"""
        
        empty_state_html = """
        <div style="
            text-align: center;
            padding: 80px 20px;
            background: rgba(30, 30, 30, 0.4);
            border-radius: 20px;
            border: 1px solid rgba(255, 255, 255, 0.05);
        ">
            <div style="
                width: 80px;
                height: 80px;
                margin: 0 auto 25px;
                background: linear-gradient(135deg, rgba(0, 122, 255, 0.2) 0%, rgba(88, 86, 214, 0.2) 100%);
                border-radius: 20px;
                display: flex;
                align-items: center;
                justify-content: center;
            ">
                <svg width="40" height="40" viewBox="0 0 24 24" fill="rgba(245, 245, 247, 0.4)">
                    <path d="M3 3h7v7h-7v-7zm11 0h7v7h-7v-7zm-11 11h7v7h-7v-7zm11 0h7v7h-7v-7z"/>
                </svg>
            </div>
            <h3 style="color: rgba(245, 245, 247, 0.8); margin: 0 0 10px 0; font-weight: 600; font-size: 22px;">
                Ready for Insights
            </h3>
            <p style="color: rgba(245, 245, 247, 0.5); margin: 0; font-size: 16px; max-width: 500px; margin: 0 auto;">
                Upload your data and start asking questions.<br/>Each insight will appear here automatically.
            </p>
        </div>
        """
        
        # FIXED: Use Column instead of GridSpec
        self.results_container = pn.Column(
            pn.pane.HTML(empty_state_html, sizing_mode='stretch_width'),
            sizing_mode='stretch_width',
            scroll=True,
            styles={
                'background': 'rgba(0, 0, 0, 0.3)',
                'border-radius': '20px',
                'padding': '20px',
                'min-height': '600px',
                'max-height': '1200px'
            }
        )
        
        return pn.Column(
            pn.pane.HTML("""
                <h3 style="
                    color: #f5f5f7;
                    margin: 0 0 20px 0;
                    font-size: 24px;
                    font-weight: 600;
                    letter-spacing: -0.5px;
                ">Your Insights Dashboard</h3>
            """),
            self.results_container,
            sizing_mode='stretch_width'
        )
    
    def _on_file_upload(self, event):
        """Handle file upload"""
        
        if event.new is None:
            return
        
        try:
            self.status_indicator.visible = True
            self.status_indicator.value = True
            
            self.file_input.disabled = True
            self.query_input.disabled = True
            self.submit_button.disabled = True
            
            file_name = self.file_input.filename
            file_size_mb = len(event.new) / (1024 * 1024)
            
            if file_size_mb > self.config.max_file_size_mb:
                pn.state.notifications.error(
                    f"File too large ({file_size_mb:.1f}MB). Max: {self.config.max_file_size_mb}MB",
                    duration=5000
                )
                self.file_input.value = None
                return
            
            file_extension = Path(file_name).suffix.lower()
            if file_extension not in self.config.supported_file_types:
                pn.state.notifications.error(
                    f"Unsupported file type: {file_extension}",
                    duration=5000
                )
                self.file_input.value = None
                return
            
            pn.state.notifications.info(
                f"📂 Processing {file_name}...",
                duration=3000
            )
            
            def process_file():
                try:
                    success, message = self.query_processor.load_dataset(event.new, file_name)
                    
                    if not success:
                        raise ValueError(message)
                    
                    pn.state.execute(lambda: self._update_ui_after_upload(file_name))
                    
                    pn.state.execute(
                        lambda: pn.state.notifications.success(
                            f"✅ {file_name} loaded successfully!",
                            duration=4000
                        )
                    )
                    
                except Exception as e:
                    error_msg = f"Error: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    pn.state.execute(
                        lambda: pn.state.notifications.error(error_msg, duration=6000)
                    )
                    pn.state.execute(lambda: setattr(self.file_input, 'value', None))
                    
                finally:
                    pn.state.execute(lambda: setattr(self.status_indicator, 'visible', False))
                    pn.state.execute(lambda: setattr(self.status_indicator, 'value', False))
                    pn.state.execute(lambda: setattr(self.file_input, 'disabled', False))
                    pn.state.execute(lambda: setattr(self.query_input, 'disabled', False))
            
            thread = threading.Thread(target=process_file, daemon=True)
            thread.start()
            
        except Exception as e:
            logger.error("Upload error: %s", str(e), exc_info=True)
            pn.state.notifications.error(f"Upload failed: {str(e)}", duration=6000)
            
            self.status_indicator.visible = False
            self.status_indicator.value = False
            self.file_input.disabled = False
            self.query_input.disabled = False
            self.file_input.value = None
    
    def _update_ui_after_upload(self, file_name: str):
        """Update UI after successful upload"""
        
        self.data_loaded = True
        self.query_input.disabled = False
        self.submit_button.disabled = False
        self._update_stats()
        
        logger.info("✅ UI updated for dataset: %s", file_name)
    
    def _on_submit_query(self, event):
        """Handle query submission with support for multiple visualizations"""
        
        if self.processing or not self.query_input.value:
            return
        
        self.processing = True
        self.submit_button.disabled = True
        self.status_indicator.visible = True
        self.status_indicator.value = True
        
        query = self.query_input.value.strip()
        
        logger.info("🔍 Processing query: %s", query)
        pn.state.notifications.info(f"🤖 Analyzing...", duration=2000)
        
        def run_query():
            try:
                # Process the query - this now returns a list of results
                results = self.query_processor.process_query(query)
                
                if not isinstance(results, list):
                    results = [results]  # Convert single result to list for consistent processing
                
                visualizations = []
                sql_queries = set()
                
                # Track if any queries failed
                failed_queries = 0
                
                for result in results:
                    if not result.get('success'):
                        error_msg = result.get('error', 'Unknown error')
                        logger.warning("Query part failed: %s", error_msg)
                        
                        # Add error visualization
                        visualizations.append({
                            'success': False,
                            'error': error_msg,
                            'query': result.get('query', 'Unknown query')
                        })
                        failed_queries += 1
                        continue
                    
                    viz_config = result.get('viz_config', {})
                    data = result.get('data')
                    sql_query = result.get('sql_query', '')
                    
                    if not data or (hasattr(data, '__len__') and len(data) == 0):
                        logger.warning("No data returned for a visualization")
                        visualizations.append({
                            'success': False,
                            'error': 'No data returned for this query',
                            'query': result.get('query', 'Unknown query')
                        })
                        failed_queries += 1
                        continue
                    
                    # Create visualization
                    try:
                        viz = self.viz_engine.create_visualization(data, viz_config, query)
                        visualizations.append({
                            'success': True,
                            'viz': viz,
                            'config': viz_config,
                            'data': data,
                            'sql_query': sql_query,
                            'query': result.get('query', query)
                        })
                        if sql_query:
                            sql_queries.add(sql_query)
                    except Exception as viz_error:
                        logger.warning("Viz creation failed, using table: %s", viz_error)
                        try:
                            viz = pn.widgets.Tabulator(
                                data,
                                pagination='local',
                                page_size=20,
                                sizing_mode='stretch_width',
                                theme='midnight'
                            )
                            visualizations.append({
                                'success': True,
                                'viz': viz,
                                'config': {'visualization_type': 'table', 'title': 'Data Table'},
                                'data': data,
                                'sql_query': sql_query,
                                'query': result.get('query', query)
                            })
                            if sql_query:
                                sql_queries.add(sql_query)
                        except Exception as tabulator_error:
                            logger.error("Failed to create table: %s", tabulator_error)
                            visualizations.append({
                                'success': False,
                                'error': f'Failed to create visualization: {str(tabulator_error)}',
                                'query': result.get('query', 'Unknown query')
                            })
                            failed_queries += 1
                
                if len(visualizations) == 0:
                    raise ValueError("No valid visualizations could be created from the query")
                
                def apply_success():
                    # Add all visualizations to dashboard
                    self._add_to_dashboard(query, visualizations)
                    
                    # Add to query history if we have at least one successful visualization
                    if any(viz.get('success', False) for viz in visualizations):
                        self.query_history.append({
                            'query': query,
                            'sql': '\n\n---\n\n'.join(sql_queries) if sql_queries else '',
                            'timestamp': datetime.now().isoformat(),
                            'viz_count': len([v for v in visualizations if v.get('success', False)])
                        })
                    
                    self.clear_button.disabled = False
                    self.query_input.value = ""
                    self._update_stats()
                    
                    # Show appropriate success/warning message
                    success_count = len([v for v in visualizations if v.get('success', False)])
                    if success_count > 0:
                        if failed_queries > 0:
                            pn.state.notifications.warning(
                                f"⚠️ Added {success_count} visualization{'s' if success_count > 1 else ''} to dashboard "
                                f"({failed_queries} query{'s' if failed_queries > 1 else ''} failed)",
                                duration=4000
                            )
                        else:
                            pn.state.notifications.success(
                                f"✅ Added {success_count} visualization{'s' if success_count > 1 else ''} to dashboard!",
                                duration=3000
                            )
                    else:
                        pn.state.notifications.error(
                            "❌ Failed to create any visualizations. Please check your queries and try again.",
                            duration=5000
                        )
                
                pn.state.execute(apply_success)
                
            except Exception as exc:
                error_text = str(exc)
                logger.error("Query error: %s", error_text, exc_info=True)
                
                def apply_error():
                    pn.state.notifications.error(
                        f"❌ {error_text[:150]}" + ("..." if len(error_text) > 150 else ""),
                        duration=8000
                    )
                
                pn.state.execute(apply_error)
                
            finally:
                def reset_ui():
                    self.processing = False
                    self.submit_button.disabled = False
                    self.status_indicator.visible = False
                    self.status_indicator.value = False
                
                pn.state.execute(reset_ui)
        
        thread = threading.Thread(target=run_query, daemon=True)
        thread.start()
    
    def _add_to_dashboard(self, query, visualizations):
        """Add multiple visualization cards to the dashboard container in a responsive grid"""
        
        # Clear previous results if this is a new query
        if not hasattr(self, 'result_cards'):
            self.result_cards = []
        
        # Create cards for each visualization
        for viz_data in visualizations:
            if not viz_data.get('success'):
                # Handle failed queries
                error_card = pn.Card(
                    pn.pane.Alert(
                        f"❌ {viz_data.get('error', 'Failed to process query')}",
                        alert_type="danger",
                        margin=(10, 10, 10, 10)
                    ),
                    title="Error",
                    header_background='#2d1a2c',
                    styles={
                        'background': 'rgba(45, 26, 44, 0.7)',
                        'border': '1px solid #ff4d4d',
                        'border-radius': '8px',
                        'margin': '10px',
                        'flex': '1 1 45%',
                        'min-width': '400px',
                        'max-width': '100%'
                    }
                )
                self.result_cards.append(error_card)
                continue
                
            viz = viz_data.get('viz')
            viz_config = viz_data.get('config', {})
            sql = viz_data.get('sql_query', '')
            
            # Create a card for each visualization
            card = self._create_result_card(
                viz_data.get('query', query),  # Use individual query if available
                sql, 
                viz, 
                viz_config
            )
            self.result_cards.append(card)
        
        # Clear and rebuild the dashboard with all cards
        self.results_container.clear()
        
        if not self.result_cards:
            # Show empty state
            empty_html = """
            <div style="text-align: center; padding: 60px;">
                <p style="color: rgba(245, 245, 247, 0.6);">No insights yet</p>
            </div>
            """
            self.results_container.append(pn.pane.HTML(empty_html, sizing_mode='stretch_width'))
        else:
            # Create a responsive grid layout
            grid = pn.GridSpec(
                ncols=2,  # Default to 2 columns
                nrows=0,  # Auto-rows
                sizing_mode='stretch_both',
                margin=10,
                width_policy='max',
                height_policy='auto',
                align='start',
                css_classes=['dashboard-grid']
            )
            
            # Add cards to grid
            for i, card in enumerate(self.result_cards):
                row = i // 2
                col = i % 2
                grid[row, col] = card
            
            # Add CSS for responsive grid
            grid_styles = """
            .dashboard-grid {
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(500px, 1fr));
                gap: 20px;
                padding: 10px;
                width: 100%;
                box-sizing: border-box;
            }
            
            @media (max-width: 1100px) {
                .dashboard-grid {
                    grid-template-columns: 1fr;
                }
            }
            
            .dashboard-grid > * {
                min-height: 300px;
                transition: all 0.3s ease;
            }
            """
            
            # Add styles to the document
            pn.pane.HTML(
                f"<style>{grid_styles}</style>",
                sizing_mode='stretch_width'
            ).servable()
            
            self.results_container.append(grid)
    
    def _create_result_card(self, query, sql, viz, viz_config):
        """Create minimal Apple-style result card"""
        
        timestamp = datetime.now().strftime("%H:%M")
        viz_type = viz_config.get('visualization_type', 'chart')
        title = viz_config.get('title', 'Results')
        description = viz_config.get('description', '')
        
        # Create card with glassmorphism
        card_content = pn.Column(
            # Header with title and timestamp
            pn.pane.HTML(f"""
                <div style="
                    padding: 20px 20px 15px 20px;
                    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                ">
                    <div style="font-size: 13px; color: rgba(245, 245, 247, 0.6); margin-bottom: 8px;">
                        {timestamp} • {viz_type.replace('_', ' ').title()}
                    </div>
                    <div style="font-size: 16px; font-weight: 600; color: #f5f5f7; line-height: 1.4;">
                        {title if title != 'Results' else query}
                    </div>
                </div>
            """, sizing_mode='stretch_width'),
            
            # Description (if exists)
            *([pn.pane.HTML(f"""
                <div style="
                    padding: 0 20px 15px 20px;
                    font-size: 14px;
                    color: rgba(245, 245, 247, 0.8);
                    line-height: 1.5;
                ">
                    {description}
                </div>
            """, sizing_mode='stretch_width')] if description else []),
            
            # Main visualization
            pn.pane.HTML("<div style='padding: 0 10px;'>", margin=0, sizing_mode='stretch_width'),
            pn.pane.Pane(viz, sizing_mode='stretch_width'),
            pn.pane.HTML("</div>", margin=0, sizing_mode='stretch_width'),
            
            # SQL toggle (if SQL is available)
            *([
                pn.pane.HTML("<div style='margin-top: 15px;'></div>", margin=0, sizing_mode='stretch_width'),
                pn.Accordion(
                    ("SQL Query", pn.pane.Markdown(f"```sql\n{sql}\n```")),
                    active=[],
                    toggle=True,
                    sizing_mode='stretch_width',
                    styles={
                        'background': 'rgba(30, 30, 30, 0.7)',
                        'border-radius': '12px',
                        'margin': '0 10px 10px 10px',
                        'border': '1px solid rgba(255, 255, 255, 0.05)'
                    },
                    header_background='rgba(0, 0, 0, 0.2)',
                    header_color='rgba(245, 245, 247, 0.8)'
                )
            ] if sql else []),
            
            styles={
                'background': 'rgba(30, 30, 30, 0.7)',
                'border-radius': '16px',
                'border': '1px solid rgba(255, 255, 255, 0.1)',
                'box-shadow': '0 8px 32px rgba(0, 0, 0, 0.2)',
                'overflow': 'hidden',
                'transition': 'all 0.3s ease',
                'margin': '0 5px',
                'flex': '1 1 45%',  # Flex grow and basis for responsive layout
                'min-width': '400px',  # Minimum width before wrapping
                'max-width': '100%'   # Ensure it doesn't overflow
            },
            sizing_mode='stretch_both',
            margin=(0, 0, 20, 0)
        )
        
        return card_content
    
    def _on_clear_results(self, event):
        """Clear all results"""
        
        self.result_cards.clear()
        self.results_container.clear()
        
        empty_html = """
        <div style="
            text-align: center;
            padding: 60px 20px;
            background: rgba(30, 30, 30, 0.4);
            border-radius: 20px;
            border: 1px solid rgba(255, 255, 255, 0.05);
        ">
            <div style="font-size: 48px; margin-bottom: 15px; opacity: 0.5;">✨</div>
            <p style="color: rgba(245, 245, 247, 0.6); font-size: 16px;">
                All insights cleared. Ask another question!
            </p>
        </div>
        """
        
        self.results_container.append(pn.pane.HTML(empty_html, sizing_mode='stretch_width'))
        self.query_history.clear()
        self.clear_button.disabled = True
        self._update_stats()
        
        pn.state.notifications.info("🧹 Cleared all insights", duration=2000)
        logger.info("Results cleared")
    
    def create_app(self):
        """Create the complete Apple-inspired dark theme application"""
        
        app = pn.Column(
            pn.pane.HTML(APPLE_DARK_CSS),
            self._create_header(),
            self._create_stats_row(),
            self._create_upload_section(),
            self._create_query_section(),
            self._create_results_section(),
            sizing_mode='stretch_width',
            styles={
                'background': '#000000',
                'padding': '40px',
                'min-height': '100vh'
            }
        )
        
        return app