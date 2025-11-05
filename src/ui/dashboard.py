import threading
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import traceback

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
    
    /* Fix textarea and input visibility */
    textarea, input, select {
        color: #f5f5f7 !important;
    }
    
    textarea::placeholder, input::placeholder {
        color: rgba(245, 245, 247, 0.5) !important;
    }
</style>
"""


class DataWhispererDashboard(param.Parameterized):
    
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
        self.results_grid = None
        self.stats_row = None
        
        self.query_history = []
        self.result_cards = []
        
        logger.info("dashboard initialized")
    
    def _create_header(self):
        
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
                        ">Ask multiple questions at once. Get instant dashboard insights.</p>
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
            <div class="glass smooth-transition hover-lift" style="padding: 24px; border-radius: 16px; text-align: center;">
                <div style="font-size: 13px; color: rgba(245, 245, 247, 0.6); font-weight: 500; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px;">Dataset</div>
                <div style="font-size: 26px; font-weight: 700; color: #007AFF; letter-spacing: -0.5px;">{self.query_processor.table_name}</div>
            </div>
            
            <div class="glass smooth-transition hover-lift" style="padding: 24px; border-radius: 16px; text-align: center;">
                <div style="font-size: 13px; color: rgba(245, 245, 247, 0.6); font-weight: 500; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px;">Rows</div>
                <div style="font-size: 26px; font-weight: 700; color: #30D158; letter-spacing: -0.5px;">{self.query_processor.row_count:,}</div>
            </div>
            
            <div class="glass smooth-transition hover-lift" style="padding: 24px; border-radius: 16px; text-align: center;">
                <div style="font-size: 13px; color: rgba(245, 245, 247, 0.6); font-weight: 500; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px;">Columns</div>
                <div style="font-size: 26px; font-weight: 700; color: #FF9F0A; letter-spacing: -0.5px;">{len(self.query_processor.schema_details)}</div>
            </div>
            
            <div class="glass smooth-transition hover-lift" style="padding: 24px; border-radius: 16px; text-align: center;">
                <div style="font-size: 13px; color: rgba(245, 245, 247, 0.6); font-weight: 500; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px;">Insights</div>
                <div style="font-size: 26px; font-weight: 700; color: #BF5AF2; letter-spacing: -0.5px;">{len(self.query_history)}</div>
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
                'border-radius': '12px',
                'color': '#f5f5f7'
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
            <div style="display: inline-flex; gap: 8px; flex-wrap: wrap; justify-content: center;">
                <span style="background: rgba(255, 255, 255, 0.1); padding: 6px 12px; border-radius: 8px; font-size: 13px; color: rgba(245, 245, 247, 0.7);">CSV</span>
                <span style="background: rgba(255, 255, 255, 0.1); padding: 6px 12px; border-radius: 8px; font-size: 13px; color: rgba(245, 245, 247, 0.7);">Excel</span>
                <span style="background: rgba(255, 255, 255, 0.1); padding: 6px 12px; border-radius: 8px; font-size: 13px; color: rgba(245, 245, 247, 0.7);">JSON</span>
                <span style="background: rgba(255, 255, 255, 0.1); padding: 6px 12px; border-radius: 8px; font-size: 13px; color: rgba(245, 245, 247, 0.7);">Parquet</span>
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
        """Create minimal query interface with proper text visibility"""
        
        self.query_input = pn.widgets.TextAreaInput(
            placeholder='Ask multiple questions (one per line):\nWhat is the total revenue?\nShow top 10 products by sales\nRevenue trend over time',
            height=140,
            disabled=True,
            sizing_mode='stretch_width',
            auto_grow=True,
            max_height=400,
            styles={
                'font-size': '16px',
                'background': 'rgba(30, 30, 30, 0.9)',
                'border': '1px solid rgba(255, 255, 255, 0.2)',
                'border-radius': '16px',
                'padding': '16px',
                'color': '#f5f5f7',
                'backdrop-filter': 'blur(20px)',
                'resize': 'vertical'
            },
            stylesheets=["""
                :host {
                    --design-background-color: rgba(30, 30, 30, 0.9);
                }
                textarea {
                    color: #f5f5f7 !important;
                    background: rgba(30, 30, 30, 0.9) !important;
                    resize: vertical !important;
                    min-height: 140px !important;
                }
                textarea::placeholder {
                    color: rgba(245, 245, 247, 0.5) !important;
                }
            """]
        )
        
        self.submit_button = pn.widgets.Button(
            name='✨ Analyze All',
            button_type='primary',
            width=160,
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
            name='Clear Dashboard',
            button_type='light',
            width=140,
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
        
        query_card_start = """
        <div class="glass" style="padding: 30px; border-radius: 20px; margin-bottom: 25px;">
            <h3 style="color: #f5f5f7; margin: 0 0 20px 0; font-size: 20px; font-weight: 600; letter-spacing: -0.3px;">Ask Multiple Questions</h3>
        </div>
        """
        
        return pn.Column(
            pn.pane.HTML(query_card_start.replace('</div>', ''), sizing_mode='stretch_width'),
            self.query_input,
            button_row,
            pn.pane.HTML('</div>'),
            sizing_mode='stretch_width'
        )
    
    def _create_results_section(self):
        """Create dashboard-style results container (like Power BI/Tableau)"""
        
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
                Upload your data and ask multiple questions at once.<br/>Each insight will appear here in a dashboard layout.
            </p>
        </div>
        """
        
        # Use GridBox for true dashboard-style layout
        self.results_grid = pn.GridBox(
            pn.pane.HTML(empty_state_html, sizing_mode='stretch_width'),
            ncols=2,  # 2 columns by default (Power BI style)
            sizing_mode='stretch_width'
        )
        
        return pn.Column(
            pn.pane.HTML("""
                <h3 style="
                    color: #f5f5f7;
                    margin: 0 0 20px 0;
                    font-size: 24px;
                    font-weight: 600;
                    letter-spacing: -0.5px;
                ">Dashboard</h3>
            """),
            self.results_grid,
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
        """Handle query submission with multi-query support and dashboard layout"""
        
        if self.processing or not self.query_input.value:
            return
        
        self.processing = True
        self.submit_button.disabled = True
        self.query_input.disabled = True
        self.status_indicator.visible = True
        self.status_indicator.value = True
        
        query_text = self.query_input.value.strip()
        
        logger.info("🔍 Processing queries: %s", query_text[:100])
        pn.state.notifications.info(f"🤖 Analyzing queries...", duration=2000)
        
        def run_queries():
            try:
                # process_query now returns a LIST of results
                results = self.query_processor.process_queries(query_text)
                
                # Validate results
                if not isinstance(results, list):
                    raise TypeError(f"Expected list of results, got {type(results)}")
                
                if not results:
                    raise ValueError("No results returned from query processor")
                
                # Process each result
                successful_results = []
                failed_results = []
                
                for idx, result in enumerate(results):
                    try:
                        # Validate result structure
                        if not isinstance(result, dict):
                            logger.error(f"Result {idx} is not a dict: {type(result)}")
                            failed_results.append({
                                'query': f'Query {idx+1}',
                                'error': f'Invalid result type: {type(result)}'
                            })
                            continue
                        
                        if not result.get('success', False):
                            error_msg = result.get('error', 'Unknown error')
                            logger.warning(f"Query {idx} failed: {error_msg}")
                            failed_results.append({
                                'query': result.get('query', f'Query {idx+1}'),
                                'error': error_msg
                            })
                            continue
                        
                        # Validate required fields
                        required_fields = ['query', 'data', 'viz_config']
                        missing_fields = [f for f in required_fields if f not in result]
                        if missing_fields:
                            logger.error(f"Result {idx} missing fields: {missing_fields}")
                            failed_results.append({
                                'query': result.get('query', f'Query {idx+1}'),
                                'error': f'Missing fields: {missing_fields}'
                            })
                            continue
                        
                        # Extract data safely
                        query = result.get('query', f'Query {idx+1}')
                        sql = result.get('sql', '')
                        data = result.get('data')
                        viz_config = result.get('viz_config', {})
                        
                        # Validate data
                        if data is None or (hasattr(data, 'empty') and data.empty):
                            logger.warning(f"Query {idx} returned empty data")
                            failed_results.append({
                                'query': query,
                                'error': 'Query returned no data'
                            })
                            continue
                        
                        # Create visualization
                        try:
                            viz = self.viz_engine.create_visualization(data, viz_config, query)
                            
                            successful_results.append({
                                'query': query,
                                'sql': sql,
                                'viz': viz,
                                'viz_config': viz_config,
                                'data': data
                            })
                            
                        except Exception as viz_error:
                            logger.error(f"Viz creation failed for query {idx}: {viz_error}", exc_info=True)
                            failed_results.append({
                                'query': query,
                                'error': f'Visualization error: {str(viz_error)}'
                            })
                    
                    except Exception as result_error:
                        logger.error(f"Error processing result {idx}: {result_error}", exc_info=True)
                        failed_results.append({
                            'query': f'Query {idx+1}',
                            'error': str(result_error)
                        })
                
                # Update UI on main thread
                def apply_results():
                    try:
                        # Show error notifications for failed queries
                        for failed in failed_results:
                            pn.state.notifications.warning(
                                f"⚠️ {failed['query']}: {failed['error'][:100]}",
                                duration=5000
                            )
                        
                        # Add successful visualizations to dashboard
                        if successful_results:
                            self._rebuild_dashboard_grid(successful_results)
                            
                            # Update query history
                            for result in successful_results:
                                self.query_history.append({
                                    'query': result['query'],
                                    'sql': result['sql'],
                                    'timestamp': datetime.now().isoformat(),
                                    'viz_type': result['viz_config'].get('visualization_type', 'unknown')
                                })
                            
                            self.clear_button.disabled = False
                            self.query_input.value = ""
                            self._update_stats()
                            
                            success_msg = f"✅ Added {len(successful_results)} insight(s) to dashboard!"
                            if failed_results:
                                success_msg += f" ({len(failed_results)} failed)"
                            pn.state.notifications.success(success_msg, duration=4000)
                        else:
                            pn.state.notifications.error(
                                "❌ All queries failed. Please check your questions and try again.",
                                duration=6000
                            )
                    
                    except Exception as ui_error:
                        logger.error(f"Error updating UI: {ui_error}", exc_info=True)
                        pn.state.notifications.error(f"UI Error: {str(ui_error)}", duration=6000)
                
                pn.state.execute(apply_results)
                
            except Exception as exc:
                error_text = str(exc)
                error_trace = traceback.format_exc()
                logger.error(f"Query processing error: {error_text}\n{error_trace}")
                
                def apply_error():
                    pn.state.notifications.error(f"❌ {error_text[:150]}", duration=6000)
                
                pn.state.execute(apply_error)
                
            finally:
                def reset_ui():
                    self.processing = False
                    self.submit_button.disabled = False
                    self.query_input.disabled = False
                    self.status_indicator.visible = False
                    self.status_indicator.value = False
                
                pn.state.execute(reset_ui)
        
        thread = threading.Thread(target=run_queries, daemon=True)
        thread.start()
    
    def _rebuild_dashboard_grid(self, new_results: List[Dict[str, Any]]):
        """Rebuild the entire dashboard grid with all visualizations (Power BI style)"""
        
        try:
            # Add new results to existing cards
            for result in new_results:
                card = self._create_result_card(
                    result['query'],
                    result['sql'],
                    result['viz'],
                    result['viz_config']
                )
                self.result_cards.append(card)
            
            # Clear the grid
            self.results_grid.clear()
            
            # Determine optimal layout based on number of cards
            n_cards = len(self.result_cards)
            
            if n_cards == 0:
                # Show empty state
                self._show_empty_dashboard()
                return
            
            # Smart grid layout (like Power BI)
            # - 1 card: Full width
            # - 2-4 cards: 2 columns
            # - 5+ cards: 2-3 columns based on viz types
            
            if n_cards == 1:
                ncols = 1
            elif n_cards <= 4:
                ncols = 2
            else:
                # For 5+ cards, use 2 columns (cleaner look)
                ncols = 2
            
            # Rebuild grid with new layout
            self.results_grid.ncols = ncols
            
            # Add all cards to grid
            for card in self.result_cards:
                self.results_grid.append(card)
            
            logger.info(f"✅ Dashboard rebuilt: {n_cards} cards in {ncols} columns")
            
        except Exception as e:
            logger.error(f"Error rebuilding dashboard: {e}", exc_info=True)
            pn.state.notifications.error(f"Dashboard layout error: {str(e)}", duration=5000)
    
    def _show_empty_dashboard(self):
        """Show empty state in dashboard"""
        empty_html = """
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
            <p style="color: rgba(245, 245, 247, 0.5); margin: 0; font-size: 16px;">
                Ask multiple questions to populate your dashboard.
            </p>
        </div>
        """
        self.results_grid.append(pn.pane.HTML(empty_html, sizing_mode='stretch_both'))
    
    def _create_result_card(self, query, sql, viz, viz_config):
        """Create minimal Apple-style result card for dashboard"""
        
        try:
            timestamp = datetime.now().strftime("%H:%M")
            viz_type = viz_config.get('visualization_type', 'chart')
            title = viz_config.get('title', 'Results')
            description = viz_config.get('description', '')
            
            # Determine card size based on viz type
            # KPIs and numbers: compact
            # Charts: standard
            # Tables: larger
            if viz_type in ['number', 'kpi', 'metric']:
                min_height = 200
            elif viz_type == 'table':
                min_height = 400
            else:
                min_height = 350
            
            # Create card with glassmorphism
            card_content = pn.Column(
                # Header
                pn.pane.HTML(f"""
                    <div style="
                        padding: 20px 20px 15px 20px;
                        border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                    ">
                        <div style="font-size: 13px; color: rgba(245, 245, 247, 0.6); margin-bottom: 8px;">
                            {timestamp} • {viz_type.replace('_', ' ').title()}
                        </div>
                        <div style="font-size: 16px; font-weight: 600; color: #f5f5f7; line-height: 1.4;">
                            {query}
                        </div>
                    </div>
                """, sizing_mode='stretch_width'),
                
                # Insight badge (if exists)
                pn.pane.HTML(f"""
                    <div style="
                        padding: 15px 20px;
                        background: rgba(0, 122, 255, 0.1);
                        border-left: 3px solid #007AFF;
                        margin: 15px 20px;
                        border-radius: 8px;
                    ">
                        <div style="font-size: 13px; color: rgba(245, 245, 247, 0.6); margin-bottom: 4px;">
                            💡 Insight
                        </div>
                        <div style="font-size: 14px; color: #f5f5f7; line-height: 1.5;">
                            {description}
                        </div>
                    </div>
                """, sizing_mode='stretch_width') if description else pn.Spacer(height=0),
                
                # Visualization container
                pn.pane.HTML('<div style="padding: 0 20px 20px 20px;">', sizing_mode='stretch_width'),
                viz,
                pn.pane.HTML('</div>', sizing_mode='stretch_width'),
                
                sizing_mode='stretch_both',
                min_height=min_height,
                styles={
                    'background': 'rgba(30, 30, 30, 0.7)',
                    'border': '1px solid rgba(255, 255, 255, 0.1)',
                    'border-radius': '20px',
                    'backdrop-filter': 'blur(20px)',
                    'overflow': 'hidden'
                }
            )
            
            return card_content
            
        except Exception as e:
            logger.error(f"Error creating result card: {e}", exc_info=True)
            # Return error card
            return pn.pane.HTML(f"""
                <div style="
                    background: rgba(255, 0, 0, 0.1);
                    border: 1px solid rgba(255, 0, 0, 0.3);
                    border-radius: 20px;
                    padding: 30px;
                    text-align: center;
                    color: #ff6b6b;
                ">
                    <h4>⚠️ Card Creation Error</h4>
                    <p>{str(e)}</p>
                </div>
            """, sizing_mode='stretch_both', height=200)
    
    def _on_clear_results(self, event):
        """Clear all results from dashboard"""
        
        try:
            self.result_cards.clear()
            self.query_history.clear()
            
            self.results_grid.clear()
            self._show_empty_dashboard()
            
            self.clear_button.disabled = True
            self._update_stats()
            
            pn.state.notifications.info("🧹 Dashboard cleared", duration=2000)
            logger.info("Dashboard cleared")
            
        except Exception as e:
            logger.error(f"Error clearing dashboard: {e}", exc_info=True)
            pn.state.notifications.error(f"Clear error: {str(e)}", duration=4000)
    
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