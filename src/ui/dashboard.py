
import threading
from datetime import datetime
from pathlib import Path

import panel as pn
import param

from src.query.query_processor import QueryProcessor
from src.visualization.viz_engine import VisualizationEngine
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


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
        self.results_column = None
        self.stats_row = None
        
        self.query_history = []
        
        logger.info("✨ Modern dashboard initialized")
    
    def _create_header(self):
        
        header_html = """
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 30px 40px;
            border-radius: 12px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            margin-bottom: 30px;
        ">
            <div style="display: flex; align-items: center; gap: 20px;">
                <div style="
                    background: rgba(255,255,255,0.2);
                    padding: 15px;
                    border-radius: 12px;
                    backdrop-filter: blur(10px);
                ">
                    <svg width="50" height="50" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <path d="M3 3h7v7H3V3zm11 0h7v7h-7V3zM3 14h7v7H3v-7zm11 0h7v7h-7v-7z" 
                              fill="white" opacity="0.9"/>
                    </svg>
                </div>
                <div>
                    <h1 style="
                        color: white;
                        margin: 0;
                        font-size: 36px;
                        font-weight: 700;
                        letter-spacing: -0.5px;
                    ">DataWhisperer</h1>
                    <p style="
                        color: rgba(255,255,255,0.9);
                        margin: 5px 0 0 0;
                        font-size: 16px;
                        font-weight: 400;
                    ">🤖 AI-Powered Dashboard Generation</p>
                </div>
            </div>
        </div>
        """
        
        return pn.pane.HTML(header_html, sizing_mode='stretch_width')
    
    def _create_stats_row(self):
        """Create stats display for loaded data"""
        
        self.stats_row = pn.Row(
            visible=False,
            sizing_mode='stretch_width',
            margin=(0, 0, 20, 0)
        )
        
        return self.stats_row
    
    def _update_stats(self):
        """Update statistics display"""
        
        if not self.query_processor.table_name:
            self.stats_row.visible = False
            return
        
        stats_html = f"""
        <div style="display: flex; gap: 15px; margin-bottom: 20px;">
            <div style="
                flex: 1;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 20px;
                border-radius: 10px;
                color: white;
                box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
            ">
                <div style="font-size: 14px; opacity: 0.9; margin-bottom: 5px;">📊 Dataset</div>
                <div style="font-size: 24px; font-weight: 700;">{self.query_processor.table_name}</div>
            </div>
            <div style="
                flex: 1;
                background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                padding: 20px;
                border-radius: 10px;
                color: white;
                box-shadow: 0 4px 15px rgba(240, 147, 251, 0.3);
            ">
                <div style="font-size: 14px; opacity: 0.9; margin-bottom: 5px;">📝 Rows</div>
                <div style="font-size: 24px; font-weight: 700;">{self.query_processor.row_count:,}</div>
            </div>
            <div style="
                flex: 1;
                background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
                padding: 20px;
                border-radius: 10px;
                color: white;
                box-shadow: 0 4px 15px rgba(79, 172, 254, 0.3);
            ">
                <div style="font-size: 14px; opacity: 0.9; margin-bottom: 5px;">🔢 Columns</div>
                <div style="font-size: 24px; font-weight: 700;">{len(self.query_processor.schema_details)}</div>
            </div>
            <div style="
                flex: 1;
                background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
                padding: 20px;
                border-radius: 10px;
                color: white;
                box-shadow: 0 4px 15px rgba(67, 233, 123, 0.3);
            ">
                <div style="font-size: 14px; opacity: 0.9; margin-bottom: 5px;">✅ Status</div>
                <div style="font-size: 24px; font-weight: 700;">Ready</div>
            </div>
        </div>
        """
        
        self.stats_row.clear()
        self.stats_row.append(pn.pane.HTML(stats_html, sizing_mode='stretch_width'))
        self.stats_row.visible = True
    
    def _create_upload_section(self):
        """Create modern file upload section"""
        
        self.file_input = pn.widgets.FileInput(
            accept=','.join(self.config.supported_file_types),
            sizing_mode='stretch_width',
            height=50
        )
        self.file_input.param.watch(self._on_file_upload, 'value')
        
        upload_html = """
        <div style="
            background: white;
            border: 3px dashed #cbd5e0;
            border-radius: 12px;
            padding: 40px;
            text-align: center;
            transition: all 0.3s ease;
        " onmouseover="this.style.borderColor='#667eea'; this.style.background='#f7fafc';" 
           onmouseout="this.style.borderColor='#cbd5e0'; this.style.background='white';">
            <div style="font-size: 48px; margin-bottom: 15px;">📁</div>
            <h3 style="color: #2d3748; margin: 10px 0;">Drop Your Data File Here</h3>
            <p style="color: #718096; margin: 10px 0 20px 0;">
                or click below to browse
            </p>
            <div style="
                display: inline-block;
                background: #edf2f7;
                padding: 8px 16px;
                border-radius: 6px;
                font-size: 13px;
                color: #4a5568;
            ">
                Supported: CSV, Excel (max 100MB)
            </div>
        </div>
        """
        
        return pn.Column(
            pn.pane.HTML(upload_html, sizing_mode='stretch_width'),
            self.file_input,
            sizing_mode='stretch_width',
            margin=(0, 0, 20, 0)
        )
    
    def _create_schema_section(self):
        """Create collapsible schema display"""
        
        self.schema_display = pn.pane.Markdown(
            "*Upload a file to see the schema*",
            styles={'color': '#718096', 'font-style': 'italic'},
            sizing_mode='stretch_width'
        )
        
        return pn.Card(
            self.schema_display,
            title="📋 Data Schema",
            collapsed=True,
            collapsible=True,
            header_background='#f7fafc',
            styles={
                'background': 'white',
                'border': '1px solid #e2e8f0',
                'border-radius': '10px',
                'box-shadow': '0 1px 3px rgba(0,0,0,0.1)'
            },
            margin=(0, 0, 20, 0),
            sizing_mode='stretch_width'
        )
    
    def _create_query_section(self):
        """Create modern query input section"""
        
        self.query_input = pn.widgets.TextAreaInput(
            placeholder='Ask anything about your data...\n\nExamples:\n• What are the top 5 products by revenue?\n• Show me monthly sales trends\n• Count customers by region',
            height=120,
            disabled=True,
            sizing_mode='stretch_width',
            styles={
                'font-size': '16px',
                'border': '2px solid #e2e8f0',
                'border-radius': '8px',
                'padding': '12px'
            }
        )
        
        self.submit_button = pn.widgets.Button(
            name='✨ Analyze',
            button_type='primary',
            width=150,
            height=45,
            disabled=True,
            styles={
                'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                'border': 'none',
                'font-size': '16px',
                'font-weight': '600',
                'border-radius': '8px',
                'box-shadow': '0 4px 15px rgba(102, 126, 234, 0.4)'
            }
        )
        self.submit_button.on_click(self._on_submit_query)
        
        self.clear_button = pn.widgets.Button(
            name='🗑️ Clear',
            button_type='light',
            width=120,
            height=45,
            disabled=True,
            styles={
                'border-radius': '8px',
                'font-size': '14px'
            }
        )
        self.clear_button.on_click(self._on_clear_results)
        
        self.status_indicator = pn.indicators.LoadingSpinner(
            value=False,
            size=30,
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
            margin=(15, 0)
        )
        
        # Smart examples
        examples_html = """
        <div style="background: #f7fafc; padding: 20px; border-radius: 10px; margin-top: 15px;">
            <h4 style="color: #2d3748; margin: 0 0 15px 0; font-size: 16px;">💡 Smart Query Examples</h4>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                <div style="background: white; padding: 12px; border-radius: 6px; border-left: 3px solid #667eea;">
                    <strong style="color: #667eea;">Aggregations</strong>
                    <ul style="margin: 8px 0 0 0; padding-left: 20px; color: #4a5568; font-size: 14px;">
                        <li>Total sales by category</li>
                        <li>Average order value</li>
                        <li>Count of active users</li>
                    </ul>
                </div>
                <div style="background: white; padding: 12px; border-radius: 6px; border-left: 3px solid #f093fb;">
                    <strong style="color: #f093fb;">Trends</strong>
                    <ul style="margin: 8px 0 0 0; padding-left: 20px; color: #4a5568; font-size: 14px;">
                        <li>Monthly revenue trend</li>
                        <li>Sales over time</li>
                        <li>Daily user growth</li>
                    </ul>
                </div>
                <div style="background: white; padding: 12px; border-radius: 6px; border-left: 3px solid #4facfe;">
                    <strong style="color: #4facfe;">Rankings</strong>
                    <ul style="margin: 8px 0 0 0; padding-left: 20px; color: #4a5568; font-size: 14px;">
                        <li>Top 10 customers</li>
                        <li>Best performing products</li>
                        <li>Lowest conversion rates</li>
                    </ul>
                </div>
                <div style="background: white; padding: 12px; border-radius: 6px; border-left: 3px solid #43e97b;">
                    <strong style="color: #43e97b;">Distributions</strong>
                    <ul style="margin: 8px 0 0 0; padding-left: 20px; color: #4a5568; font-size: 14px;">
                        <li>Customer breakdown by region</li>
                        <li>Product category distribution</li>
                        <li>Status percentages</li>
                    </ul>
                </div>
            </div>
        </div>
        """
        
        return pn.Column(
            pn.pane.HTML('<h3 style="color: #2d3748; margin: 0 0 15px 0;">💬 Ask Your Question</h3>'),
            self.query_input,
            button_row,
            pn.pane.HTML(examples_html, sizing_mode='stretch_width'),
            styles={
                'background': 'white',
                'padding': '25px',
                'border-radius': '12px',
                'border': '1px solid #e2e8f0',
                'box-shadow': '0 1px 3px rgba(0,0,0,0.1)'
            },
            sizing_mode='stretch_width',
            margin=(0, 0, 20, 0)
        )
    
    def _create_results_section(self):
        """Create modern results display"""
        
        empty_state_html = """
        <div style="
            text-align: center;
            padding: 60px 20px;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 12px;
            min-height: 300px;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
        ">
            <div style="font-size: 64px; margin-bottom: 20px;">📊</div>
            <h3 style="color: #2d3748; margin: 0 0 10px 0;">Ready to Analyze</h3>
            <p style="color: #718096; max-width: 500px; margin: 0;">
                Upload your data and ask questions to generate intelligent visualizations and insights
            </p>
        </div>
        """
        
        self.results_column = pn.Column(
            pn.pane.HTML(empty_state_html, sizing_mode='stretch_width'),
            sizing_mode='stretch_width'
        )
        
        return pn.Column(
            pn.pane.HTML('<h3 style="color: #2d3748; margin: 0 0 15px 0;">📈 Analysis Results</h3>'),
            self.results_column,
            styles={
                'background': 'white',
                'padding': '25px',
                'border-radius': '12px',
                'border': '1px solid #e2e8f0',
                'box-shadow': '0 1px 3px rgba(0,0,0,0.1)'
            },
            sizing_mode='stretch_width'
        )
    
    def _on_file_upload(self, event):
        """Handle file upload with modern feedback"""
        
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
                f"📂 Processing {file_name} ({file_size_mb:.1f}MB)...",
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
        self.schema_display.object = self._format_schema()
        self.query_input.disabled = False
        self.submit_button.disabled = False
        self._update_stats()
        
        logger.info("✅ UI updated for dataset: %s", file_name)
    
    def _format_schema(self) -> str:
        """Format schema for display"""
        
        if not self.query_processor.schema_details:
            return "*No schema available*"
        
        md = f"### 📊 Table: `{self.query_processor.table_name}`\n\n"
        md += f"**Total Rows:** {self.query_processor.row_count:,}\n\n"
        md += "#### Columns:\n\n"
        
        for i, detail in enumerate(self.query_processor.schema_details, 1):
            md += f"{i}. {detail}\n"
        
        return md
    
    def _on_submit_query(self, event):
        """Handle query submission"""
        
        if self.processing or not self.query_input.value:
            return
        
        self.processing = True
        self.submit_button.disabled = True
        self.status_indicator.visible = True
        self.status_indicator.value = True
        
        query = self.query_input.value.strip()
        
        logger.info("🔍 Processing query: %s", query)
        pn.state.notifications.info(f"🤖 Analyzing: {query[:50]}...", duration=3000)
        
        def run_query():
            try:
                result = self.query_processor.process_query(query)
                
                if not result.get('success'):
                    raise ValueError(result.get('error', 'Query failed'))
                
                viz_config = result.get('viz_config', {})
                data = result.get('data')
                sql_query = result.get('sql_query', '')
                
                if data is None:
                    raise ValueError("No data returned")
                
                # Create visualization
                try:
                    viz = self.viz_engine.create_visualization(data, viz_config, query)
                except Exception as viz_error:
                    logger.warning("Viz creation failed, using table: %s", viz_error)
                    viz = pn.widgets.Tabulator(
                        data,
                        pagination='local',
                        page_size=20,
                        sizing_mode='stretch_width',
                        theme='modern'
                    )
                
                def apply_success():
                    result_card = self._create_result_card(query, sql_query, viz, viz_config)
                    self.results_column.insert(0, result_card)
                    
                    self.query_history.append({
                        'query': query,
                        'sql': sql_query,
                        'timestamp': datetime.now().isoformat(),
                        'viz_type': viz_config.get('visualization_type', 'table')
                    })
                    
                    self.clear_button.disabled = False
                    self.query_input.value = ""
                    
                    pn.state.notifications.success(
                        f"✅ Analysis complete!",
                        duration=4000
                    )
                
                pn.state.execute(apply_success)
                
            except Exception as exc:
                error_text = str(exc)
                logger.error("Query error: %s", error_text, exc_info=True)
                
                def apply_error():
                    error_html = f"""
                    <div style="
                        background: linear-gradient(135deg, #fee 0%, #fdd 100%);
                        border: 2px solid #f5c6cb;
                        border-radius: 10px;
                        padding: 25px;
                        margin-bottom: 20px;
                    ">
                        <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 15px;">
                            <div style="font-size: 36px;">⚠️</div>
                            <div>
                                <h4 style="color: #721c24; margin: 0;">Analysis Failed</h4>
                                <p style="color: #721c24; margin: 5px 0 0 0; opacity: 0.8;">{error_text}</p>
                            </div>
                        </div>
                        <div style="background: white; padding: 12px; border-radius: 6px; font-family: monospace; font-size: 13px; color: #555;">
                            <strong>Query:</strong> {query}
                        </div>
                    </div>
                    """
                    self.results_column.insert(0, pn.pane.HTML(error_html, sizing_mode='stretch_width'))
                    self.clear_button.disabled = False
                    pn.state.notifications.error(f"❌ {error_text[:100]}", duration=6000)
                
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
    
    def _create_result_card(self, query, sql, viz, viz_config):
        """Create modern result card"""
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        viz_type = viz_config.get('visualization_type', 'chart')
        title = viz_config.get('title', 'Results')
        description = viz_config.get('description', '')
        
        # Insight badge
        insight_html = f"""
        <div style="
            background: linear-gradient(135deg, #fff7cd 0%, #ffeaa7 100%);
            border-left: 4px solid #fdcb6e;
            padding: 15px 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        ">
            <div style="display: flex; align-items: center; gap: 10px;">
                <div style="font-size: 24px;">💡</div>
                <div>
                    <strong style="color: #2d3748;">Insight:</strong>
                    <span style="color: #4a5568;"> {description}</span>
                </div>
            </div>
        </div>
        """ if description else ""
        
        # SQL accordion
        sql_section = pn.Accordion(
            ("🔍 View Generated SQL", pn.pane.Markdown(
                f"```sql\n{sql}\n```",
                sizing_mode='stretch_width'
            )),
            active=[],
            header_background='#f7fafc',
            sizing_mode='stretch_width',
            styles={'margin-top': '15px'}
        )
        
        # Metadata
        meta_html = f"""
        <div style="
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 20px;
            background: #f7fafc;
            border-radius: 8px;
            margin-top: 15px;
            font-size: 13px;
            color: #718096;
        ">
            <span>🕐 {timestamp}</span>
            <span>📊 {viz_type.replace('_', ' ').title()}</span>
            <span>📝 {len(query)} characters</span>
        </div>
        """
        
        result_card = pn.Column(
            pn.pane.HTML(f'<h4 style="color: #2d3748; margin: 0 0 10px 0;">❓ {query}</h4>'),
            pn.pane.HTML(insight_html) if description else pn.Spacer(height=0),
            pn.pane.HTML(f'<div style="padding: 10px 0;"><strong style="color: #4a5568;">📈 {title}</strong></div>'),
            viz,
            sql_section,
            pn.pane.HTML(meta_html, sizing_mode='stretch_width'),
            styles={
                'background': 'white',
                'padding': '25px',
                'border-radius': '12px',
                'border': '1px solid #e2e8f0',
                'box-shadow': '0 4px 6px rgba(0,0,0,0.1)',
                'margin-bottom': '20px'
            },
            sizing_mode='stretch_width'
        )
        
        return result_card
    
    def _on_clear_results(self, event):
        """Clear all results"""
        
        self.results_column.clear()
        
        empty_html = """
        <div style="
            text-align: center;
            padding: 40px;
            background: #f7fafc;
            border-radius: 10px;
            color: #718096;
        ">
            <div style="font-size: 48px; margin-bottom: 10px;">🧹</div>
            <p>Results cleared. Ask another question!</p>
        </div>
        """
        
        self.results_column.append(pn.pane.HTML(empty_html, sizing_mode='stretch_width'))
        self.query_history.clear()
        self.clear_button.disabled = True
        
        pn.state.notifications.info("🧹 Results cleared", duration=2000)
        logger.info("Results cleared")
    
    def create_app(self):
        """Create the complete modern application"""
        
        app = pn.Column(
            self._create_header(),
            self._create_stats_row(),
            self._create_upload_section(),
            self._create_schema_section(),
            self._create_query_section(),
            self._create_results_section(),
            sizing_mode='stretch_width',
            styles={
                'background': '#f0f4f8',
                'padding': '30px',
                'min-height': '100vh'
            }
        )
        
        return app