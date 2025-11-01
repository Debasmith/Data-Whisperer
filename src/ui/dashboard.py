"""
Main Dashboard UI for DataWhisperer
Save this as: src/ui/dashboard.py
"""

import panel as pn
import param
from typing import Optional
from src.query.query_processor import QueryProcessor
from src.visualization.viz_engine import VisualizationEngine
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class DataWhispererDashboard(param.Parameterized):
    """Main dashboard for DataWhisperer application"""
    
    # State parameters
    data_loaded = param.Boolean(default=False)
    processing = param.Boolean(default=False)
    
    def __init__(self, config, **params):
        super().__init__(**params)
        self.config = config
        
        # Initialize components
        self.query_processor = QueryProcessor(config)
        self.viz_engine = VisualizationEngine(config)
        
        # UI components
        self.file_input = None
        self.query_input = None
        self.submit_button = None
        self.clear_button = None
        self.status_indicator = None
        self.schema_display = None
        self.results_column = None
        
        # Query history
        self.query_history = []
        
        logger.info("Dashboard initialized")
    
    def _create_header(self):
        """Create modern dashboard header"""
        header_style = {
            'background': 'linear-gradient(135deg, #1a237e 0%, #283593 100%)',
            'color': 'white',
            'padding': '25px',
            'border-radius': '8px',
            'box-shadow': '0 4px 6px rgba(0,0,0,0.1)',
            'margin-bottom': '20px'
        }
        
        return pn.Row(
            pn.pane.PNG('https://img.icons8.com/color/96/000000/data-configuration.png', width=80, height=80),
            pn.Column(
                pn.pane.Markdown(
                    "<h1 style='color: white; margin: 0; font-weight: 600;'>DataWhisperer</h1>"
                ),
                pn.pane.Markdown(
                    "<h3 style='color: rgba(255,255,255,0.9); margin: 5px 0 0 0; font-weight: 400;'>AI-Powered Data Analysis Dashboard</h3>"
                ),
                margin=(10, 0, 0, 15)
            ),
            styles=header_style,
            sizing_mode='stretch_width'
        )
    
    def _create_upload_section(self):
        """Create modern file upload section"""
        self.file_input = pn.widgets.FileInput(
            name='Upload Data File',
            accept=','.join(self.config.supported_file_types),
            align='center',
            width=300
        )
        self.file_input.param.watch(self._on_file_upload, 'value')
        
        upload_icon = pn.pane.HTML(
            '<i class="fas fa-cloud-upload-alt" style="font-size: 48px; color: #5c6bc0; margin: 20px 0;"></i>',
            align='center'
        )
        
        supported_formats = pn.pane.Markdown(
            "<div style='text-align: center; color: #666;'>"
            "<h3 style='margin-bottom: 10px;'>Supported Formats</h3>"
            f"<p style='margin: 0 0 10px 0;'>{', '.join([f'<code>{fmt}</code>' for fmt in self.config.supported_file_types])}</p>"
            f"<p><strong>Max file size:</strong> {self.config.max_file_size_mb}MB</p>"
            "</div>"
        )
        
        return pn.Card(
            pn.Column(
                upload_icon,
                pn.Row(
                    self.file_input,
                    align='center',
                    sizing_mode='fixed',
                    margin=(0, 0, 20, 0)
                ),
                supported_formats,
                align='center',
                sizing_mode='stretch_width',
                margin=(0, 20)
            ),
            title="📁 UPLOAD YOUR DATA",
            header_background='#f8f9fa',
            header_color='#333',
            sizing_mode='stretch_width',
            styles={
                'background': '#ffffff',
                'border': '1px solid #e0e0e0',
                'border-radius': '8px',
                'box-shadow': '0 2px 4px rgba(0,0,0,0.05)'
            },
            margin=(0, 0, 20, 0)
        )
    
    def _create_schema_section(self):
        """Create modern schema display section"""
        self.schema_display = pn.pane.Markdown(
            "<div style='font-size: 0.95em; line-height: 1.6;'><em>No data loaded yet</em></div>",
            sizing_mode='stretch_width'
        )
        
        return pn.Card(
            self.schema_display,
            title="🔍 DATA SCHEMA",
            collapsed=True,
            header_background='#f8f9fa',
            header_color='#333',
            sizing_mode='stretch_width',
            styles={
                'background': '#ffffff',
                'border': '1px solid #e0e0e0',
                'border-radius': '8px',
                'box-shadow': '0 2px 4px rgba(0,0,0,0.05)'
            },
            margin=(0, 0, 20, 0)
        )
    
    def _create_query_section(self):
        """Create modern query input section"""
        self.query_input = pn.widgets.TextAreaInput(
            name='Ask a question about your data',
            placeholder='Example: Show me the top 5 customers by revenue\nExample: What is the average order value?\nExample: Count customers by region',
            height=120,
            disabled=True,
            sizing_mode='stretch_width',
            styles={'font-size': '1.1em'}
        )
        
        self.submit_button = pn.widgets.Button(
            name='🔍 Analyze Data',
            button_type='primary',
            button_style='solid',
            width=200,
            height=40,
            disabled=True,
            align='center'
        )
        self.submit_button.on_click(self._on_submit_query)
        
        self.clear_button = pn.widgets.Button(
            name='🗑️ Clear All',
            button_type='light',
            width=150,
            height=40,
            disabled=True,
            align='center'
        )
        self.clear_button.on_click(self._on_clear_results)
        
        self.status_indicator = pn.indicators.LoadingSpinner(
            value=False,
            size=25,
            color='#5c6bc0',
            bgcolor='light',
            visible=False
        )
        
        button_row = pn.Row(
            self.submit_button,
            self.clear_button,
            self.status_indicator,
            align='center',
            justify='center',
            margin=(10, 0, 20, 0)
        )
        
        examples = pn.Accordion(
            ("💡 Example Questions", pn.Column(
                pn.pane.Markdown(
                    "<div style='font-size: 1em; color: #555; line-height: 1.8;'>"
                    "<ul style='padding-left: 20px; margin: 0;'>"
                    "<li>What are the total sales by product category?</li>"
                    "<li>Show me customers who haven't been contacted in 60 days</li>"
                    "<li>Calculate the average contract value by industry</li>"
                    "<li>Count active vs inactive customers</li>"
                    "<li>What is the distribution of customers across regions?</li>"
                    "<li>Show me monthly revenue trends for the last year</li>"
                    "<li>What is the correlation between price and sales volume?</li>"
                    "<li>Compare this year's sales to last year's</li>"
                    "</ul>"
                    "</div>"
                ),
                margin=(0, 0, 0, 10)
            )),
            active=[],
            active_header_background='#f8f9fa',
            header_background='#f1f3f5',
            header_color='#495057',
            sizing_mode='stretch_width'
        )
        
        return pn.Card(
            pn.Column(
                pn.pane.Markdown("<div style='margin: 0 0 15px 0'><h3>🔍 ASK A QUESTION</h3></div>"),
                self.query_input,
                button_row,
                examples,
                sizing_mode='stretch_width',
                margin=(0, 20, 15, 20)
            ),
            styles={
                'background': '#ffffff',
                'border': '1px solid #e0e0e0',
                'border-radius': '8px',
                'box-shadow': '0 2px 4px rgba(0,0,0,0.05)'
            },
            margin=(0, 0, 20, 0),
            sizing_mode='stretch_width'
        )
    
    def _create_results_section(self):
        """Create modern results display section"""
        self.results_column = pn.Column(
            pn.pane.HTML(
                """
                <div style="text-align: center; padding: 40px; background: #f8f9fa; border-radius: 8px; border: 2px dashed #dee2e6;">
                    <i class="fas fa-chart-line" style="font-size: 48px; color: #adb5bd; margin-bottom: 15px;"></i>
                    <h3 style="color: #6c757d; margin: 10px 0;">Your Analysis Will Appear Here</h3>
                    <p style="color: #868e96; max-width: 600px; margin: 0 auto;">
                        Ask a question about your data to generate visualizations and insights.
                    </p>
                </div>
                """,
                sizing_mode='stretch_width'
            ),
            sizing_mode='stretch_width',
            min_height=300
        )
        
        return pn.Card(
            self.results_column,
            title="📊 ANALYSIS RESULTS",
            header_background='#f8f9fa',
            header_color='#333',
            sizing_mode='stretch_width',
            styles={
                'background': '#ffffff',
                'border': '1px solid #e0e0e0',
                'border-radius': '8px',
                'box-shadow': '0 2px 4px rgba(0,0,0,0.05)'
            }
        )
    
    def _on_file_upload(self, event):
        """Handle file upload with improved feedback and error handling"""
        if event.new is None:
            return
        
        try:
            # Show loading state
            self.status_indicator.visible = True
            self.status_indicator.value = True
            
            # Disable UI during processing
            self.file_input.disabled = True
            self.query_input.disabled = True
            self.submit_button.disabled = True
            
            logger.info("Processing uploaded file...")
            
            # Check file size
            max_size_mb = self.config.max_file_size_mb
            file_size_mb = len(event.new) / (1024 * 1024)
            
            if file_size_mb > max_size_mb:
                error_msg = f"❌ File size ({file_size_mb:.2f}MB) exceeds maximum allowed size ({max_size_mb}MB)"
                logger.error(error_msg)
                pn.state.notifications.error(error_msg, duration=5000)
                self.file_input.value = None
                return
            
            # Get file info
            file_name = self.file_input.filename
            file_extension = file_name.split('.')[-1].lower()
            
            if file_extension not in self.config.supported_file_types:
                supported = ", ".join([f".{ext}" for ext in self.config.supported_file_types])
                error_msg = f"❌ Unsupported file type: .{file_extension}. Supported types: {supported}"
                logger.error(error_msg)
                pn.state.notifications.error(error_msg, duration=5000)
                self.file_input.value = None
                return
            
            # Show processing message
            processing_msg = f"🔍 Analyzing {file_name} ({file_size_mb:.1f} MB)..."
            pn.state.notifications.info(processing_msg, duration=3000)
            
            # Process the file in a separate thread to avoid blocking the UI
            def process_file():
                try:
                    # Load the dataset
                    df = self.query_processor.load_dataset(file_name, event.new)
                    
                    # Update UI on the main thread
                    pn.state.execute_on_main_thread(
                        self._update_ui_after_upload,
                        df,
                        file_name
                    )
                    
                    success_msg = f"✅ Successfully loaded {file_name} with {len(df.columns)} columns and {len(df):,} rows"
                    logger.info(success_msg)
                    pn.state.execute_on_main_thread(
                        pn.state.notifications.success,
                        success_msg,
                        duration=4000
                    )
                    
                except Exception as e:
                    error_msg = f"❌ Error processing {file_name}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    pn.state.execute_on_main_thread(
                        pn.state.notifications.error,
                        error_msg,
                        duration=6000
                    )
                    
                    # Reset file input on error
                    pn.state.execute_on_main_thread(
                        lambda: setattr(self.file_input, 'value', None)
                    )
                finally:
                    # Re-enable UI elements
                    pn.state.execute_on_main_thread(
                        lambda: setattr(self.status_indicator, 'visible', False)
                    )
                    pn.state.execute_on_main_thread(
                        lambda: setattr(self.status_indicator, 'value', False)
                    )
                    pn.state.execute_on_main_thread(
                        lambda: setattr(self.file_input, 'disabled', False)
                    )
                    pn.state.execute_on_main_thread(
                        lambda: setattr(self.query_input, 'disabled', False)
                    )
            
            # Start the file processing in a separate thread
            import threading
            thread = threading.Thread(target=process_file)
            thread.daemon = True
            thread.start()
            
        except Exception as e:
            error_msg = f"❌ Unexpected error during file upload: {str(e)}"
            logger.error(error_msg, exc_info=True)
            pn.state.notifications.error(error_msg, duration=6000)
            
            # Reset UI state on error
            self.status_indicator.visible = False
            self.status_indicator.value = False
            self.file_input.disabled = False
            self.query_input.disabled = False
            self.file_input.value = None
    
    def _format_schema(self) -> str:
        """Format schema information for display"""
        if not self.query_processor.schema_details:
            return "*No schema information available*"
        
        md = f"**Table:** {self.query_processor.table_name}\n\n"
        md += f"**Rows:** {self.query_processor.row_count:,}\n\n"
        md += "**Columns:**\n\n"
        
        for detail in self.query_processor.schema_details:
            md += f"- {detail}\n"
        
        return md
    
    async def _on_submit_query(self, event):
        """Handle query submission with improved visualization and error handling"""
        if self.processing or not self.query_input.value:
            return
        
        # Set processing state
        self.processing = True
        self.submit_button.disabled = True
        self.status_indicator.visible = True
        self.status_indicator.value = True
        self.status_indicator.name = "Analyzing..."
        
        query = self.query_input.value.strip()
        logger.info(f"Processing query: {query}")
        
        try:
            # Show processing notification
            pn.state.notifications.info(f"Analyzing: {query}", duration=3000)
            
            # Process the query
            result = await self.query_processor.process_query(query)
            
            if not result or 'sql' not in result or 'viz_config' not in result:
                raise ValueError("Invalid result format from query processor")
            
            # Create visualization with error handling
            try:
                viz = self.viz_engine.create_visualization(
                    result['data'],
                    result['viz_config']
                )
                
                # Ensure the visualization has a reasonable size
                if hasattr(viz, 'sizing_mode'):
                    viz.sizing_mode = 'stretch_width'
                if hasattr(viz, 'width'):
                    viz.width = None  # Allow responsive width
                if hasattr(viz, 'height') and viz.height > 600:  # Cap height for large visualizations
                    viz.height = 600
                
            except Exception as viz_error:
                logger.error(f"Error creating visualization: {str(viz_error)}", exc_info=True)
                # Fallback to a data table if visualization fails
                viz = pn.widgets.Tabulator(
                    result['data'],
                    pagination='local',
                    page_size=10,
                    sizing_mode='stretch_width'
                )
                result['viz_config']['title'] = f"{result['viz_config'].get('title', 'Results')} (Data Table)"
            
            # Create result card
            result_card = self._create_result_card(
                query,
                result['sql'],
                viz,
                result['viz_config']
            )
            
            # Add to results (with smooth scroll to top)
            self.results_column.insert(0, result_card)
            
            # Add to query history
            self.query_history.append({
                'query': query,
                'sql': result['sql'],
                'timestamp': datetime.now().isoformat(),
                'viz_type': result['viz_config'].get('chart_type', 'table')
            })
            
            # Enable clear button if we have results
            self.clear_button.disabled = False
            
            # Show success message
            success_msg = f"✅ Analysis complete for: {query}"
            pn.state.notifications.success(success_msg, duration=4000)
            logger.info(f"Successfully processed query: {query}")
            
            # Clear the input field for the next query
            self.query_input.value = ""
            
        except Exception as e:
            error_msg = f"❌ Error processing query: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            # Show detailed error in a card
            error_card = pn.Card(
                pn.Column(
                    pn.pane.HTML(
                        '<div style="text-align: center; padding: 20px;">'
                        '<i class="fas fa-exclamation-triangle" style="font-size: 36px; color: #dc3545; margin-bottom: 15px;"></i>'
                        '<h3 style="color: #dc3545; margin: 10px 0;">Analysis Failed</h3>'
                        f'<p style="color: #6c757d;">{str(e)}</p>'
                        '<p style="color: #6c757d; font-size: 0.9em;">Please try rephrasing your question or check the data schema.</p>'
                        '</div>'
                    ),
                    pn.Accordion(
                        ("📋 View Query", pn.pane.Markdown(f"<div style='background: #f8f9fa; padding: 10px; border-radius: 4px;'><pre><code>{query}</code></pre></div>")),
                        active=[],
                        header_background='#f8f9fa',
                        header_color='#495057',
                        sizing_mode='stretch_width'
                    )
                ),
                styles={
                    'background': '#fff',
                    'border': '1px solid #f5c6cb',
                    'border-radius': '8px',
                    'box-shadow': '0 2px 4px rgba(0,0,0,0.05)'
                },
                margin=(0, 0, 20, 0),
                sizing_mode='stretch_width'
            )
            self.results_column.insert(0, error_card)
            self.clear_button.disabled = False
            
        finally:
            # Reset UI state
            self.processing = False
            self.submit_button.disabled = False
            self.status_indicator.visible = False
            self.status_indicator.value = False
            self.status_indicator.name = ""
    
    def _create_result_card(self, query, sql, viz, viz_config):
        """Create a modern card for displaying results"""
        # Format the SQL display with syntax highlighting
        sql_display = pn.pane.Markdown(
            f"<div style='background: #f8f9fa; border-radius: 6px; padding: 10px;'>"
            f"<pre style='margin: 0;'><code class='language-sql'>{sql}</code></pre>"
            "</div>",
            sizing_mode='stretch_width'
        )
        
        # Create collapsible SQL section
        sql_section = pn.Accordion(
            ("🔍 VIEW GENERATED SQL", sql_display),
            active=[],
            header_background='#f1f3f5',
            header_color='#495057',
            sizing_mode='stretch_width'
        )
        
        # Format the insight
        insight = viz_config.get('description', 'Analysis complete')
        insight_pane = pn.Card(
            pn.Row(
                pn.pane.HTML('<i class="fas fa-lightbulb" style="font-size: 24px; color: #ffc107; margin-right: 15px;"></i>'),
                pn.pane.Markdown(f"<div style='font-size: 1.05em'><strong>Insight:</strong> {insight}</div>"),
                align='center',
                margin=(0, 0, 10, 0)
            ),
            styles={
                'background': '#fff8e1',
                'border': '1px solid #ffe082',
                'border-radius': '8px',
                'padding': '15px'
            },
            margin=(0, 0, 20, 0),
            sizing_mode='stretch_width'
        )
        
        # Create the main result card
        result_card = pn.Card(
            pn.Column(
                pn.Row(
                    pn.pane.HTML('<i class="fas fa-question-circle" style="font-size: 20px; color: #5c6bc0; margin-right: 10px;"></i>'),
                    pn.pane.Markdown(f"<div style='font-size: 1.1em'><strong>{query}</strong></div>"),
                    align='center',
                    margin=(0, 0, 15, 0)
                ),
                insight_pane,
                pn.pane.HTML('<hr style="border-top: 1px solid #e9ecef; margin: 15px 0;">'),
                viz,
                pn.pane.HTML('<div style="height: 15px"></div>'),
                sql_section,
                sizing_mode='stretch_width',
                margin=(0, 10, 10, 10)
            ),
            title=f"📊 {viz_config.get('title', 'Analysis Result').upper()}",
            header_background='#f8f9fa',
            header_color='#333',
            styles={
                'background': '#ffffff',
                'border': '1px solid #e0e0e0',
                'border-radius': '8px',
                'box-shadow': '0 2px 4px rgba(0,0,0,0.05)',
                'margin-bottom': '20px'
            },
            sizing_mode='stretch_width',
            margin=(0, 0, 20, 0)
        )
        
        # Add timestamp and action buttons
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        footer = pn.Row(
            pn.pane.Markdown(
                f"<div style='color: #6c757d; font-size: 0.85em;'><i class='far fa-clock'></i> {timestamp}</div>",
                margin=(0, 0, 0, 10)
            ),
            pn.layout.HSpacer(),
            pn.widgets.Button(
                name='🔄 Refresh',
                button_type='light',
                width=100,
                height=30,
                margin=(0, 5)
            ),
            pn.widgets.Button(
                name='📥 Export',
                button_type='light',
                width=100,
                height=30,
                margin=(0, 5)
            ),
            align='center',
            sizing_mode='stretch_width',
            margin=(0, 10, 10, 10)
        )
        
        return pn.Column(
            result_card,
            footer,
            sizing_mode='stretch_width',
            margin=(0, 0, 20, 0)
        )
    
    def _on_clear_results(self, event):
        """Clear all results"""
        self.results_column.clear()
        self.results_column.append(
            pn.pane.Markdown("*Ask a question to see results*")
        )
        self.query_history.clear()
        self.clear_button.disabled = True
        
        logger.info("Results cleared")
        pn.state.notifications.info("Results cleared", duration=2000)
    
    def create_app(self):
        """Create the complete dashboard application"""
        # Main layout
        app = pn.Column(
            self._create_header(),
            self._create_upload_section(),
            self._create_schema_section(),
            self._create_query_section(),
            self._create_results_section(),
            sizing_mode='stretch_width',
            styles={'background': '#f5f5f5', 'padding': '20px'}
        )
        
        return app