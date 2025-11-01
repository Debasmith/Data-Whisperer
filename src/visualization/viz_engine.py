"""
Intelligent Visualization Engine with Smart Chart Selection
"""

import pandas as pd
import panel as pn
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, Optional
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class VisualizationEngine:
    """Smart visualization engine that creates appropriate charts"""
    
    def __init__(self, config):
        self.config = config
        self.color_palette = config.get_color_palette()
    
    def create_visualization(
        self,
        data: pd.DataFrame,
        viz_config: Dict[str, Any],
        query: str
    ):
        """Create visualization based on config and data characteristics"""
        
        viz_type = viz_config.get('visualization_type', 'auto')
        
        # Auto-detect if not specified or if 'auto'
        if viz_type == 'auto':
            viz_type = self._auto_detect_viz_type(data, query, viz_config)
            viz_config['visualization_type'] = viz_type
            logger.info(f"🎨 Auto-detected visualization type: {viz_type}")
        
        # Create visualization based on type
        viz_creators = {
            'number': self._create_number_display,
            'metric': self._create_number_display,
            'kpi': self._create_number_display,
            'bar': self._create_bar_chart,
            'horizontal_bar': self._create_horizontal_bar,
            'line': self._create_line_chart,
            'pie': self._create_pie_chart,
            'donut': self._create_donut_chart,
            'scatter': self._create_scatter_plot,
            'area': self._create_area_chart,
            'heatmap': self._create_heatmap,
            'table': self._create_table,
            'treemap': self._create_treemap,
            'funnel': self._create_funnel_chart
        }
        
        creator = viz_creators.get(viz_type, self._create_table)
        
        try:
            return creator(data, viz_config)
        except Exception as e:
            logger.error(f"Error creating {viz_type} visualization: {e}")
            # Fallback to table
            return self._create_table(data, viz_config)
    
    def _auto_detect_viz_type(
        self,
        data: pd.DataFrame,
        query: str,
        viz_config: Dict[str, Any]
    ) -> str:
        """Intelligently detect the best visualization type"""
        
        query_lower = query.lower()
        n_rows = len(data)
        n_cols = len(data.columns)
        
        # Single value queries (count, sum, average, etc.)
        if n_rows == 1 and n_cols == 1:
            return 'number'
        
        if n_rows == 1 and n_cols <= 3:
            return 'number'
        
        # Keywords for specific chart types
        if any(word in query_lower for word in ['count', 'how many', 'number of', 'total']):
            if n_rows == 1:
                return 'number'
            elif n_rows <= 10:
                return 'pie'
            else:
                return 'bar'
        
        if any(word in query_lower for word in ['trend', 'over time', 'timeline', 'history']):
            return 'line'
        
        if any(word in query_lower for word in ['distribution', 'breakdown', 'composition']):
            if n_rows <= 10:
                return 'pie'
            else:
                return 'bar'
        
        if any(word in query_lower for word in ['compare', 'comparison', 'versus', 'vs']):
            return 'bar'
        
        if any(word in query_lower for word in ['correlation', 'relationship', 'scatter']):
            return 'scatter'
        
        # Data shape analysis
        if n_rows <= 10 and n_cols == 2:
            # Check if one column looks like a category
            if data.iloc[:, 0].dtype == 'object':
                return 'pie'
        
        if n_rows > 10 and n_cols == 2:
            if data.iloc[:, 0].dtype == 'object':
                return 'bar'
        
        if n_cols >= 3 and n_rows <= 100:
            # Check for date columns
            date_cols = [col for col in data.columns 
                        if pd.api.types.is_datetime64_any_dtype(data[col])]
            if date_cols:
                return 'line'
        
        # Default fallback
        if n_rows <= 20:
            return 'table'
        else:
            return 'bar'
    
    def _create_number_display(self, data: pd.DataFrame, config: Dict) -> pn.Column:
        """Create large number/KPI display"""
        
        # Get the value
        if len(data) == 1 and len(data.columns) == 1:
            value = data.iloc[0, 0]
            label = data.columns[0]
        else:
            # Multiple values - create multiple KPIs
            return self._create_multi_kpi(data, config)
        
        # Format the value
        if isinstance(value, (int, float)):
            if abs(value) >= 1_000_000:
                display_value = f"{value/1_000_000:.2f}M"
            elif abs(value) >= 1_000:
                display_value = f"{value/1_000:.2f}K"
            else:
                display_value = f"{value:,.2f}" if isinstance(value, float) else f"{value:,}"
        else:
            display_value = str(value)
        
        # Create KPI card
        kpi_html = f"""
        <div style="text-align: center; padding: 40px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    border-radius: 15px; box-shadow: 0 10px 25px rgba(0,0,0,0.2);">
            <div style="color: rgba(255,255,255,0.8); font-size: 18px; font-weight: 500; margin-bottom: 10px;">
                {label.upper().replace('_', ' ')}
            </div>
            <div style="color: white; font-size: 64px; font-weight: bold; font-family: 'Arial Black', sans-serif;">
                {display_value}
            </div>
        </div>
        """
        
        return pn.pane.HTML(kpi_html, sizing_mode='stretch_width')
    
    def _create_multi_kpi(self, data: pd.DataFrame, config: Dict) -> pn.Row:
        """Create multiple KPI displays"""
        kpis = []
        
        for col in data.columns:
            value = data[col].iloc[0]
            
            if isinstance(value, (int, float)):
                if abs(value) >= 1_000_000:
                    display_value = f"{value/1_000_000:.2f}M"
                elif abs(value) >= 1_000:
                    display_value = f"{value/1_000:.2f}K"
                else:
                    display_value = f"{value:,.0f}" if value == int(value) else f"{value:,.2f}"
            else:
                display_value = str(value)
            
            kpi_html = f"""
            <div style="text-align: center; padding: 30px; background: linear-gradient(135deg, {self.color_palette[len(kpis) % len(self.color_palette)]} 0%, {self.color_palette[(len(kpis)+1) % len(self.color_palette)]} 100%); 
                        border-radius: 12px; margin: 5px; box-shadow: 0 8px 16px rgba(0,0,0,0.15);">
                <div style="color: rgba(255,255,255,0.9); font-size: 14px; font-weight: 600; margin-bottom: 8px;">
                    {col.upper().replace('_', ' ')}
                </div>
                <div style="color: white; font-size: 42px; font-weight: bold;">
                    {display_value}
                </div>
            </div>
            """
            kpis.append(pn.pane.HTML(kpi_html))
        
        return pn.Row(*kpis, sizing_mode='stretch_width')
    
    def _create_bar_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create bar chart with smart defaults"""
        
        x_col = config.get('x_axis') or data.columns[0]
        y_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Bar Chart')
        
        # Limit bars if too many
        if len(data) > 20:
            data = data.nlargest(20, y_col)
            title += " (Top 20)"
        
        fig = px.bar(
            data,
            x=x_col,
            y=y_col,
            title=title,
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white',
            hovermode='x unified'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_horizontal_bar(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create horizontal bar chart"""
        
        x_col = config.get('y_axis') or data.columns[1]
        y_col = config.get('x_axis') or data.columns[0]
        title = config.get('title', 'Horizontal Bar Chart')
        
        fig = px.bar(
            data,
            x=x_col,
            y=y_col,
            orientation='h',
            title=title,
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_pie_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create pie chart"""
        
        names_col = config.get('x_axis') or data.columns[0]
        values_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Distribution')
        
        # Limit slices
        if len(data) > 10:
            top_data = data.nlargest(9, values_col)
            others_sum = data.nsmallest(len(data) - 9, values_col)[values_col].sum()
            others = pd.DataFrame({
                names_col: ['Others'],
                values_col: [others_sum]
            })
            data = pd.concat([top_data, others], ignore_index=True)
        
        fig = px.pie(
            data,
            names=names_col,
            values=values_col,
            title=title,
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Value: %{value}<br>Percentage: %{percent}<extra></extra>'
        )
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white',
            showlegend=True
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_donut_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create donut chart"""
        
        names_col = config.get('x_axis') or data.columns[0]
        values_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Distribution')
        
        fig = px.pie(
            data,
            names=names_col,
            values=values_col,
            title=title,
            color_discrete_sequence=self.color_palette,
            hole=0.4
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label'
        )
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_line_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create line chart"""
        
        x_col = config.get('x_axis') or data.columns[0]
        y_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Trend')
        
        fig = px.line(
            data,
            x=x_col,
            y=y_col,
            title=title,
            markers=True,
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_traces(
            line=dict(width=3),
            marker=dict(size=8)
        )
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white',
            hovermode='x unified'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_area_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create area chart"""
        
        x_col = config.get('x_axis') or data.columns[0]
        y_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Area Chart')
        
        fig = px.area(
            data,
            x=x_col,
            y=y_col,
            title=title,
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_scatter_plot(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create scatter plot"""
        
        x_col = config.get('x_axis') or data.columns[0]
        y_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Scatter Plot')
        
        color_by = config.get('color_by')
        
        fig = px.scatter(
            data,
            x=x_col,
            y=y_col,
            color=color_by if color_by and color_by in data.columns else None,
            title=title,
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_traces(marker=dict(size=10, opacity=0.7))
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_heatmap(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create heatmap"""
        
        title = config.get('title', 'Heatmap')
        
        # For correlation heatmap
        if data.shape[0] == data.shape[1]:
            fig = px.imshow(
                data,
                title=title,
                color_continuous_scale='RdBu_r',
                aspect='auto'
            )
        else:
            fig = px.imshow(
                data,
                title=title,
                color_continuous_scale='Viridis',
                aspect='auto'
            )
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_treemap(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create treemap"""
        
        labels_col = config.get('x_axis') or data.columns[0]
        values_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Treemap')
        
        fig = px.treemap(
            data,
            path=[labels_col],
            values=values_col,
            title=title,
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_funnel_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create funnel chart"""
        
        x_col = config.get('x_axis') or data.columns[0]
        y_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Funnel Chart')
        
        fig = px.funnel(
            data,
            x=y_col,
            y=x_col,
            title=title,
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_table(self, data: pd.DataFrame, config: Dict) -> pn.widgets.Tabulator:
        """Create interactive table"""
        
        # Limit rows for display
        display_data = data.head(self.config.max_rows_display)
        
        return pn.widgets.Tabulator(
            display_data,
            pagination='remote',
            page_size=20,
            sizing_mode='stretch_width',
            theme='modern',
            show_index=False,
            buttons={'Download': '<i class="fa fa-download"></i>'}
        )