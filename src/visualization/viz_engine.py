import pandas as pd
import panel as pn
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class VisualizationEngine:
    """Smart visualization engine with enhanced auto-detection"""
    
    def __init__(self, config):
        self.config = config
        self.color_palette = config.get_color_palette()
    
    def _apply_dark_theme(self, fig):
        """Apply consistent dark theme to plotly figure"""
        fig.update_layout(
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)',
            font={'color': '#f5f5f7', 'family': 'Inter, -apple-system, sans-serif'},
            title_font={'size': 20, 'color': '#f5f5f7', 'family': 'Inter'},
            xaxis={
                'gridcolor': 'rgba(255, 255, 255, 0.1)',
                'linecolor': 'rgba(255, 255, 255, 0.2)',
                'zerolinecolor': 'rgba(255, 255, 255, 0.2)',
                'color': '#f5f5f7'
            },
            yaxis={
                'gridcolor': 'rgba(255, 255, 255, 0.1)',
                'linecolor': 'rgba(255, 255, 255, 0.2)',
                'zerolinecolor': 'rgba(255, 255, 255, 0.2)',
                'color': '#f5f5f7'
            },
            legend={
                'bgcolor': 'rgba(30, 30, 30, 0.9)',
                'bordercolor': 'rgba(255, 255, 255, 0.1)',
                'font': {'color': '#f5f5f7'}
            },
            hoverlabel={
                'bgcolor': 'rgba(30, 30, 30, 0.95)',
                'font': {'color': '#f5f5f7', 'family': 'Inter'}
            }
        )
        return fig
    
    def create_visualization(self, data: pd.DataFrame, config: Dict[str, Any], query: str):
        """Create visualization with smart fallbacks"""
        
        viz_type = config.get('visualization_type', 'auto')
        
        # Auto-detect if needed
        if viz_type == 'auto' or not viz_type:
            viz_type = self._smart_detect_viz_type(data, query, config)
            config['visualization_type'] = viz_type
            logger.info(f"Auto-detected: {viz_type}")
        
        # Normalize viz type
        viz_type = self._normalize_viz_type(viz_type)
        
        # Create visualization
        viz_creators = {
            'number': self._create_number_display,
            'kpi': self._create_number_display,
            'metric': self._create_number_display,
            'bar': self._create_bar_chart,
            'stacked_bar': self._create_stacked_bar_chart,
            'horizontal_bar': self._create_horizontal_bar,
            'line': self._create_line_chart,
            'area': self._create_area_chart,
            'pie': self._create_pie_chart,
            'donut': self._create_donut_chart,
            'scatter': self._create_scatter_plot,
            'heatmap': self._create_heatmap,
            'table': self._create_table,
            'treemap': self._create_treemap,
            'funnel': self._create_funnel_chart
        }
        
        creator = viz_creators.get(viz_type, self._create_table)
        
        try:
            viz = creator(data, config)
            logger.info(f"Created {viz_type} visualization")
            return viz
        except Exception as e:
            logger.error(f"Error creating {viz_type}: {e}", exc_info=True)
            # Fallback to table
            return self._create_table(data, config)
    
    def _normalize_viz_type(self, viz_type: str) -> str:
        """Normalize visualization type names"""
        
        if not viz_type:
            return 'table'
        
        normalized = viz_type.lower().strip().replace('-', '_').replace(' ', '_')
        
        # Map synonyms
        type_map = {
            'kpi': 'number',
            'metric': 'number',
            'single_value': 'number',
            'column': 'bar',
            'vertical_bar': 'bar',
            'barh': 'horizontal_bar',
            'time_series': 'line',
            'trend': 'line',
            'doughnut': 'donut',
            'ring': 'donut',
            'bubble': 'scatter',
            'data_table': 'table',
            'grid': 'table'
        }
        
        return type_map.get(normalized, normalized)
    
    def _smart_detect_viz_type(self, data: pd.DataFrame, query: str, config: Dict) -> str:
        """Intelligently detect the best visualization type with comprehensive analysis"""
        
        n_rows = len(data)
        n_cols = len(data.columns)
        query_lower = query.lower()
        
        # Analyze data types
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        datetime_cols = self._detect_datetime_columns(data)
        categorical_cols = [col for col in data.columns if col not in numeric_cols + datetime_cols]
        
        logger.info(f"Data profile: {n_rows} rows, {n_cols} cols | Numeric: {len(numeric_cols)}, DateTime: {len(datetime_cols)}, Categorical: {len(categorical_cols)}")
        
        # Rule 0: Explicit chart type mentions (highest priority)
        explicit_types = {
            'scatter plot': 'scatter',
            'scatter': 'scatter',
            'scatterplot': 'scatter',
            'correlation plot': 'scatter',
            'heatmap': 'heatmap',
            'heat map': 'heatmap',
            'matrix': 'heatmap',
            'pivot': 'heatmap',
            'pie chart': 'pie',
            'pie': 'pie',
            'donut chart': 'donut',
            'donut': 'donut',
            'doughnut': 'donut',
            'bar chart': 'bar',
            'bar graph': 'bar',
            'column chart': 'bar',
            'line chart': 'line',
            'line graph': 'line',
            'area chart': 'area',
            'table': 'table',
            'treemap': 'treemap',
            'kpi': 'number',
            'metric': 'number',
            'card': 'number'
        }
        
        for keyword, viz_type in explicit_types.items():
            if keyword in query_lower:
                logger.info(f"→ Explicit mention '{keyword}' → {viz_type}")
                return viz_type
        
        # Rule 1: Single value KPI displays
        if n_rows == 1 and n_cols <= 6:
            if len(numeric_cols) >= 1:
                logger.info("→ Single row with metrics → number (KPI cards)")
                return 'number'
        
        # Rule 2: Correlation / Scatter (2+ numeric columns, many rows)
        correlation_keywords = ['correlation', 'correlate', 'relationship', 'vs', 'versus', 'against', 
                                'impact', 'influence', 'affect', 'depend', 'related']
        has_correlation_hint = any(kw in query_lower for kw in correlation_keywords)
        
        if (has_correlation_hint or len(numeric_cols) >= 2) and n_rows >= 10:
            # Check if we have exactly 2 numeric columns (perfect for scatter)
            if len(numeric_cols) == 2:
                logger.info("→ Two numeric columns + correlation intent → scatter")
                return 'scatter'
            # Or if explicitly about correlation
            elif has_correlation_hint and len(numeric_cols) >= 2:
                logger.info("→ Multiple numerics + correlation keyword → scatter")
                return 'scatter'
        
        # Rule 3: Heatmap (pivot-style data or matrix)
        heatmap_keywords = ['heatmap', 'matrix', 'pivot', 'cross-tab', 'crosstab']
        has_heatmap_hint = any(kw in query_lower for kw in heatmap_keywords)
        
        if has_heatmap_hint or (len(categorical_cols) >= 2 and len(numeric_cols) >= 1):
            # Check if data looks like a pivot (multiple combos of two categories)
            if len(categorical_cols) >= 2 and n_rows >= 6:
                logger.info("→ Two categoricals + numeric (pivot structure) → heatmap")
                return 'heatmap'
        
        # Rule 4: Time series / Trends
        time_keywords = ['trend', 'over time', 'timeline', 'history', 'evolution', 'time series',
                        'monthly', 'weekly', 'daily', 'yearly', 'quarterly', 'per day', 'per month',
                        'per year', 'growth', 'change over']
        has_time_hint = any(kw in query_lower for kw in time_keywords)
        
        if (has_time_hint or datetime_cols) and numeric_cols:
            if 'cumulative' in query_lower or 'stacked' in query_lower or 'filled' in query_lower:
                logger.info("→ Time series + cumulative → area")
                return 'area'
            logger.info("→ Time series detected → line")
            return 'line'
        
        # Rule 2: Table for detailed data or many columns
        table_keywords = ['table', 'list', 'show all', 'details', 'records']
        if any(kw in query_lower for kw in table_keywords) or n_cols > 6:
            logger.info("→ Detailed data → table")
            return 'table'
        
        # Rule 3: Time series
        time_keywords = ['trend', 'over time', 'timeline', 'history', 'monthly', 'weekly', 'daily', 'yearly']
        has_time_hint = any(kw in query_lower for kw in time_keywords)
        
        if (datetime_cols or has_time_hint) and numeric_cols:
            if 'cumulative' in query_lower or 'stacked' in query_lower:
                logger.info("→ Time series (cumulative) → area")
                return 'area'
            logger.info("→ Time series → line")
            return 'line'
        
        # Rule 4: Distribution/Composition
        dist_keywords = ['distribution', 'breakdown', 'composition', 'share', 'percentage', 'proportion', 'split']
        if any(kw in query_lower for kw in dist_keywords):
            if n_rows <= 8 and categorical_cols and numeric_cols:
                if 'donut' in query_lower or 'doughnut' in query_lower:
                    logger.info("→ Distribution (donut) → donut")
                    return 'donut'
                logger.info("→ Distribution (small) → pie")
                return 'pie'
            elif n_rows <= 20:
                logger.info("→ Distribution (medium) → bar")
                return 'bar'
        
        # Rule 4: Comparisons and rankings (enhanced for grouped data)
        rank_keywords = ['top', 'bottom', 'best', 'worst', 'highest', 'lowest', 'rank', 'compare']
        dist_keywords = ['distribution', 'breakdown', 'across', 'by location', 'by region', 'by store']
        
        if any(kw in query_lower for kw in rank_keywords + dist_keywords):
            # Check if we have grouped data (store_location, product_category, count)
            if n_cols >= 3 and categorical_cols and numeric_cols:
                logger.info("→ Multi-dimensional data with comparison → bar (grouped)")
                return 'bar'
            elif n_rows <= 10:
                logger.info("→ Ranking/comparison (few items) → horizontal_bar")
                return 'horizontal_bar'
            elif n_rows <= 30:
                logger.info("→ Ranking/comparison → bar")
                return 'bar'
            else:
                logger.info("→ Too many items for ranking → table")
                return 'table'
        
        # Rule 6: Correlation
        corr_keywords = ['correlation', 'relationship', 'vs', 'versus', 'against']
        if any(kw in query_lower for kw in corr_keywords) and len(numeric_cols) >= 2:
            logger.info("→ Correlation → scatter")
            return 'scatter'
        
        # Rule 7: Default based on data shape (improved)
        if categorical_cols and numeric_cols:
            if n_rows <= 6:
                logger.info("→ Few categories → pie")
                return 'pie'
            elif n_rows <= 15:
                logger.info("→ Medium categories → bar")
                return 'bar'
            elif n_rows <= 50:
                logger.info("→ Many categories → horizontal_bar")
                return 'horizontal_bar'
            else:
                # For very large datasets, still try bar chart first
                logger.info("→ Large dataset → bar (will auto-limit)")
                return 'bar'
        
        # Rule 8: Multiple numerics → scatter
        if len(numeric_cols) >= 2 and n_rows > 5:
            logger.info("→ Multiple numerics → scatter")
            return 'scatter'
        
        # Rule 9: When in doubt, prefer bar charts over tables for numeric data
        if numeric_cols and n_rows > 1:
            logger.info("→ Default with numeric data → bar")
            return 'bar'
        
        # Final fallback
        logger.info("→ Final fallback → table")
        return 'table'
    
    def _detect_datetime_columns(self, data: pd.DataFrame) -> List[str]:
        """Detect datetime-like columns"""
        
        datetime_cols = []
        
        for col in data.columns:
            # Already datetime
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                datetime_cols.append(col)
                continue
            
            # Check column name
            col_lower = col.lower()
            if any(kw in col_lower for kw in ['date', 'time', 'timestamp', 'day', 'month', 'year']):
                # Try to parse
                try:
                    parsed = pd.to_datetime(data[col].head(20), errors='coerce')
                    if parsed.notna().sum() / len(parsed) > 0.7:
                        datetime_cols.append(col)
                except:
                    pass
        
        return datetime_cols
    
    def _create_number_display(self, data: pd.DataFrame, config: Dict) -> pn.pane.HTML:
        """Create modern KPI display"""
        
        if len(data) == 1 and len(data.columns) == 1:
            value = data.iloc[0, 0]
            label = data.columns[0]
            
            display_value = self._format_number(value)
            
            html = f"""
            <div style="
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 50px;
                border-radius: 15px;
                text-align: center;
                box-shadow: 0 10px 30px rgba(102, 126, 234, 0.4);
            ">
                <div style="
                    color: rgba(255,255,255,0.9);
                    font-size: 18px;
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    margin-bottom: 15px;
                ">{label.replace('_', ' ')}</div>
                <div style="
                    color: white;
                    font-size: 72px;
                    font-weight: 900;
                    font-family: 'Arial Black', sans-serif;
                    text-shadow: 0 2px 10px rgba(0,0,0,0.2);
                ">{display_value}</div>
            </div>
            """
            
            return pn.pane.HTML(html, sizing_mode='stretch_width')
        
        # Multiple KPIs
        return self._create_multi_kpi(data, config)
    
    def _create_multi_kpi(self, data: pd.DataFrame, config: Dict) -> pn.Row:
        """Create multiple KPI cards"""
        
        kpis = []
        colors = [
            ['#667eea', '#764ba2'],
            ['#f093fb', '#f5576c'],
            ['#4facfe', '#00f2fe'],
            ['#43e97b', '#38f9d7'],
            ['#fa709a', '#fee140']
        ]
        
        for idx, col in enumerate(data.columns):
            value = data[col].iloc[0]
            display_value = self._format_number(value)
            
            color_pair = colors[idx % len(colors)]
            
            html = f"""
            <div style="
                background: linear-gradient(135deg, {color_pair[0]} 0%, {color_pair[1]} 100%);
                padding: 30px;
                border-radius: 12px;
                text-align: center;
                box-shadow: 0 6px 20px rgba(0,0,0,0.15);
                min-width: 200px;
            ">
                <div style="
                    color: rgba(255,255,255,0.95);
                    font-size: 14px;
                    font-weight: 600;
                    text-transform: uppercase;
                    margin-bottom: 10px;
                ">{col.replace('_', ' ')}</div>
                <div style="
                    color: white;
                    font-size: 48px;
                    font-weight: 900;
                ">{display_value}</div>
            </div>
            """
            
            kpis.append(pn.pane.HTML(html))
        
        return pn.Row(*kpis, sizing_mode='stretch_width')
    
    def _format_number(self, value) -> str:
        """Format number for display"""
        
        if not isinstance(value, (int, float)):
            return str(value)
        
        if abs(value) >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        elif abs(value) >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        elif abs(value) >= 1_000:
            return f"{value / 1_000:.2f}K"
        elif isinstance(value, float):
            return f"{value:.2f}"
        else:
            return f"{value:,}"
    
    def _create_bar_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create modern bar chart with support for grouped data"""
        
        # Determine columns
        x_col = config.get('x_axis', data.columns[0])
        y_col = config.get('y_axis', data.columns[-1])  # Last column often has values
        color_col = config.get('color_by', None)
        
        title = config.get('title', 'Bar Chart')
        
        # Check if we have 3+ columns and need grouped bars
        if len(data.columns) >= 3 and color_col is None:
            # Auto-detect grouping column (middle column)
            potential_group_col = data.columns[1] if len(data.columns) > 2 else None
            if potential_group_col and potential_group_col not in [x_col, y_col]:
                color_col = potential_group_col
                logger.info(f"🎨 Detected grouped bar chart: x={x_col}, y={y_col}, color={color_col}")
        
        # Find numeric column for y-axis if not specified
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        if y_col not in numeric_cols and numeric_cols:
            y_col = numeric_cols[0]
        
        # Limit to top items if too many (but keep all groups)
        unique_x = data[x_col].nunique()
        if unique_x > 20:
            # Get top x categories by sum of y values
            top_categories = data.groupby(x_col)[y_col].sum().nlargest(20).index
            data = data[data[x_col].isin(top_categories)]
            title += " (Top 20)"
        
        fig = px.bar(
            data,
            x=x_col,
            y=y_col,
            color=color_col,
            title=title,
            color_discrete_sequence=px.colors.qualitative.Set3,
            barmode='group'  # Grouped bars side by side
        )
        
        fig.update_traces(
            marker_line_color='rgb(8,48,107)',
            marker_line_width=1.5,
            opacity=0.9
        )
        
        fig.update_layout(
            height=max(500, min(800, len(data) * 15)),
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title(),
            legend_title=color_col.replace('_', ' ').title() if color_col else None,
            xaxis={'categoryorder': 'total descending'},
            hovermode='x unified'
        )
        
        # Apply dark theme
        fig = self._apply_dark_theme(fig)
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_stacked_bar_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create stacked bar chart for multi-category data"""
        
        x_col = config.get('x_axis', data.columns[0])
        y_col = config.get('y_axis', data.columns[-1])
        color_col = config.get('color_by', None)
        
        # Auto-detect grouping column
        if len(data.columns) >= 3 and color_col is None:
            color_col = data.columns[1]
        
        title = config.get('title', 'Stacked Bar Chart')
        
        # Find numeric column
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        if y_col not in numeric_cols and numeric_cols:
            y_col = numeric_cols[0]
        
        fig = px.bar(
            data,
            x=x_col,
            y=y_col,
            color=color_col,
            title=title,
            color_discrete_sequence=px.colors.qualitative.Set3,
            barmode='stack'  # Stacked bars
        )
        
        fig.update_traces(
            marker_line_color='white',
            marker_line_width=1
        )
        
        fig.update_layout(
            height=600,
            template='plotly_white',
            title_font_size=20,
            title_font_color='#2d3748',
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title(),
            legend_title=color_col.replace('_', ' ').title() if color_col else None
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_horizontal_bar(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create horizontal bar chart"""
        
        x_col = config.get('y_axis', data.columns[1] if len(data.columns) > 1 else data.columns[0])
        y_col = config.get('x_axis', data.columns[0])
        title = config.get('title', 'Comparison')
        
        # Sort by value
        data = data.sort_values(by=x_col, ascending=True)
        
        # Limit if too many
        if len(data) > 15:
            data = data.tail(15)
            title += " (Top 15)"
        
        fig = px.bar(
            data,
            x=x_col,
            y=y_col,
            orientation='h',
            title=title,
            color_discrete_sequence=['#f093fb']
        )
        
        fig.update_traces(marker_line_color='rgb(8,48,107)', marker_line_width=1.5, opacity=0.9)
        
        fig.update_layout(
            height=max(400, len(data) * 30),
            template='plotly_white',
            title_font_size=20,
            title_font_color='#2d3748',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_line_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create line chart with support for multiple series and trends"""
        
        x_col = config.get('x_axis', data.columns[0])
        y_col = config.get('y_axis')
        color_col = config.get('color_by', None)
        title = config.get('title', 'Trend')
        
        # Find numeric and datetime columns
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        datetime_cols = self._detect_datetime_columns(data)
        
        # Auto-detect x-axis (prefer datetime)
        if datetime_cols and x_col not in datetime_cols:
            x_col = datetime_cols[0]
        
        # Auto-detect y-axis (prefer numeric)
        if not y_col or y_col not in data.columns:
            remaining_numeric = [col for col in numeric_cols if col != x_col]
            y_col = remaining_numeric[0] if remaining_numeric else numeric_cols[0] if numeric_cols else data.columns[-1]
        
        # Auto-detect color/grouping column for multi-line
        if not color_col and len(data.columns) >= 3:
            categorical_cols = [col for col in data.columns if col not in numeric_cols + datetime_cols]
            if categorical_cols:
                color_col = categorical_cols[0]
                logger.info(f"📊 Multi-line chart detected: grouping by {color_col}")
        
        # Prepare data
        plot_data = data.copy()
        
        # Try to convert x to datetime if it looks like dates
        if x_col in plot_data.columns:
            try:
                converted = pd.to_datetime(plot_data[x_col], errors='coerce', infer_datetime_format=True)
                if converted.notna().mean() >= 0.6:
                    plot_data[x_col] = converted
                    logger.info(f"✅ Converted {x_col} to datetime")
            except Exception as e:
                logger.warning(f"Could not convert {x_col} to datetime: {e}")
        
        # Sort by x column
        plot_data = plot_data.dropna(subset=[x_col])
        if pd.api.types.is_datetime64_any_dtype(plot_data[x_col]) or pd.api.types.is_numeric_dtype(plot_data[x_col]):
            plot_data = plot_data.sort_values(by=x_col)
        
        logger.info(f"📈 Line chart: x={x_col}, y={y_col}, color={color_col}")
        
        # Create line chart
        fig = px.line(
            plot_data,
            x=x_col,
            y=y_col,
            color=color_col,
            title=title,
            markers=True,
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        
        fig.update_traces(
            line=dict(width=3),
            marker=dict(size=8, line=dict(width=2, color='white'))
        )
        
        fig.update_layout(
            height=550,
            hovermode='x unified',
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title(),
            legend_title=color_col.replace('_', ' ').title() if color_col else None
        )
        
        # Apply dark theme
        fig = self._apply_dark_theme(fig)
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_area_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create area chart"""
        
        x_col = config.get('x_axis', data.columns[0])
        y_col = config.get('y_axis', data.columns[1] if len(data.columns) > 1 else data.columns[0])
        title = config.get('title', 'Trend')
        
        plot_data = data.copy()
        try:
            plot_data[x_col] = pd.to_datetime(plot_data[x_col], errors='coerce')
            plot_data = plot_data.dropna(subset=[x_col]).sort_values(x_col)
        except:
            pass
        
        fig = px.area(
            plot_data,
            x=x_col,
            y=y_col,
            title=title,
            color_discrete_sequence=['#43e97b']
        )
        
        fig.update_traces(line=dict(width=3))
        
        fig.update_layout(
            height=500,
            template='plotly_white',
            hovermode='x unified',
            title_font_size=20,
            title_font_color='#2d3748'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_pie_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create modern pie chart"""
        
        # Special case: Single row with multiple numeric columns (e.g., active_count, churned_count, inactive_count)
        if len(data) == 1 and len(data.columns) > 1:
            # Check if all columns are numeric
            numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
            if len(numeric_cols) == len(data.columns):
                # Transform: columns become categories, values become the data
                plot_data = pd.DataFrame({
                    'category': [col.replace('_', ' ').title() for col in data.columns],
                    'value': data.iloc[0].values
                })
                names_col = 'category'
                values_col = 'value'
            else:
                names_col = config.get('x_axis', data.columns[0])
                values_col = config.get('y_axis', data.columns[1] if len(data.columns) > 1 else data.columns[0])
                plot_data = data
        else:
            names_col = config.get('x_axis', data.columns[0])
            values_col = config.get('y_axis', data.columns[1] if len(data.columns) > 1 else data.columns[0])
            plot_data = data
        
        title = config.get('title', 'Distribution')
        
        # Limit to top items if too many
        if len(plot_data) > 10:
            top_data = plot_data.nlargest(9, values_col)
            others_sum = plot_data.nsmallest(len(plot_data) - 9, values_col)[values_col].sum()
            others = pd.DataFrame({names_col: ['Others'], values_col: [others_sum]})
            plot_data = pd.concat([top_data, others], ignore_index=True)
        
        fig = px.pie(
            plot_data,
            names=names_col,
            values=values_col,
            title=title,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            textfont_size=14,
            marker=dict(line=dict(color='white', width=2))
        )
        
        fig.update_layout(
            height=500,
            title_font_size=20,
            title_font_color='#2d3748',
            showlegend=True
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_donut_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create donut chart"""
        
        # Special case: Single row with multiple numeric columns
        if len(data) == 1 and len(data.columns) > 1:
            numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
            if len(numeric_cols) == len(data.columns):
                # Transform: columns become categories
                plot_data = pd.DataFrame({
                    'category': [col.replace('_', ' ').title() for col in data.columns],
                    'value': data.iloc[0].values
                })
                names_col = 'category'
                values_col = 'value'
            else:
                names_col = config.get('x_axis', data.columns[0])
                values_col = config.get('y_axis', data.columns[1] if len(data.columns) > 1 else data.columns[0])
                plot_data = data
        else:
            names_col = config.get('x_axis', data.columns[0])
            values_col = config.get('y_axis', data.columns[1] if len(data.columns) > 1 else data.columns[0])
            plot_data = data
        
        title = config.get('title', 'Distribution')
        
        fig = px.pie(
            plot_data,
            names=names_col,
            values=values_col,
            title=title,
            hole=0.5,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            textfont_size=14,
            marker=dict(line=dict(color='white', width=3))
        )
        
        fig.update_layout(
            height=500,
            title_font_size=20,
            title_font_color='#2d3748',
            showlegend=True,
            annotations=[dict(text='Total', x=0.5, y=0.5, font_size=20, showarrow=False)]
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_scatter_plot(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create scatter plot"""
        
        x_col = config.get('x_axis', data.columns[0])
        y_col = config.get('y_axis', data.columns[1] if len(data.columns) > 1 else data.columns[0])
        title = config.get('title', 'Scatter Plot')
        
        fig = px.scatter(
            data,
            x=x_col,
            y=y_col,
            title=title,
            color_discrete_sequence=['#667eea'],
            trendline='ols'
        )
        
        fig.update_traces(marker=dict(size=12, opacity=0.7, line=dict(width=1, color='white')))
        
        fig.update_layout(
            height=500,
            template='plotly_white',
            title_font_size=20,
            title_font_color='#2d3748'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_heatmap(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create heatmap - handles both matrix data and pivot-style data"""
        
        title = config.get('title', 'Heatmap')
        
        # Check if data needs to be pivoted (has categorical columns)
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = [col for col in data.columns if col not in numeric_cols]
        
        if len(categorical_cols) >= 2 and len(numeric_cols) >= 1:
            # Pivot data: cat1 × cat2 → values
            cat1 = categorical_cols[0]
            cat2 = categorical_cols[1]
            value_col = numeric_cols[0]
            
            logger.info(f"📊 Pivoting for heatmap: {cat1} × {cat2} → {value_col}")
            
            try:
                # Create pivot table
                pivot_data = data.pivot_table(
                    values=value_col,
                    index=cat1,
                    columns=cat2,
                    aggfunc='sum',
                    fill_value=0
                )
                
                fig = px.imshow(
                    pivot_data,
                    title=title,
                    color_continuous_scale='RdYlBu_r',
                    aspect='auto',
                    labels=dict(x=cat2.replace('_', ' ').title(),
                               y=cat1.replace('_', ' ').title(),
                               color=value_col.replace('_', ' ').title())
                )
                
                fig.update_xaxes(side='bottom')
                
            except Exception as e:
                logger.warning(f"Pivot failed: {e}, using direct matrix")
                # Fall back to direct matrix
                matrix_data = data.select_dtypes(include=['number'])
                fig = px.imshow(
                    matrix_data,
                    title=title,
                    color_continuous_scale='RdYlBu_r',
                    aspect='auto'
                )
        else:
            # Already in matrix format or all numeric
            matrix_data = data.select_dtypes(include=['number'])
            
            if matrix_data.empty:
                logger.warning("No numeric data for heatmap, using all data")
                matrix_data = data
            
            fig = px.imshow(
                matrix_data,
                title=title,
                color_continuous_scale='Viridis',
                aspect='auto',
                text_auto=True  # Show values in cells
            )
        
        fig.update_layout(
            height=max(400, min(800, len(data) * 30)),
            title_font_size=20,
            title_font_color='#2d3748',
            template='plotly_white'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_treemap(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create treemap"""
        
        labels_col = config.get('x_axis', data.columns[0])
        values_col = config.get('y_axis', data.columns[1] if len(data.columns) > 1 else data.columns[0])
        title = config.get('title', 'Treemap')
        
        fig = px.treemap(
            data,
            path=[labels_col],
            values=values_col,
            title=title,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        
        fig.update_layout(
            height=500,
            title_font_size=20,
            title_font_color='#2d3748'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_funnel_chart(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create funnel chart"""
        
        x_col = config.get('x_axis', data.columns[0])
        y_col = config.get('y_axis', data.columns[1] if len(data.columns) > 1 else data.columns[0])
        title = config.get('title', 'Funnel')
        
        fig = px.funnel(
            data,
            x=y_col,
            y=x_col,
            title=title,
            color_discrete_sequence=['#667eea']
        )
        
        fig.update_layout(
            height=500,
            title_font_size=20,
            title_font_color='#2d3748'
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width')
    
    def _create_table(self, data: pd.DataFrame, config: Dict) -> pn.widgets.Tabulator:
        """Create modern interactive table"""
        
        return pn.widgets.Tabulator(
            data,
            pagination='local',
            page_size=20,
            sizing_mode='stretch_width',
            theme='modern',
            show_index=False,
            layout='fit_data_table',
            header_filters=True
        )