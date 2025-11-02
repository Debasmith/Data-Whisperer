"""
Intelligent Visualization Engine with Smart Chart Selection
"""

import pandas as pd
import panel as pn
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, Optional, List
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class VisualizationEngine:
    """Smart visualization engine that creates appropriate charts"""
    
    def __init__(self, config):
        self.config = config
        self.color_palette = config.get_color_palette()

    def _is_datetime_like_column(self, series: pd.Series, column_name: str) -> bool:
        """Infer datetime columns from content and naming heuristics."""

        if pd.api.types.is_datetime64_any_dtype(series):
            return True

        if pd.api.types.is_bool_dtype(series):
            return False

        sample = series.dropna().head(50)
        if sample.empty:
            return False

        parse_ratio = 0.0
        try:
            parsed = pd.to_datetime(sample, errors='coerce', infer_datetime_format=True)
            parse_ratio = parsed.notna().mean()
        except Exception:
            parse_ratio = 0.0

        if parse_ratio >= 0.8:
            return True

        name_hint = column_name.lower()
        if any(token in name_hint for token in ['date', 'time', 'timestamp', 'day', 'month', 'year', 'week']):
            return parse_ratio >= 0.5

        return False

    def _is_numeric_like_series(self, series: pd.Series) -> bool:
        """Detect numeric columns even when stored as strings."""

        if pd.api.types.is_numeric_dtype(series):
            return True

        if pd.api.types.is_bool_dtype(series):
            return False

        sample = series.dropna().head(50)
        if sample.empty:
            return False

        numeric_sample = pd.to_numeric(sample, errors='coerce')
        valid_ratio = numeric_sample.notna().mean()

        return valid_ratio >= 0.8

    def _looks_like_percentage_distribution(
        self,
        data: pd.DataFrame,
        numeric_cols: List[str]
    ) -> bool:
        """Identify datasets that represent percentage splits."""

        for col in numeric_cols:
            series = pd.to_numeric(data[col], errors='coerce').dropna()

            if series.empty:
                continue

            col_name = col.lower()
            if any(token in col_name for token in ['percent', 'percentage', 'share', 'ratio', 'rate']):
                return True

            if series.between(0, 1).all():
                total = series.sum()
                if 0.92 <= total <= 1.08:
                    return True

            if series.between(0, 100).all():
                total = series.sum()
                if 92 <= total <= 108:
                    return True

        return False
    
    def _normalize_viz_type(self, viz_type: Optional[str], viz_config: Dict[str, Any]) -> str:
        """Normalize visualization type hints coming from configuration."""

        candidates = []
        if viz_type:
            candidates.append(viz_type)

        for key in (
            'chart_type',
            'chart',
            'type',
            'preferred_chart',
            'recommended_chart',
            'viz_type'
        ):
            value = viz_config.get(key)
            if value:
                candidates.append(value)

        for candidate in candidates:
            canonical = self._normalize_single_viz_type(candidate)
            if canonical:
                viz_config['visualization_type'] = canonical
                return canonical

        return 'auto'

    def _normalize_single_viz_type(self, value: Optional[str]) -> Optional[str]:
        """Map various naming conventions to the engine's canonical chart types."""

        if not value:
            return None

        normalized = str(value).strip().lower()
        normalized = normalized.replace('-', ' ').replace('_', ' ')
        normalized = ' '.join(normalized.split())

        synonym_map = {
            'number': {
                'number', 'metric', 'metric card', 'kpi', 'kpi card', 'indicator',
                'single value', 'value', 'stat', 'statistic'
            },
            'bar': {
                'bar', 'bar chart', 'column', 'column chart', 'vertical bar',
                'grouped bar', 'stacked bar', 'histogram'
            },
            'horizontal_bar': {
                'horizontal bar', 'horizontal bar chart', 'barh', 'bar h'
            },
            'line': {
                'line', 'line chart', 'line graph', 'time series'
            },
            'area': {
                'area', 'area chart', 'stacked area', 'cumulative area'
            },
            'pie': {
                'pie', 'pie chart'
            },
            'donut': {
                'donut', 'donut chart', 'doughnut', 'doughnut chart', 'ring chart'
            },
            'scatter': {
                'scatter', 'scatter plot', 'bubble', 'bubble chart'
            },
            'heatmap': {
                'heatmap', 'heat map', 'matrix', 'correlation heatmap'
            },
            'table': {
                'table', 'table view', 'data table', 'tabular', 'grid'
            },
            'treemap': {
                'treemap', 'tree map'
            },
            'funnel': {
                'funnel', 'funnel chart', 'conversion funnel'
            }
        }

        for canonical, synonyms in synonym_map.items():
            if normalized == canonical or normalized in synonyms:
                return canonical

        return None

    def create_visualization(
        self,
        data: pd.DataFrame,
        viz_config: Dict[str, Any],
        query: str
    ):
        """Create visualization based on config and data characteristics"""
        
        viz_type = viz_config.get('visualization_type', 'auto')
        viz_type = self._normalize_viz_type(viz_type, viz_config)

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
    
    def _analyze_data_profile(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze dataframe to understand column data types and uniqueness."""

        profile: Dict[str, Any] = {
            'numeric': [],
            'categorical': [],
            'datetime': [],
            'boolean': [],
            'unique_counts': {}
        }

        for column in data.columns:
            series = data[column]
            unique_count = series.nunique(dropna=True)
            profile['unique_counts'][column] = unique_count

            if self._is_datetime_like_column(series, column):
                profile['datetime'].append(column)
                continue

            if pd.api.types.is_bool_dtype(series):
                profile['boolean'].append(column)
                profile['categorical'].append(column)
                continue

            if self._is_numeric_like_series(series):
                profile['numeric'].append(column)
                if unique_count <= 8:
                    profile['categorical'].append(column)
                continue

            profile['categorical'].append(column)

        return profile

    def _auto_detect_viz_type(
        self,
        data: pd.DataFrame,
        query: str,
        viz_config: Dict[str, Any]
    ) -> str:
        """Intelligently detect the best visualization type."""

        query_lower = query.lower()
        n_rows = len(data)
        n_cols = len(data.columns)

        profile = self._analyze_data_profile(data)
        numeric_cols = list(profile['numeric'])
        categorical_cols = list(profile['categorical'])
        datetime_cols = list(profile['datetime'])

        percentage_distribution = self._looks_like_percentage_distribution(data, numeric_cols)

        datetime_hint_columns = [
            col for col in data.columns
            if any(token in col.lower() for token in ['date', 'time', 'timestamp', 'day', 'month', 'year', 'week'])
        ]

        for col in datetime_hint_columns:
            if col not in datetime_cols and self._is_datetime_like_column(data[col], col):
                datetime_cols.append(col)

        orientation_hint = str(viz_config.get('orientation', '')).lower()
        if orientation_hint in {'horizontal', 'h'} and categorical_cols:
            return 'horizontal_bar'
        if orientation_hint in {'vertical', 'v'} and categorical_cols:
            return 'bar'

        # Single value queries (count, sum, average, etc.)
        if n_rows == 1 and n_cols == 1:
            return 'number'
        if n_rows == 1 and n_cols <= 3:
            return 'number'

        # Table-specific hints
        if any(word in query_lower for word in ['table', 'raw data', 'records', 'detailed list', 'tabular', 'export']):
            return 'table'

        # Funnel / pipeline style questions
        if any(word in query_lower for word in ['funnel', 'conversion', 'pipeline', 'stage']):
            if categorical_cols and numeric_cols:
                return 'funnel'

        # Treemap / hierarchy
        if any(word in query_lower for word in ['treemap', 'hierarchy', 'portfolio', 'segments']):
            if categorical_cols and numeric_cols:
                return 'treemap'

        # Heatmap / correlation matrix
        if any(word in query_lower for word in ['heatmap', 'heat map', 'correlation matrix']):
            if len(numeric_cols) >= 2:
                return 'heatmap'

        # Time series
        time_keywords = [
            'trend', 'over time', 'timeline', 'history', 'evolution', 'time series',
            'monthly', 'weekly', 'daily', 'yearly', 'per day', 'per month'
        ]
        has_time_hint = any(word in query_lower for word in time_keywords)
        if (has_time_hint or datetime_cols) and numeric_cols:
            if any(word in query_lower for word in ['cumulative', 'stacked', 'area', 'filled']):
                return 'area'
            return 'line'

        # Composition and percentage focus
        composition_keywords = [
            'distribution', 'breakdown', 'composition', 'share', 'percentage',
            'proportion', 'contribution', 'split', 'share of', 'mix'
        ]
        if (
            (any(word in query_lower for word in composition_keywords) or percentage_distribution)
            and categorical_cols
            and numeric_cols
            and n_rows <= 15
        ):
            if any(word in query_lower for word in ['donut', 'doughnut', 'ring']):
                return 'donut'
            if percentage_distribution or any(word in query_lower for word in ['percent', 'percentage', 'share']):
                return 'donut'
            return 'pie'

        # Ranking / comparison
        comparison_keywords = ['compare', 'comparison', 'versus', 'vs', 'difference', 'gap', 'benchmark', 'higher', 'lower']
        ranking_keywords = ['top', 'bottom', 'highest', 'lowest', 'best', 'worst', 'rank', 'ranking', 'leaders', 'laggards']
        if any(word in query_lower for word in comparison_keywords + ranking_keywords):
            if categorical_cols and numeric_cols:
                if 'horizontal' in query_lower or n_rows <= 8:
                    return 'horizontal_bar'
                return 'bar'

        # Correlation / relationship
        correlation_keywords = ['correlation', 'relationship', 'impact', 'influence', 'association']
        if any(word in query_lower for word in correlation_keywords) and len(numeric_cols) >= 2:
            if n_rows >= 10:
                return 'scatter'
            return 'heatmap'

        # When two numeric series are provided without clear categories, scatter is useful
        if len(numeric_cols) >= 2 and not categorical_cols:
            if datetime_cols:
                return 'line'
            return 'scatter'

        # Prefer donut/pie for small categorical splits even without explicit keywords
        if categorical_cols and numeric_cols:
            if n_rows <= 4:
                return 'pie'
            if n_rows <= 8 and (percentage_distribution or any(word in query_lower for word in ['share', 'ratio', 'portion'])):
                return 'donut'
            if n_rows <= 8:
                return 'horizontal_bar'
            if n_rows <= 12:
                if 'horizontal' in query_lower or n_rows >= 10:
                    return 'horizontal_bar'
                return 'bar'
            if n_rows <= 25:
                return 'bar'

        # Dataset with a single meaningful metric column
        if len(numeric_cols) == 1:
            if n_rows == 1:
                return 'number'
            if n_rows <= 8:
                return 'horizontal_bar'
            if n_rows <= 25:
                return 'bar'
            return 'table'

        # If multiple numerics and manageable size, favor scatter or heatmap
        if len(numeric_cols) >= 2:
            if n_rows <= 80:
                return 'scatter'
            return 'heatmap'

        # Fallback to table when unsure
        return 'table'

    def _create_number_display(self, data: pd.DataFrame, config: Dict) -> pn.Column:
        """Create large number/KPI display."""

        if len(data) == 1 and len(data.columns) == 1:
            value = data.iloc[0, 0]
            label = data.columns[0]
        else:
            return self._create_multi_kpi(data, config)

        if isinstance(value, (int, float)):
            if abs(value) >= 1_000_000:
                display_value = f"{value / 1_000_000:.2f}M"
            elif abs(value) >= 1_000:
                display_value = f"{value / 1_000:.2f}K"
            else:
                display_value = f"{value:,.2f}" if isinstance(value, float) else f"{value:,}"
        else:
            display_value = str(value)

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
        """Create multiple KPI displays."""

        kpis = []
        for col in data.columns:
            value = data[col].iloc[0]

            if isinstance(value, (int, float)):
                if abs(value) >= 1_000_000:
                    display_value = f"{value / 1_000_000:.2f}M"
                elif abs(value) >= 1_000:
                    display_value = f"{value / 1_000:.2f}K"
                else:
                    display_value = f"{value:,.0f}" if value == int(value) else f"{value:,.2f}"
            else:
                display_value = str(value)

            kpi_html = f"""
            <div style="text-align: center; padding: 30px; background: linear-gradient(135deg, {self.color_palette[len(kpis) % len(self.color_palette)]} 0%, {self.color_palette[(len(kpis) + 1) % len(self.color_palette)]} 100%);
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
        """Create bar chart with smart defaults."""

        x_col = config.get('x_axis') or data.columns[0]
        y_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Bar Chart')

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
        """Create horizontal bar chart."""

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
        """Create pie chart."""

        names_col = config.get('x_axis') or data.columns[0]
        values_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Distribution')

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
        """Create donut chart."""

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
        """Create line chart."""

        x_col = config.get('x_axis') or data.columns[0]
        y_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Trend')

        color_by = config.get('color_by')
        plot_data = data.copy()

        if x_col in plot_data.columns:
            try:
                converted = pd.to_datetime(plot_data[x_col], errors='coerce', infer_datetime_format=True)
                if converted.notna().mean() >= 0.6:
                    plot_data[x_col] = converted
            except Exception:
                converted = None

            plot_data = plot_data.dropna(subset=[x_col])
            if pd.api.types.is_datetime64_any_dtype(plot_data[x_col]):
                plot_data = plot_data.sort_values(by=x_col)

        fig = px.line(
            plot_data,
            x=x_col,
            y=y_col,
            title=title,
            markers=True,
            color=color_by if color_by and color_by in plot_data.columns else None,
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
        """Create area chart."""

        x_col = config.get('x_axis') or data.columns[0]
        y_col = config.get('y_axis') or data.columns[1]
        title = config.get('title', 'Area Chart')

        color_by = config.get('color_by')
        plot_data = data.copy()

        if x_col in plot_data.columns:
            try:
                converted = pd.to_datetime(plot_data[x_col], errors='coerce', infer_datetime_format=True)
                if converted.notna().mean() >= 0.6:
                    plot_data[x_col] = converted
            except Exception:
                converted = None

            plot_data = plot_data.dropna(subset=[x_col])
            if pd.api.types.is_datetime64_any_dtype(plot_data[x_col]):
                plot_data = plot_data.sort_values(by=x_col)

        fig = px.area(
            plot_data,
            x=x_col,
            y=y_col,
            title=title,
            color=color_by if color_by and color_by in plot_data.columns else None,
            color_discrete_sequence=self.color_palette
        )

        fig.update_layout(
            height=self.config.chart_height,
            template='plotly_white'
        )

        return pn.pane.Plotly(fig, sizing_mode='stretch_width')

    def _create_scatter_plot(self, data: pd.DataFrame, config: Dict) -> pn.pane.Plotly:
        """Create scatter plot."""

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
        """Create heatmap."""

        title = config.get('title', 'Heatmap')

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
        """Create treemap."""

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
        """Create funnel chart."""

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
        """Create interactive table."""

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