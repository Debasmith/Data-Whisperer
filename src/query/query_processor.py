"""Enhanced Query Processing Module with smarter LLM integration and visualization."""

import io
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class QueryProcessor:
    """Process natural language queries into SQL and visualizations with enhanced intelligence."""

    def __init__(self, config):
        self.config = config
        self.spark: Optional[SparkSession] = None
        self.table_name: Optional[str] = None
        self.schema_details: List[str] = []
        self.sample_data: str = ""
        self.row_count: int = 0

        self.llm = ChatOpenAI(
            model=config.llm_model,
            openai_api_base=config.llm_base_url,
            openai_api_key=config.llm_api_key,
            temperature=config.llm_temperature,
        )
        self.parser = StrOutputParser()
        self._init_chains()
        logger.info("Enhanced query processor initialized")

    def load_dataset(self, file_bytes: bytes, filename: str) -> Tuple[bool, str]:
        """Load the uploaded dataset into Spark and prepare metadata."""
        try:
            extension = Path(filename).suffix.lower()
            if extension not in self.config.supported_file_types:
                return False, f"Unsupported file type: {extension}"

            pandas_df = self._read_with_pandas(file_bytes, extension)
            if pandas_df.empty:
                return False, "Uploaded file contains no rows"

            spark = self._get_spark_session()
            spark_df = spark.createDataFrame(pandas_df)

            table_name = self._sanitize_table_name(Path(filename).stem)
            spark_df.createOrReplaceTempView(table_name)

            self.spark = spark
            self.table_name = table_name
            self.row_count = spark_df.count()
            
            # Enhanced schema with data type hints
            self.schema_details = []
            for field in spark_df.schema.fields:
                col_name = field.name
                col_type = field.dataType.simpleString()
                
                # Get sample values for better context
                samples = spark_df.select(col_name).limit(3).toPandas()[col_name].tolist()
                sample_str = ", ".join(str(s) for s in samples if s is not None)
                
                self.schema_details.append(
                    f"`{table_name}`.`{col_name}` ({col_type}) - Examples: [{sample_str}]"
                )
            
            # Enhanced sample data
            self.sample_data = pandas_df.head(self.config.sample_rows_for_llm).to_string(index=False)

            logger.info(
                "Dataset loaded: %s rows • %s columns • table '%s'",
                f"{self.row_count:,}",
                len(spark_df.columns),
                table_name,
            )
            return True, f"File '{filename}' loaded successfully"

        except Exception as exc:
            logger.error("Failed to load dataset %s: %s", filename, exc, exc_info=True)
            return False, str(exc)

    def _get_spark_session(self) -> SparkSession:
        """Lazily initialize and cache the Spark session."""
        if self.spark is not None:
            return self.spark

        logger.info("Initializing Spark session...")
        builder = (
            SparkSession.builder.appName(self.config.spark_app_name)
            .master(self.config.spark_master)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        )

        if self.config.python_executable:
            builder = (
                builder.config("spark.pyspark.python", self.config.python_executable)
                .config("spark.pyspark.driver.python", self.config.python_executable)
            )

        if getattr(self.config, "spark_driver_host", None):
            builder = builder.config("spark.driver.host", self.config.spark_driver_host)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        self.spark = spark
        return spark

    @staticmethod
    def _sanitize_table_name(name: str) -> str:
        """Create a Spark-safe table name from filename."""
        sanitized = re.sub(r"[^0-9a-zA-Z_]", "_", name)
        if not sanitized or sanitized[0].isdigit():
            sanitized = "table_" + sanitized
        return sanitized.lower()

    @staticmethod
    def _read_with_pandas(file_bytes: bytes, extension: str) -> pd.DataFrame:
        """Read uploaded bytes into a pandas DataFrame."""
        buffer = io.BytesIO(file_bytes)

        if extension == ".csv":
            return pd.read_csv(buffer)
        if extension in {".xlsx", ".xls"}:
            return pd.read_excel(buffer)
        if extension == ".json":
            return pd.read_json(buffer)
        if extension == ".parquet":
            return pd.read_parquet(buffer)

        raise ValueError(f"Unsupported file type: {extension}")
    
    def _init_chains(self):
        """Initialize enhanced LangChain chains with better prompts."""
        
        # Enhanced SQL Generation Chain
        sql_system = """You are an expert SQL query generator for Spark SQL.
Your task is to generate precise, optimized SQL queries based on natural language questions.

CRITICAL RULES:
1. Return ONLY the SQL query - no explanations, no markdown, no extra text
2. Use Spark SQL syntax (not MySQL/PostgreSQL specific features)
3. Always use the exact table name provided in the schema
4. Column names are case-sensitive - use them exactly as shown
5. For aggregations, always provide meaningful column aliases
6. For date operations: use date_sub(), date_add(), current_date()
7. Order results logically (e.g., by value DESC for rankings)
8. Limit large result sets appropriately (TOP 10, TOP 20, etc.)

QUERY PATTERNS:
- "Count X by Y" → SELECT Y, COUNT(*) as count FROM table GROUP BY Y ORDER BY count DESC
- "Total/Sum of X" → SELECT SUM(X) as total FROM table
- "Average X" → SELECT AVG(X) as average FROM table
- "Top N by X" → SELECT * FROM table ORDER BY X DESC LIMIT N
- "Distribution of X" → SELECT X, COUNT(*) as count FROM table GROUP BY X
- "Trend over time" → SELECT date_column, SUM(value) as total FROM table GROUP BY date_column ORDER BY date_column

EXAMPLES:
Question: "What is the total revenue?"
SQL: SELECT SUM(revenue) as total_revenue FROM sales

Question: "Count customers by status"
SQL: SELECT status, COUNT(*) as customer_count FROM customers GROUP BY status ORDER BY customer_count DESC

Question: "Top 5 products by sales"
SQL: SELECT product_name, SUM(sales) as total_sales FROM sales GROUP BY product_name ORDER BY total_sales DESC LIMIT 5
"""
        
        sql_human = """Generate a SQL query for this question:

QUESTION: {user_query}

SCHEMA:
{schema_str}

TABLE NAME: {table_name}

SAMPLE DATA:
{sample_data}

Return ONLY the SQL query:"""
        
        sql_prompt = ChatPromptTemplate.from_messages([
            ("system", sql_system),
            ("human", sql_human)
        ])
        self.sql_chain = sql_prompt | self.llm | self.parser
        
        # Enhanced Visualization Recommendation Chain
        viz_system = """You are a data visualization expert. Analyze the query and results to recommend the BEST chart type.

CRITICAL: If the user EXPLICITLY asks for a specific chart type (e.g., "use a pie chart", "show as bar chart"), you MUST recommend that chart type.

VISUALIZATION DECISION TREE:

1. EXPLICIT REQUEST (HIGHEST PRIORITY):
   - User says "pie chart" → "pie"
   - User says "bar chart" → "bar"
   - User says "line chart" → "line"
   - User says "table" → "table"
   - ALWAYS honor explicit requests!

2. SINGLE VALUE (1 row, 1-3 columns with single value):
   → "number" - Display as KPI card

3. RATIO/PROPORTION DATA (1 row with multiple count/ratio columns):
   Examples: "active_count, churned_count, inactive_count" in one row
   → "pie" - Perfect for showing proportions
   → "donut" - Alternative for proportions

4. COUNT/AGGREGATE QUERIES:
   - Single total/count/average → "number"
   - Breakdown by category (< 8 items) → "pie" or "donut"
   - Breakdown by category (8-20 items) → "bar"
   - Breakdown by category (> 20 items) → "horizontal_bar" or "table"

5. TIME SERIES (date/time column + numeric):
   → "line" - Show trends over time
   → "area" - For cumulative or stacked trends

6. COMPARISONS:
   - Few categories (< 8) → "bar" or "horizontal_bar"
   - Many categories (> 8) → "horizontal_bar" or "table"
   - Rankings → "horizontal_bar" (best for reading labels)

7. DISTRIBUTIONS/COMPOSITION:
   - Part-to-whole (< 10 items) → "pie"
   - Part-to-whole with percentages → "donut"
   - Distribution across categories (grouped data) → "bar"
   - Many items → "bar"

8. MULTI-DIMENSIONAL DATA:
   - Data with 3 columns (category1, category2, value) → "bar" with grouping
   - Example: store_location, product_category, count → grouped bar chart
   - Keywords: "across", "by location", "by store", "distribution" → "bar"

8. CORRELATIONS:
   - Two numeric variables → "scatter"
   - Multiple variables → "heatmap"

9. HIERARCHIES:
   - Nested categories + values → "treemap"

10. DETAILED DATA:
    - Many columns or complex data → "table"

CRITICAL RULES:
- ALWAYS check if user explicitly requested a chart type first
- For ratio/proportion queries (e.g., "ratio of X to Y"), use "pie" or "donut"
- For single row with multiple count columns, use "pie" (columns become slices)
- Consider query intent: "breakdown", "distribution", "split" → pie/donut preferred

Return ONLY valid JSON:
{{
  "visualization_type": "number|bar|horizontal_bar|pie|donut|line|area|scatter|heatmap|treemap|table",
  "title": "Clear, descriptive title",
  "x_axis": "column_name_for_x",
  "y_axis": "column_name_for_y",
  "description": "One sentence insight about the data",
  "reasoning": "Why this chart type was chosen"
}}"""
        
        viz_human = """Recommend the best visualization:

USER QUESTION: {user_query}

SQL QUERY: {sql_query}

RESULT PREVIEW (first 5 rows):
{query_results}

METADATA:
- Columns: {columns}
- Total Rows: {row_count}
- Column Types: {column_types}

Analyze and return JSON:"""
        
        viz_prompt = ChatPromptTemplate.from_messages([
            ("system", viz_system),
            ("human", viz_human)
        ])
        self.viz_chain = viz_prompt | self.llm | self.parser
        
        # Error Fix Chain
        error_system = """You are a SQL debugging expert for Spark SQL.
Analyze the error and fix the query. Common issues:
- Column name typos or case sensitivity
- Missing aggregations in GROUP BY
- Invalid Spark SQL functions
- Type mismatches

Return ONLY the corrected SQL query."""
        
        error_human = """Fix this SQL query:

ORIGINAL QUERY:
{sql_query}

ERROR MESSAGE:
{error_message}

SCHEMA:
{schema_str}

Return corrected SQL:"""
        
        error_prompt = ChatPromptTemplate.from_messages([
            ("system", error_system),
            ("human", error_human)
        ])
        self.error_chain = error_prompt | self.llm | self.parser
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process a natural language query end-to-end with enhanced intelligence."""
        if not self.spark or not self.schema_details:
            return {
                'success': False,
                'error': 'No dataset loaded. Please upload data first.',
            }

        try:
            logger.info("🔍 Processing query: %s", user_query)
            
            # Step 1: Generate SQL
            sql_query = self._generate_sql(user_query)
            logger.info("📝 Generated SQL: %s", sql_query[:150])
            
            # Step 2: Execute with retry
            result_df = self._execute_with_retry(sql_query)
            result_pandas = result_df.toPandas()
            
            if result_pandas.empty:
                return {
                    'success': False,
                    'error': 'Query returned no results. Try rephrasing your question.'
                }
            
            logger.info("✅ Got %d rows, %d columns", len(result_pandas), len(result_pandas.columns))
            
            # Step 3: Smart visualization recommendation
            viz_config = self._recommend_visualization(
                user_query,
                sql_query,
                result_pandas
            )
            
            logger.info("🎨 Recommended visualization: %s", viz_config.get('visualization_type'))
            
            return {
                'success': True,
                'sql_query': sql_query,
                'data': result_pandas,
                'viz_config': viz_config
            }
            
        except Exception as e:
            logger.error("❌ Query processing failed: %s", str(e), exc_info=True)
            return {
                'success': False,
                'error': f"Query processing failed: {str(e)}"
            }
    
    def _generate_sql(self, user_query: str) -> str:
        """Generate SQL from natural language."""
        table_name = self.table_name or 'data'
        
        sql = self.sql_chain.invoke({
            'user_query': user_query,
            'schema_str': '\n'.join(self.schema_details),
            'sample_data': self.sample_data,
            'table_name': table_name
        })
        
        # Clean up the response
        sql = sql.replace('```sql', '').replace('```', '').strip()
        sql = re.sub(r'^SQL:\s*', '', sql, flags=re.IGNORECASE)
        
        return sql
    
    def _execute_with_retry(self, sql_query: str) -> DataFrame:
        """Execute SQL with automatic error fixing."""
        spark = self._get_spark_session()
        current_sql = sql_query

        for attempt in range(self.config.max_retries):
            try:
                result = spark.sql(current_sql)
                # Force execution to catch errors
                _ = result.count()
                return result
                
            except Exception as e:
                error_msg = str(e)
                logger.warning("⚠️ Attempt %d failed: %s", attempt + 1, error_msg[:100])
                
                if attempt < self.config.max_retries - 1:
                    # Try to fix the error
                    current_sql = self.error_chain.invoke({
                        'schema_str': '\n'.join(self.schema_details),
                        'sql_query': current_sql,
                        'error_message': error_msg
                    })
                    current_sql = current_sql.replace('```sql', '').replace('```', '').strip()
                    logger.info("🔄 Retrying with fixed query...")
                else:
                    raise Exception(f"Failed after {self.config.max_retries} attempts: {error_msg}")
    
    def _recommend_visualization(
        self,
        user_query: str,
        sql_query: str,
        result_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """Get smart visualization recommendation with fallback logic."""
        
        # Prepare metadata
        column_types = {
            col: str(result_data[col].dtype) 
            for col in result_data.columns
        }
        
        results_preview = result_data.head(5).to_string(index=False)
        
        try:
            viz_response = self.viz_chain.invoke({
                'user_query': user_query,
                'sql_query': sql_query,
                'query_results': results_preview,
                'columns': ', '.join(result_data.columns.tolist()),
                'row_count': len(result_data),
                'column_types': json.dumps(column_types, indent=2)
            })
            
            # Parse JSON response
            viz_config = self._parse_viz_response(viz_response)
            
        except Exception as e:
            logger.warning("⚠️ LLM viz recommendation failed, using fallback: %s", str(e))
            viz_config = self._fallback_viz_config(result_data, user_query)
        
        # Validate and set defaults
        viz_config = self._validate_viz_config(viz_config, result_data)
        
        return viz_config
    
    def _parse_viz_response(self, response: str) -> Dict[str, Any]:
        """Parse LLM response to extract visualization config."""
        try:
            # Try direct JSON parse
            return json.loads(response)
        except json.JSONDecodeError:
            # Extract JSON from text
            json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(0))
                except:
                    pass
            
            # Last resort: extract key fields manually
            config = {}
            
            # Extract visualization_type
            type_match = re.search(r'"visualization_type"\s*:\s*"([^"]+)"', response)
            if type_match:
                config['visualization_type'] = type_match.group(1)
            
            # Extract title
            title_match = re.search(r'"title"\s*:\s*"([^"]+)"', response)
            if title_match:
                config['title'] = title_match.group(1)
            
            return config
    
    def _validate_viz_config(self, config: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
        """Validate and complete visualization configuration."""
        
        # Ensure required fields
        if 'visualization_type' not in config or not config['visualization_type']:
            config['visualization_type'] = 'auto'
        
        if 'title' not in config:
            config['title'] = 'Analysis Results'
        
        # Set axis defaults if not specified
        if len(data.columns) >= 1 and 'x_axis' not in config:
            config['x_axis'] = data.columns[0]
        
        if len(data.columns) >= 2 and 'y_axis' not in config:
            config['y_axis'] = data.columns[1]
        
        if 'description' not in config:
            config['description'] = f"Analysis of {len(data)} records"
        
        return config
    
    def _fallback_viz_config(self, data: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Intelligent fallback visualization configuration."""
        
        n_rows = len(data)
        n_cols = len(data.columns)
        
        # Single value
        if n_rows == 1 and n_cols <= 3:
            return {
                'visualization_type': 'number',
                'title': 'Result',
                'description': 'Single value result'
            }
        
        # Detect numeric columns
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        
        # Time series keywords
        time_keywords = ['trend', 'over time', 'timeline', 'history', 'monthly', 'weekly', 'daily']
        if any(kw in query.lower() for kw in time_keywords) and numeric_cols:
            return {
                'visualization_type': 'line',
                'title': 'Trend Analysis',
                'x_axis': data.columns[0],
                'y_axis': numeric_cols[0] if numeric_cols else data.columns[1],
                'description': 'Time series visualization'
            }
        
        # Distribution/breakdown
        dist_keywords = ['distribution', 'breakdown', 'by', 'each', 'per']
        if any(kw in query.lower() for kw in dist_keywords):
            if n_rows <= 8:
                return {
                    'visualization_type': 'pie',
                    'title': 'Distribution',
                    'x_axis': data.columns[0],
                    'y_axis': numeric_cols[0] if numeric_cols else data.columns[1],
                    'description': 'Distribution breakdown'
                }
            else:
                return {
                    'visualization_type': 'bar',
                    'title': 'Comparison',
                    'x_axis': data.columns[0],
                    'y_axis': numeric_cols[0] if numeric_cols else data.columns[1],
                    'description': 'Category comparison'
                }
        
        # Default based on data shape
        if n_rows <= 20 and numeric_cols:
            return {
                'visualization_type': 'bar',
                'title': 'Analysis Results',
                'x_axis': data.columns[0],
                'y_axis': numeric_cols[0],
                'description': f'Analysis of {n_rows} records'
            }
        
        # Fallback to table
        return {
            'visualization_type': 'table',
            'title': 'Data Table',
            'description': f'Detailed view of {n_rows} records'
        }