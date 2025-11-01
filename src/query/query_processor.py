"""Query Processing Module with LLM Integration and dataset loading."""

import io
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    SystemMessagePromptTemplate,
)
from langchain_openai import ChatOpenAI
from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class QueryProcessor:
    """Process natural language queries into SQL and visualizations."""

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

        logger.info("Query processor initialized")

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
            self.schema_details = [
                f"`{table_name}`.`{field.name}` {field.dataType.simpleString()}"
                for field in spark_df.schema.fields
            ]
            self.sample_data = json.dumps(
                pandas_df.head(self.config.sample_rows_for_llm).to_dict(orient="records"),
                ensure_ascii=False,
                indent=2,
            )

            logger.info(
                "Dataset loaded: %s rows • %s columns • table '%s'",
                f"{self.row_count:,}",
                len(spark_df.columns),
                table_name,
            )

            return True, f"File '{filename}' loaded successfully"

        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Failed to load dataset %s: %s", filename, exc, exc_info=True)
            return False, str(exc)

    def _get_spark_session(self) -> SparkSession:
        """Lazily initialise and cache the Spark session."""

        if self.spark is not None:
            return self.spark

        logger.info("Initialising Spark session...")
        builder = (
            SparkSession.builder.appName(self.config.spark_app_name)
            .master(self.config.spark_master)
        )

        if self.config.python_executable:
            builder = (
                builder.config("spark.pyspark.python", self.config.python_executable)
                .config("spark.pyspark.driver.python", self.config.python_executable)
                .config("spark.executorEnv.PYSPARK_PYTHON", self.config.python_executable)
            )

        if getattr(self.config, "spark_driver_host", None):
            builder = builder.config("spark.driver.host", self.config.spark_driver_host)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        self.spark = spark
        return spark

    def _safe_chain_invoke(
        self,
        chain,
        inputs: Dict[str, Any],
        stage: str,
        allow_retry: bool = True
    ) -> str:
        """Run an LLM chain with defensive error handling and adaptive retries."""

        try:
            return chain.invoke(inputs)
        except Exception as exc:
            error_message = str(exc)

            if "memory layout cannot be allocated" in error_message:
                if allow_retry:
                    logger.warning(
                        "LLM memory error during %s; retrying with reduced context.",
                        stage,
                    )
                    reduced_inputs = self._reduce_llm_inputs(inputs)
                    return self._safe_chain_invoke(
                        chain,
                        reduced_inputs,
                        stage,
                        allow_retry=False,
                    )

                logger.error(
                    "LLM chain failed during %s even after context reduction: %s",
                    stage,
                    exc,
                    exc_info=True,
                )
                raise RuntimeError(
                    "LLM service ran out of memory while {stage}. Please retry shortly "
                    "or simplify the request."
                ).format(stage=stage) from exc

            logger.error("LLM chain failed during %s: %s", stage, exc, exc_info=True)
            raise

    def _reduce_llm_inputs(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Truncate large text fields before retrying an LLM call."""

        limits = {
            'schema_str': 3000,
            'sample_data': 2000,
            'user_query': 1000,
            'sql_query': 2500,
            'query_results': 2000,
            'columns': 1000,
            'row_count': 100,
            'table_name': 200,
        }

        reduced: Dict[str, Any] = {}
        for key, value in inputs.items():
            if isinstance(value, str):
                limit = limits.get(key, 2000)
                reduced[key] = self._truncate_text(value, limit)
            else:
                reduced[key] = value

        return reduced

    @staticmethod
    def _truncate_text(text: str, limit: int) -> str:
        """Limit text length while marking truncation for the LLM."""

        if len(text) <= limit:
            return text

        trimmed = text[:limit].rstrip()
        return f"{trimmed}\n...[truncated]"

    @staticmethod
    def _sanitize_table_name(name: str) -> str:
        """Create a Spark-safe table name from filename."""

        sanitized = re.sub(r"[^0-9a-zA-Z_]", "_", name)
        if not sanitized:
            sanitized = "data"
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
        """Initialize LangChain chains"""
        
        sql_system = """You are an expert SQL query generator for Spark SQL.
Generate precise, optimized queries based on the schema provided.

CRITICAL RULES:
1. Return ONLY the SQL query, no explanations or markdown
2. Use Spark SQL syntax (not MySQL/PostgreSQL)
3. For date operations: use date_sub(current_date(), N) or date_add()
4. Always use the table name exactly as provided in the schema
5. Use simple, clear aliases (A, B, C, etc.)
6. For COUNT queries, return the count with a meaningful column name
7. Ensure column names match the schema exactly (case-sensitive)

EXAMPLES:
- "Count customers by status" → SELECT status, COUNT(*) as customer_count FROM table_name GROUP BY status
- "What is the total revenue?" → SELECT SUM(revenue) as total_revenue FROM table_name
- "Average age" → SELECT AVG(age) as average_age FROM table_name
"""
        
        sql_human = """Based on this schema and sample data, write a SQL query for: {user_query}

SCHEMA:
{schema_str}

SAMPLE DATA:
{sample_data}

TABLE NAME: {table_name}

SQL QUERY:"""
        
        sql_prompt = ChatPromptTemplate.from_messages([
            SystemMessagePromptTemplate.from_template(sql_system),
            HumanMessagePromptTemplate.from_template(sql_human)
        ])
        
        self.sql_chain = sql_prompt | self.llm | self.parser
        
        validation_system = """You are a SQL validator for Spark SQL.
Check if the query is valid and fix any errors.
Return ONLY the corrected SQL query."""
        
        validation_human = """Validate and fix this SQL query if needed:

SCHEMA:
{schema_str}

SQL QUERY:
{sql_query}

CORRECTED SQL:"""
        
        validation_prompt = ChatPromptTemplate.from_messages([
            SystemMessagePromptTemplate.from_template(validation_system),
            HumanMessagePromptTemplate.from_template(validation_human)
        ])
        
        self.validation_chain = validation_prompt | self.llm | self.parser
        
        viz_system = """You are a data visualization expert.
Recommend the BEST visualization for the given query and results.

VISUALIZATION TYPES:
- "number" or "kpi": Single numeric value (counts, totals, averages)
- "bar": Comparing categories (5-20 items)
- "pie": Part-to-whole for 3-10 categories
- "line": Trends over time
- "scatter": Correlation between two numeric variables
- "table": Detailed data with many columns or rows

DECISION RULES:
1. Single value (1 row, 1-3 columns) → "number"
2. Count/Total queries → "number" if single value, else "bar" or "pie"
3. Distribution/Breakdown with <10 items → "pie"
4. Distribution/Breakdown with 10+ items → "bar"
5. Time series → "line"
6. Correlation → "scatter"
7. Detailed data → "table"

Return ONLY valid JSON:
{{
  "visualization_type": "number|bar|pie|line|scatter|table",
  "title": "Clear, descriptive title",
  "x_axis": "column_name",
  "y_axis": "column_name",
  "description": "Brief insight about the data"
}}"""
        
        viz_human = """Recommend visualization for:

USER QUERY: {user_query}
SQL: {sql_query}

RESULTS (first 5 rows):
{query_results}

Columns: {columns}
Row count: {row_count}

JSON ONLY:"""
        
        viz_prompt = ChatPromptTemplate.from_messages([
            SystemMessagePromptTemplate.from_template(viz_system),
            HumanMessagePromptTemplate.from_template(viz_human)
        ])
        
        self.viz_chain = viz_prompt | self.llm | self.parser
        
        # Error Fix Chain
        error_system = """You are a SQL debugging expert for Spark SQL.
Fix the SQL query based on the error message.
Always return a COMPLETE Spark SQL statement that starts with SELECT and can be executed directly.
Return ONLY the corrected SQL query."""
        
        error_human = """Fix this SQL query:

SCHEMA:
{schema_str}

ORIGINAL QUERY:
{sql_query}

ERROR:
{error_message}

CORRECTED SQL:"""
        
        error_prompt = ChatPromptTemplate.from_messages([
            SystemMessagePromptTemplate.from_template(error_system),
            HumanMessagePromptTemplate.from_template(error_human)
        ])
        
        self.error_chain = error_prompt | self.llm | self.parser
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process a natural language query end-to-end."""

        if not self.spark or not self.schema_details:
            return {
                'success': False,
                'error': 'No dataset loaded. Please upload data first.',
            }

        try:
            # Step 1: Generate SQL
            logger.info("🔧 Generating SQL...")
            sql_query = self._generate_sql(user_query)
            
            # Step 2: Validate SQL
            logger.info("✓ Validating SQL...")
            sql_query = self._validate_sql(sql_query)
            logger.info(f" SQL: {sql_query[:100]}...")
            
            # Step 3: Execute with auto-fix
            logger.info("Executing query...")
            result_df = self._execute_with_retry(sql_query)
            
            # Convert to pandas
            result_pandas = result_df.toPandas()
            
            if result_pandas.empty:
                return {
                    'success': False,
                    'error': 'Query returned no results'
                }
            
            logger.info(f" Got {len(result_pandas)} rows, {len(result_pandas.columns)} columns")
            
            logger.info("Generating visualization...")
            viz_config = self._recommend_visualization(
                user_query,
                sql_query,
                result_pandas
            )
            
            logger.info(f" Visualization: {viz_config.get('visualization_type', 'auto')}")
            
            return {
                'success': True,
                'sql_query': sql_query,
                'data': result_pandas,
                'viz_config': viz_config
            }
            
        except Exception as e:
            logger.error(f"Query processing failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def _generate_sql(self, user_query: str) -> str:
        """Generate SQL from natural language."""

        if not self.schema_details:
            raise ValueError("Schema details are unavailable. Load data first.")

        table_name = (
            self.schema_details[0].split('`')[1]
            if '`' in self.schema_details[0]
            else self.table_name or 'data'
        )

        sql = self._safe_chain_invoke(self.sql_chain, {
            'user_query': user_query,
            'schema_str': '\n'.join(self.schema_details),
            'sample_data': self.sample_data,
            'table_name': table_name
        }, 'SQL generation')
        
        sql = sql.replace('```sql', '').replace('```', '').strip()
        
        return sql
    
    def _validate_sql(self, sql_query: str) -> str:
        """Validate and fix SQL query."""

        validated = self._safe_chain_invoke(self.validation_chain, {
            'schema_str': '\n'.join(self.schema_details),
            'sql_query': sql_query
        }, 'SQL validation')
        
        validated = validated.replace('```sql', '').replace('```', '').strip()
        
        return validated
    
    def _execute_with_retry(self, sql_query: str):
        """Execute SQL with automatic error fixing."""

        spark = self._get_spark_session()
        current_sql = sql_query

        for attempt in range(self.config.max_retries):
            try:
                result = spark.sql(current_sql)
                return result
                
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"Attempt {attempt + 1} failed: {error_msg[:100]}...")
                
                if attempt < self.config.max_retries - 1:
                    # Try to fix the error
                    current_sql = self._safe_chain_invoke(self.error_chain, {
                        'schema_str': '\n'.join(self.schema_details),
                        'sql_query': current_sql,
                        'error_message': error_msg
                    }, 'SQL error correction')
                    
                    current_sql = current_sql.replace('```sql', '').replace('```', '').strip()
                    logger.info(f"Retrying with fixed query...")
                else:
                    raise Exception(f"Failed after {self.config.max_retries} attempts: {error_msg}")
    
    def _recommend_visualization(
        self,
        user_query: str,
        sql_query: str,
        result_data
    ) -> Dict[str, Any]:
        """Get visualization recommendation from LLM"""
        
        results_preview = result_data.head(5).to_string(index=False)
        
        viz_response = self._safe_chain_invoke(self.viz_chain, {
            'user_query': user_query,
            'sql_query': sql_query,
            'query_results': results_preview,
            'columns': ', '.join(result_data.columns.tolist()),
            'row_count': len(result_data)
        }, 'visualization recommendation')
        
        # Parse JSON
        try:
            # Try direct JSON parse
            viz_config = json.loads(viz_response)
        except json.JSONDecodeError:
            # Try to extract JSON from response
            start_idx = viz_response.find('{')
            end_idx = viz_response.rfind('}') + 1
            
            if start_idx >= 0 and end_idx > start_idx:
                json_str = viz_response[start_idx:end_idx]
                try:
                    viz_config = json.loads(json_str)
                except:
                    viz_config = self._fallback_viz_config(result_data, user_query)
            else:
                viz_config = self._fallback_viz_config(result_data, user_query)
        
        # Validate and set defaults
        if 'visualization_type' not in viz_config:
            viz_config['visualization_type'] = 'auto'
        
        if 'title' not in viz_config:
            viz_config['title'] = 'Analysis Results'
        
        # Set axis defaults
        if len(result_data.columns) >= 2:
            if 'x_axis' not in viz_config:
                viz_config['x_axis'] = result_data.columns[0]
            if 'y_axis' not in viz_config:
                viz_config['y_axis'] = result_data.columns[1]
        
        return viz_config
    
    def _fallback_viz_config(self, data, query: str) -> Dict[str, Any]:
        """Fallback visualization configuration"""
        
        config = {
            'visualization_type': 'auto',
            'title': 'Analysis Results',
            'description': 'Data analysis complete'
        }
        
        if len(data.columns) >= 2:
            config['x_axis'] = data.columns[0]
            config['y_axis'] = data.columns[1]
        
        return config