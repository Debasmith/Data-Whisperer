"""
Data Loading Module - Handles file uploads and Spark integration
Save this as: src/data/data_loader.py
"""

import io
import os
import tempfile
from pathlib import Path
from typing import Tuple, List
from pyspark.sql import SparkSession
import pandas as pd
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class DataLoader:
    """Handle data file loading and Spark table creation"""
    
    def __init__(self, config):
        self.config = config
        self.spark = None
        self.table_name = None
        self.schema_details = []
        self.sample_data = ""
        self.row_count = 0
        
        # Initialize Spark
        self._init_spark()
    
    def _init_spark(self):
        try:
            builder = (
                SparkSession.builder
                .appName(self.config.spark_app_name)
                .master(self.config.spark_master)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            )

            if getattr(self.config, "python_executable", None):
                builder = (
                    builder.config("spark.pyspark.python", self.config.python_executable)
                    .config("spark.pyspark.driver.python", self.config.python_executable)
                    .config("spark.executorEnv.PYSPARK_PYTHON", self.config.python_executable)
                )

            if getattr(self.config, "spark_driver_host", None):
                builder = builder.config("spark.driver.host", self.config.spark_driver_host)

            self.spark = builder.getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("✅ Spark session initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}", exc_info=True)
            raise
    
    def load_file(self, file_bytes: bytes, filename: str) -> Tuple[bool, str]:
        """Load file and create Spark table"""
        
        try:
            file_ext = Path(filename).suffix.lower()
            
            if file_ext not in self.config.supported_file_types:
                return False, f"Unsupported file type: {file_ext}"
            
            self.table_name = self._generate_table_name(filename)
            
            self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name}")
            
            if file_ext == '.csv':
                success, message = self._load_csv(file_bytes, filename)
            elif file_ext in ['.xlsx', '.xls']:
                success, message = self._load_excel(file_bytes, filename)
            elif file_ext == '.json':
                success, message = self._load_json(file_bytes, filename)
            elif file_ext == '.parquet':
                success, message = self._load_parquet(file_bytes, filename)
            else:
                return False, f"Handler not implemented for {file_ext}"
            
            if not success:
                return False, message
            
            self._extract_metadata()
            
            return True, f"Successfully loaded {self.row_count:,} rows from {filename}"
            
        except Exception as e:
            logger.error(f"Error loading file: {e}", exc_info=True)
            return False, f"Error loading file: {str(e)}"
    
    def _load_csv(self, file_bytes: bytes, filename: str) -> Tuple[bool, str]:
        """Load CSV file"""
        try:
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name
            
            try:
                df = self.spark.read.csv(
                    f"file:///{tmp_path}",
                    header=True,
                    inferSchema=True,
                    multiLine=True,
                    escape='"'
                )
                
                df.createOrReplaceTempView(self.table_name)
                
                return True, "CSV loaded successfully"
                
            finally:
                try:
                    os.unlink(tmp_path)
                except:
                    pass
                    
        except Exception as e:
            return False, f"CSV loading error: {str(e)}"
    
    def _load_excel(self, file_bytes: bytes, filename: str) -> Tuple[bool, str]:
        """Load Excel file"""
        try:
            excel_data = io.BytesIO(file_bytes)
            pdf = pd.read_excel(excel_data)
            
            df = self.spark.createDataFrame(pdf)
            
            df.createOrReplaceTempView(self.table_name)
            
            return True, "Excel loaded successfully"
            
        except Exception as e:
            return False, f"Excel loading error: {str(e)}"
    
    def _load_json(self, file_bytes: bytes, filename: str) -> Tuple[bool, str]:
        """Load JSON file"""
        try:
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.json', delete=False) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name
            
            try:
                df = self.spark.read.json(f"file:///{tmp_path}")
                
                df.createOrReplaceTempView(self.table_name)
                
                return True, "JSON loaded successfully"
                
            finally:
                try:
                    os.unlink(tmp_path)
                except:
                    pass
                    
        except Exception as e:
            return False, f"JSON loading error: {str(e)}"
    
    def _load_parquet(self, file_bytes: bytes, filename: str) -> Tuple[bool, str]:
        """Load Parquet file"""
        try:
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name
            
            try:
                df = self.spark.read.parquet(f"file:///{tmp_path}")
                
                df.createOrReplaceTempView(self.table_name)
                
                return True, "Parquet loaded successfully"
                
            finally:
                try:
                    os.unlink(tmp_path)
                except:
                    pass
                    
        except Exception as e:
            return False, f"Parquet loading error: {str(e)}"
    
    def _extract_metadata(self):
        """Extract schema details and sample data"""
        
        df = self.spark.table(self.table_name)
        
        self.row_count = df.count()
        
        schema = df.schema
        self.schema_details = []
        
        for field in schema.fields:
            col_name = field.name
            col_type = str(field.dataType)
            
            try:
                samples = df.select(col_name).distinct().limit(5).collect()
                sample_values = [str(row[0]) for row in samples if row[0] is not None]
                sample_str = ", ".join(sample_values)
                
                self.schema_details.append(
                    f"Column: `{col_name}` (Type: {col_type}) - Sample: [{sample_str}]"
                )
            except Exception as e:
                self.schema_details.append(
                    f"Column: `{col_name}` (Type: {col_type})"
                )
        
        sample_df = df.limit(self.config.sample_rows_for_llm)
        self.sample_data = sample_df.toPandas().to_string(index=False)
        
        logger.info(f"📊 Extracted metadata: {len(self.schema_details)} columns, {self.row_count} rows")
    
    def _generate_table_name(self, filename: str) -> str:
        """Generate a valid Spark table name"""
        name = Path(filename).stem
        name = ''.join(c if c.isalnum() else '_' for c in name)
        name = name.lower()
        
        if name[0].isdigit():
            name = 'table_' + name
        
        return name
    
    def get_dataframe(self):
        if self.table_name:
            return self.spark.table(self.table_name)
        return None