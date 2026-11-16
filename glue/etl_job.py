import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# 1. Parámetros que recibe el job
#    --ENTRADA: ruta del CSV en S3 (la envía Lambda en cada ejecución)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENTRADA'])
ENTRADA = args['ENTRADA']
SALIDA = 's3://prueba-etl-juan-2026/processed/boston_housing/'

# 2. Arranque de Spark y Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 3. EXTRACT: leer el CSV desde S3
df = (spark.read
      .option('header', True)
      .option('inferSchema', True)
      .csv(ENTRADA))

# 4. TRANSFORM: limpiar y enriquecer
columnas = [c.strip().lower().replace(' ', '_') for c in df.columns]
df = df.toDF(*columnas)
df = df.dropDuplicates().dropna()
df = df.withColumn(
    'precio_categoria',
    F.when(F.col('medv') >= 30, 'alto')
     .when(F.col('medv') >= 20, 'medio')
     .otherwise('bajo')
)
df.printSchema()
print(f'Filas procesadas: {df.count()}')

# 5. LOAD: guardar en S3 como Parquet
df.write.mode('overwrite').parquet(SALIDA)

job.commit()
