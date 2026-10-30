from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round, desc

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisTransporteBatch") \
    .getOrCreate()

# 2. Cargar el dataset real asegurando la codificación correcta
archivo_fuente = "transporte.csv"
df_original = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .option("encoding", "ISO-8859-1") \
                        .csv(archivo_fuente)

print("--- 1. Esquema Original del Dataset ---")
df_original.printSchema()
print(f"Total de registros iniciales: {df_original.count()}")

# Renombramos las columnas según la estructura de tu archivo (Año, Mes, Información, Cantidad)
df_renombrado = df_original.toDF("Anio", "Mes", "Informacion", "Cantidad")

# 3. Limpieza de Datos: Eliminar duplicados y valores nulos en la columna Cantidad
df_limpio = df_renombrado.dropDuplicates().dropna(subset=["Cantidad"])
print(f"Total de registros después de la limpieza: {df_limpio.count()}")

# 4. Transformación: Filtrar registros válidos
df_filtrado = df_limpio.filter(col("Cantidad") > 0)

print("--- 2. Muestra de Datos Limpios y Transformados ---")
df_filtrado.show(10, truncate=False)

# 5. Análisis Exploratorio (EDA) y Visualización formal
print("--- 3. Análisis Agrupado por Tipo de Información ---")
analisis_resultado = df_filtrado.groupBy("Informacion") \
    .agg(
        count("*").alias("Total_Registros"),
        sum("Cantidad").alias("Suma_Total_Cantidad"),
        round(avg("Cantidad"), 2).alias("Promedio_Cantidad")
    ) \
    .orderBy(desc("Suma_Total_Cantidad"))

analisis_resultado.show(truncate=False)