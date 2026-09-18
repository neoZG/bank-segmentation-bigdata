# Almacenamiento en la Nube, Comparativa de Bibliotecas y Hadoop MapReduce

## 1. Almacenamiento en la Nube (GCS)

El dataset se almacena en un bucket de Google Cloud Storage
(`gs://bank-segmentation-bigdata-data`, región `us-central1`, acceso
uniforme a nivel de bucket), con el archivo original en el prefijo `raw/`
(67.564.190 bytes, 1.048.567 filas, idéntico al archivo local).

Usar almacenamiento en la nube permite mantener el dataset disponible y
durable fuera de una sola máquina, que los procesos de Dataproc/Spark lo
lean directamente sin necesidad de subirlo de nuevo, y separar los datos
crudos (`raw/`) de los datos procesados (`processed/`) mediante prefijos
dentro de un mismo bucket.

![Contenido del bucket](gcp/screenshots/bucket-contents.png)

## 2. Arquitectura

![Diagrama de arquitectura](gcp/screenshots/architecture-diagram.png)

## 3. Comparativa de Bibliotecas: Local, Nube y Nube Distribuida

### 3.1 Metodología

Se ejecutó la misma consulta de control (filtrar `TransactionAmount > 0`,
agrupar por `CustLocation`, calcular conteo/suma/promedio, ordenar) en
Polars, Dask, Modin y PySpark, midiendo tiempo de ejecución y memoria pico
del proceso mediante un instrumento común (muestreo de RSS cada 50ms).

Se compararon tres entornos:

- **Local**: equipo del autor, leyendo el CSV desde disco local.
- **Nube**: una máquina virtual de Dataproc (`e2-standard-4`, 4 vCPU/16GB),
  leyendo el mismo CSV directamente desde el bucket (`gs://...`), con el
  mismo código, en un solo proceso (PySpark en modo `local[*]`).
- **Nube distribuida**: solo PySpark, misma máquina, pero con Spark
  apuntando a `--master yarn`, distribuyendo la ejecución entre los 2 nodos
  worker del clúster (`e2-standard-2` cada uno).

En el entorno distribuido, la memoria medida corresponde únicamente al
proceso driver; el cómputo real ocurre en los executores de los nodos
worker, por lo que ese valor no es comparable con los otros entornos en el
eje de memoria, solo en el de tiempo de ejecución.

### 3.2 Resultados

| Biblioteca | Entorno | Segundos | Memoria pico (MB) |
|---|---|---|---|
| Polars | Local | 0.11 | 633 |
| Polars | Nube | 1.55 | 660 |
| Dask | Local | 0.88 | 558 |
| Dask | Nube | 4.31 | 835 |
| Modin | Local | 1.79 | 200 |
| Modin | Nube | 10.51 | 311 |
| PySpark | Local | 2.42 | 135 |
| PySpark | Nube | 20.29 | 138 |
| PySpark | Nube distribuida | 25.18 | 140 |

![Tiempo de ejecución](gcp/screenshots/benchmark_time.png)
![Uso de memoria](gcp/screenshots/benchmark_memory.png)

### 3.3 Análisis

Todas las bibliotecas resultaron más lentas en la nube que en local, en
algunos casos de forma marcada (Polars 14x, PySpark hasta 8-10x). La
variable que cambia entre ambos entornos es el origen de los datos: lectura
de disco local frente a lectura por red desde Cloud Storage. Esta última
implica latencia de red y ausencia de caché de sistema operativo en una
máquina recién iniciada, lo cual explica la mayor parte de la diferencia
observada, más allá de la capacidad de cómputo de la máquina virtual.

La ejecución distribuida de PySpark sobre YARN (25,18s) resultó más lenta
que la misma consulta en un solo proceso en la nube (20,29s). El costo de
coordinación (asignación de contenedores en los nodos worker, shuffle de
datos por red) no se compensa con un dataset de este tamaño (~1M filas),
que cabe cómodamente en la memoria de una sola máquina. El beneficio del
procesamiento distribuido se manifiesta en volúmenes de datos que superan
la capacidad de una única máquina, no en este caso de prueba.

En cuanto a memoria, Polars, Dask y Modin muestran un consumo levemente
mayor en la nube, atribuible a la sobrecarga de las librerías de lectura
remota (`gcsfs`/`fsspec`). La memoria de PySpark se mantiene estable
(~135-140MB) en los tres entornos porque la métrica solo captura el
proceso driver, cuyo consumo no varía según dónde ocurra el cómputo real.

## 4. Hadoop MapReduce en Google Cloud Dataproc

Se seleccionaron y ejecutaron tres programas del archivo
`hadoop-mapreduce-examples.jar` (distintos de wordcount y grep) sobre un
clúster de Dataproc compuesto por un nodo maestro (`e2-standard-4`) y dos
nodos worker (`e2-standard-2` cada uno), región `us-central1`. El clúster
se eliminó inmediatamente después de su uso para evitar costos
innecesarios.

Los tres programas se ejecutaron sobre un mismo archivo de entrada,
`locations.txt`, generado a partir de la columna `CustLocation` del
dataset (`bank_transactions.csv`), con un valor por línea (1.048.567
líneas), almacenado en HDFS. Dado que estos programas tokenizan por
espacios en blanco, los nombres de ciudad compuestos por más de una palabra
(por ejemplo, "NAVI MUMBAI") se cuentan como dos palabras separadas,
resultando en 1.296.123 palabras totales.

### 4.1 WordMean

**Objetivo**: calcula la longitud promedio (en caracteres) de las palabras
del archivo de entrada, es decir, la longitud promedio de los nombres de
ciudad de los clientes.

**Comando**:
```
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar wordmean \
  /user/neo/input/locations.txt /user/neo/output/wordmean
```

**Resultado** (`hadoop fs -cat /user/neo/output/wordmean/part-r-*`):
```
count   1296123
length  8250551
```
Longitud promedio = 8.250.551 / 1.296.123 ≈ **6,37 caracteres**.

### 4.2 WordMedian

**Objetivo**: calcula la mediana de la longitud de las palabras, mediante
un histograma de longitud y frecuencia.

**Comando**:
```
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar wordmedian \
  /user/neo/input/locations.txt /user/neo/output/wordmedian
```

**Resultado** (histograma longitud → frecuencia, combinando las tres
particiones de salida):
```
1:10401  2:9769   3:97962  4:86371  5:295875 6:244228 7:199153
8:77717  9:198443 10:42360 11:13646 12:9000  13:7501  14:2175
15:662   16:257   17:87    18:466   19:3     20:9     21:21
22:4     25:4     29:9
```
El total de palabras (1.296.123) coincide con el valor de WordMean. La
posición central (palabra número 648.062) cae dentro de la longitud 6, por
lo que la **mediana es 6 caracteres**.

### 4.3 WordStandardDeviation

**Objetivo**: calcula la desviación estándar de la longitud de las
palabras, completando el perfil estadístico junto con WordMean y
WordMedian.

**Comando**:
```
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar wordstandarddeviation \
  /user/neo/input/locations.txt /user/neo/output/wordstddev
```

**Resultado**:
```
count   1296123
length  8250551
square  58602375
```
Varianza = (square/count) − promedio² = 45,21 − 40,52 ≈ 4,69
**Desviación estándar ≈ 2,17 caracteres**.

Los tres resultados son consistentes entre sí (promedio 6,37, mediana 6,
desviación estándar 2,17), describiendo una distribución razonable para la
longitud de nombres de ciudad.

## 5. Comandos de Infraestructura

```bash
# Creación del clúster
gcloud dataproc clusters create bank-bd-cluster \
  --region=us-central1 --zone=us-central1-a \
  --master-machine-type=e2-standard-4 --master-boot-disk-size=50GB \
  --num-workers=2 --worker-machine-type=e2-standard-2 --worker-boot-disk-size=50GB \
  --project=bank-segmentation-bigdata

# Ejecución de la comparativa en la nube
export BENCHMARK_ENVIRONMENT=cloud
export BENCHMARK_DATA_PATH=gs://bank-segmentation-bigdata-data/raw/bank_transactions.csv
python3 polars/example_query.py
python3 dask/example_query.py
python3 modin/example_query.py
python3 pyspark/example_query.py
BENCHMARK_ENVIRONMENT=cloud_distributed BENCHMARK_SPARK_MASTER=yarn python3 pyspark/example_query.py

# Eliminación del clúster
gcloud dataproc clusters delete bank-bd-cluster --region=us-central1 --quiet
```
