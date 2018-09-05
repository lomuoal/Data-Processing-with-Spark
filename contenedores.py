#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

from helpers import *

path_containers = 'data/containers.csv'

def file2(sc):
  rdd_file = sc.textFile("data/containers.csv")
  header = rdd_file.first()
  rdd_file = rdd_file.filter(lambda x: x != header)
  return rdd_file.map(lambda line: line.split(";"))

def ejercicio_0(sc, path_resultados):
  lineas = sc.parallelize(range(10)).collect()
  with open(path_resultados(0), 'w') as f:
    f.write("{}\n".format(",".join([str(s) for s in lineas])))
  return lineas

# Ejercicio 1. Leer el archivo data/containers.csv y contar el número de líneas.
def ejercicio_1(sc, path_resultados):
  rdd_file = sc.textFile("data/containers.csv")
  # Devolver número de líneas
  return rdd_file.count()


# Ejercicio 2. Leer el archivo data/containers.csv y filtrar aquellos 
# contenedores cuyo ship_imo es DEJ1128330 y el grupo del contenedor es 22P1.
# Guardar los resultados en un archivo de texto en resultados/resutado_2.
def ejercicio_2(sc, path_resultados):
  # COMPLETAR CODIGO AQUI
  # Devolver rdd contenedores filtrados (rdd.collect())
  # Guardar en resultados/resultado_2. La función path_resultados devuelve
  # la ruta donde se van a guardar los resultados, para que los tests puedan
  # ejecutar de forma correcta. Por ejemplo, path_resultados(2) devuelve la
  # ruta para el ejercicio 2, path_resultados(3) para el 3, etc.

  rdd_file = file2(sc)
  rdd_file = rdd_file.filter(lambda s: s[0] == "DEJ1128330" and s[6] == "22P1").collect()
  with open(path_resultados(2), 'w') as f:
    for s in rdd_file:
      f.write(str(s) + "\n")

  return rdd_file

# Ejercicio 3. Leer el archivo data/containers.csv y convertir a formato 
# Parquet. Recuerda que puedes hacer uso de la funcion parse_container en
# helpers.py tal y como vimos en clase. Guarda los resultados en 
# resultados/resultado_3.
def ejercicio_3(sc, path_resultados):
  # COMPLETAR CODIGO AQUI
  
  context = SQLContext(sc)

  csv_source = sc.textFile("data/containers.csv").filter(lambda s: not s.startswith(container_fields[0])).map(parse_container).map(lambda c: Row(**dict(c._asdict())))

  containerSchema = context.createDataFrame(csv_source)
  containerSchema.createOrReplaceTempView('container')

  df = context.sql("SELECT * FROM container")

	# Guardar resultados y devolver RDD/DataFrame

  df.write.mode('overwrite').parquet(path_resultados(3))

  return df



# Ejercicio 4. Lee el archivo de Parquet guardado en el ejercicio 3 y filtra 
# los barcos que tienen al menos un contenedor donde la columna customs_ok es 
# igual a false. Extrae un fichero de texto una lista con los identificadores 
# de barco, ship_imo, sin duplicados y ordenados alfabéticamente.
def ejercicio_4(sc, path_resultados):
  # COMPLETAR CODIGO AQUI
  # Guardar resultados y devolver RDD/DataFrame

  context = SQLContext(sc)
  df = context.read.parquet(path_resultados(3))

  df.createOrReplaceTempView('container')
  query = context.sql("SELECT DISTINCT ship_imo FROM container WHERE customs_ok = 'false' ORDER BY ship_imo")
  rdd_query = query.rdd.collect()
  with open(path_resultados(4), 'w') as f:
    for s in rdd_query:
      f.write(str(s.ship_imo) + "\n")

  return query

# Ejercicio 5. Crea una UDF para validar el código de identificación del 
# contenedor container_id. Para simplificar la validación, daremos como 
# válidos aquellos códigos compuestos de 3 letras para el propietario, 1 
# letra para la categoría, 6 números y 1 dígito de control. Devuelve un
# DataFrame con los campos: ship_imo, container_id, propietario, categoria, 
# numero_serie y digito_control.
def ejercicio_5(sc, path_resultados):
  # COMPLETAR CODIGO AQUI
  # Guardar resultados y devolver RDD/DataFrame
  context = SQLContext(sc)
  df = context.read.parquet(path_resultados(3))

  df.createOrReplaceTempView('container')
  #Siguiendo como guía el ejemplo container.py (directorio spark)
  context.registerFunction('propietario',lambda c: c[0:3])
  context.registerFunction('categoria', lambda c: c[3])
  context.registerFunction('numeros', lambda c: c[4:10])
  context.registerFunction('digitocontrol', lambda c: c[-1])
  context.registerFunction('correcto', lambda c: c[0:4].isalpha() and c[4:11].isdigit() and len(c)==11 )

  query = context.sql("SELECT ship_imo, container_id, propietario(container_id) as propietario, "
                    "categoria(container_id) as categoria, numeros(container_id) as numero_serie, "
                    "digitocontrol(container_id) as digcontrol "
                    "FROM container "
                    "WHERE correcto(container_id) = 'true'")

  return query

# Ejercicio 6. Extrae una lista con peso total de cada barco, `net_weight`, 
# sumando cada contenedor y agrupado por los campos `ship_imo` y `container_group`. 
# Devuelve un DataFrame con la siguiente estructura: 
# `ship_imo`, `ship_name`, `container`, `total_net_weight`.
def ejercicio_6(sc, path_resultados):
  # COMPLETAR CODIGO AQUI
  # Guardar resultados y devolver RDD/DataFrame

  context = SQLContext(sc)
  df = context.read.parquet(path_resultados(3))

  df.createOrReplaceTempView('container')
  query = context.sql( "SELECT ship_imo, ship_name, container_group as container, SUM(net_weight) as total_net_weight FROM container GROUP BY ship_imo, ship_name, container_group")


  return query

# Ejercicio 7. Guarda los resultados del ejercicio anterior en formato Parquet.
def ejercicio_7(sc, path_resultados):
  query = ejercicio_6(sc, path_resultados)

  query.write.mode('overwrite').parquet(path_resultados(7))

#Ejercicio 8. ¿En qué casos crees que es más eficiente utilizar formatos como Parquet? ¿Existe alguna desventaja frente a formatos de texto como CSV?
#Solución:

# -Parquet es ideal para usar con Spark SQL.

# -Spark SQL es mucho más rápido con Parquet. Consultas que tardan aprox. 12 horas en completarse usando archivos CSV, con formato parquet tardan menos de una hora, con lo que se observa una mejora de rendmiento de 11 veces mayor.


#Ejercicio 9. ¿Es posible procesar XML mediante Spark? ¿Existe alguna restricción por la cual no sea eficiente procesar un único archivo en multiples nodos? ¿Se te ocurre alguna posible solución para trocear archivos suficientemente grandes? ¿Existe la misma problemática con otros formatos de texto como JSON?
#Solución:
# -Si que es posible procesar XML mediante Spark
# -La estructura en arbol de XML es una restricción para procesar el archivo en multiples nodos.
# -Para archivos más grandes puede usar los formatos de entrada de Hadoop. Si la estructura es simple, puede dividir registros utilizando textinputformat.record.delimiter. La entrada no es un XML sino que debes idear cómo procesarlo. De lo contrario Mahout proporciona XmlInputFormat
#-Con JSON, spark puede acceder a los campos directamente.

#Ejercicio 10. Spark SQL tiene una función denominada avg que se utiliza para calcular el promedio de un conjunto de valores ¿Por qué los autores han creado esta función en lugar de usar el API estándar de Python o Scala?
#Solución:
#En lugar de usar el API de Python o Scala, los autores decidieron crear la función avg porque resulta mucho más eficiente.
def main():
  sc = SparkContext('local', 'practicas_spark')
  pr = definir_path_resultados('./resultados')
  ejercicio_0(sc, pr)
  ejercicio_1(sc, pr)
  ejercicio_2(sc, pr)
  ejercicio_3(sc, pr)
  ejercicio_4(sc, pr)
  ejercicio_5(sc, pr)
  ejercicio_6(sc, pr)
  ejercicio_7(sc, pr)

if __name__ == '__main__':
  main()
