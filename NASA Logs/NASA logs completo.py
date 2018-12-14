
# coding: utf-8

# In[1]:

from pyspark import SparkContext
sc = SparkContext()


# In[17]:

import urllib
f = urllib.urlretrieve("https://www.dropbox.com/s/73wr8xb5s6fdj7g/apache.access.log.PROJECT?dl=1", "apache.access.log.PROJECT")


# In[2]:

datos = "./apache.access.log.PROJECT"
datos_raw = sc.textFile(datos)


# In[19]:

datos_raw.count()


# In[20]:

datos_raw.take(5)


# In[3]:

#Vamos a tratar los datos previamente
datos_tratados = datos_raw.map(lambda x: x.replace(" -", ' 0'))    .map(lambda x: x.split(" "))

#Vamos a recorrer los datos imprimiendo cada columna para ver si está todo correcto
for i in range(0,10):
    datos_test = datos_tratados.map(lambda x: x[i])
    print(datos_test.take(1))

#0 es la url
#1 no es nada
#2 no es nada
#3 es la fecha - Habria que parsear con []
#4 ni idea, pero creo que nada
#5 protocolo de respuesta: GET/POST
#6 url de archivo accedido
#7 HTTP version
#8 HTTP status - Access through -2
#9 Size - Access through -1   


# In[247]:

#### EJERCICIO 1 ######

#Podemos transformar los datos aplicando map al dataset para obtener solo la columna
#deseada

datos_split = datos_tratados.map(lambda x: int(x[-1]))
print(datos_split.max())
print(datos_split.min())
print(datos_split.mean())


# In[23]:

####### EJERCICIO 2 #########
### Numero de peticiones de cada codigo de respuesta

from operator import add
response_code = datos_tratados.map(lambda y: (int(y[-2]), 1))

#Muestra una lista de tuplas con valor(statusCode, count)
response_code.reduceByKey(add).sortByKey().collect()


# In[24]:

##### EJERCICIO 3 ######
# Mostrar 20 hosts que han sido visitados mas de 10 veces
hosts = datos_tratados.map(lambda y: (y[0], 1))
agrupados = hosts.reduceByKey(add)    .sortByKey()    .filter(lambda x: x[1] > 10)    .take(20)

#Muestra una lista de tuplas con valor(host, count)
agrupados


# In[228]:

##### EJERCICIO 4 #####
# Mostrar los 10 endpoints mas visitados
endpoint = datos_tratados.map(lambda y: (y[6], 1))
endpoint_agr = endpoint.reduceByKey(add)    .sortBy(lambda x: x[1], False)    .take(10)

#Muestra una lista de tuplas con valor(endpoint, count)
endpoint_agr


# In[229]:

##### EJERCICIO 5 ######
# Mostrar 10 endpoints que NO tengan codigo 200
endpointNO = datos_tratados.map(lambda y: (y[6], y[-2]))
endpointNO_agr = endpointNO.sortBy(lambda x: x[1], False)    .filter(lambda x: x[1] != 200)    .take(10)

#Muestra una lista de tuplas con valor(endpoint, statusCode)
#Con la condicion del status code distinto de 200
endpointNO_agr


# In[230]:

##### EJERCICIO 6 ######
# Calcular el numero de hosts distintos

hosts_distinct = datos_tratados    .map(lambda y: y[0]).distinct().count()

#Muestra un integer con el numero de hosts distintos
hosts_distinct


# In[5]:

from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import *
sqlContext = SQLContext(sc)


# In[6]:

##### EJERCICIO 7 #####
# Calcular el numero de host unicos cada dia

row_data = datos_tratados.map(lambda p: Row(
    host = p[0], 
    empty_first = int(p[1]),
    empty_second = p[2],
    date = p[3],
    protocol = p[4],
    endpoint = p[6],
    version = p[7],
    status_Code = int(p[-2]),
    size = p[-1]
    )
)

interactions_df = sqlContext.createDataFrame(row_data)

newdf = interactions_df.withColumn('date', regexp_replace('date', '\[', ''))
newdf2 = newdf.withColumn('date', regexp_replace('date', 'Aug', '08'))

newdf3 = newdf2.select(from_unixtime(unix_timestamp('date', 'dd/MM/yyyy:hh:mm:ss')).alias('date'), 'host')

newdf4 = newdf3.na.fill(0)

#Muestra los host distintos de cada día del mes de Agosto
newdf4.groupby(dayofmonth("date")        .alias("Dia del mes"))        .agg(countDistinct('host').alias('Host distintos'))        .sort('Dia del mes')        .show()


# In[7]:

##### EJERCICIO 8 #####
# Calcular la media de peticiones diarias por host

data_frame = newdf4.withColumn("month", month(col("date"))).                                   withColumn("DayOfmonth", dayofmonth(col("date")))
data_frame.createOrReplaceTempView('data_frame')
sqlContext.sql("SELECT DATE(date) Date, COUNT(host)/COUNT(DISTINCT host)                AS Peticiones_diarias_host FROM data_frame GROUP BY  DATE(date) ORDER BY DATE(date) ASC").show(n = 10)


# In[232]:

##### EJERCICIO 9 #####
# Mostrar una lista de 40 endpoints distintos que tiran 404

endpoint_error = datos_tratados.map(lambda y: (y[6], int(y[-2])))
endpointerror_agr = endpoint_error.reduceByKey(add)    .sortBy(lambda x: x[1], False)    .filter(lambda x: x[1] == 404)    .distinct()    .take(40)

#Muestra una lista de tuplas con valor(endpoint, statusCode)
endpointerror_agr


# In[234]:

##### EJERCICIO 10 #####
# Top 25 endpoints que más 404 tiran

#Necesitamos solo el status code 404, el endpoint y un contador para agrupar dentro de la tupla
endp_filtrado = datos_tratados.filter(lambda x: int(x[-2]) == 404)

endpoint_top_error = endp_filtrado.map(lambda x: (x[6], 1))
endpointtoperror_agr = endpoint_top_error.reduceByKey(add)    .sortBy(lambda x: x[1], False)    .take(25)

endpointtoperror_agr


# In[122]:

##### EJERCICIO 11 #####
# Top 5 días que generaron código de respuesta 404
newdf5 = newdf2.select(from_unixtime(unix_timestamp('date', 'dd/MM/yyyy:hh:mm:ss'))                       .alias('date'), 'status_Code')

newdf5.filter(newdf5['status_Code'] == 404)        .groupBy(dayofmonth('date'))        .count()        .sort(col("count").desc())        .show(n = 5)

