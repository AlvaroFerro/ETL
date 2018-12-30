
# coding: utf-8

# # Examen ETL: SPARK 09/02

# Se podrá utilizar toda la información que se encuentra en el campus. 
# 
# El fichero de datos sobre el que se trabajará es el de DataSetPartidos.txt.
# 
# A cada una de las preguntas hay que responder explicando brevemente que se pretende hacer antes de lanzar el código.
# 
# Al documento lo llamareís con vuestro nombre y apellido. Debeís enviarlo a mi correo de CUNEF antes del final del examen o en su defecto por email.
# 
# El lenguaje para trabajar con Spark podrá ser python o R indistintamente.

# ## Primera pregunta: ¿ En que se diferencian las acciones de las transformaciones en SPARK? ¿Podrías ponerme un par de ejemplo de cada una de ellas y describir que hacen, para que sirve?

# Las transformaciones crean un nuevo dataset sobre uno ya existente y las acciones que devuelven valores al driver después de hacer una computación en un dataset.
# 
# - Map(transformación): Pasa cada elemento de un dataset a través de una función especificada y devuelve un RDD nuevo con los valores especificados por el usuario.
# - Reduce (acción): Agrega todos los elementos del RDD usando una función y devuelve el resultado final al driver.
# - Filter (transformación): Filtra los elementos del dataset en función de las restricciones especificadas.
# - Collect (acción): Devuelve todos los elementos del dataset especificado.

# ## Segunda tarea: Inicializar spark context y cargar los datos desde el fichero.

# In[1]:

from pyspark import SparkContext
sc = SparkContext()


# In[2]:

from pyspark import SQLContext
sqlContext = SQLContext(sc)


# In[3]:

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime as dt


# In[4]:

datos = "./DataSetPartidos.txt"
raw_data = sc.textFile(datos)


# In[5]:

raw_data.take(1)


# In[6]:

raw_data.count()


# In[7]:

datos_tratados = raw_data.map(lambda x: x.split('::'))
datos_tratados.take(1)


# In[10]:

DatosTratadosDF = datos_tratados.map(lambda x: Row(Id = x[0],
                                                    Temporada = x[1],
                                                    Jornada = int(x[2]),
                                                    Local = x[3],
                                                    Visitante = x[4],
                                                    GolLocal = int(x[5]),
                                                    GolVisitante = int(x[6]),
                                                    Date = dt.datetime.strptime(x[7], '%d/%m/%Y').strftime('%d/%m/%Y'),
                                                    Unknown2 = x[8]))

df_interact = sqlContext.createDataFrame(DatosTratadosDF)
df_interact.registerTempTable('interactions')


# In[11]:

df_interact.show()


# ## Tercera pregunta: Calcular el número total de goles que ha marcado el Real Sporting de Gijón.

# A través de RDD

# In[51]:

from operator import add

sporting_local = datos_tratados.filter(lambda x: x[3] == 'Sporting de Gijon').map(lambda x: int(x[5])).reduce(add)
sporting_visitante = datos_tratados.filter(lambda x: x[4] == 'Sporting de Gijon').map(lambda x: int(x[6])).reduce(add)

sporting_local + sporting_visitante


# Resolución del apartado 3 a través de DataFrame

# In[214]:

como_local = df_interact.select('Local', 'GolLocal').filter(df_interact.Local == 'Sporting de Gijon')
como_local_acum = como_local.groupBy('Local').sum()

como_visitante = df_interact.select('Visitante', 'GolVisitante').filter(df_interact.Visitante == 'Sporting de Gijon')
como_visitante_acum = como_visitante.groupBy('Visitante').sum()

como_local_acum.take(1)[0][1] + como_visitante_acum.take(1)[0][1]


# ## Cuarta pregunta: ¿ En que temporada se marcaron más goles?

# In[44]:

mas_goles = df_interact.select('Temporada', 'GolLocal', 'GolVisitante')
mas_goles = mas_goles.withColumn('Total Goles', mas_goles.GolLocal + mas_goles.GolVisitante)
mas_goles.groupBy('Temporada').sum().sort(desc('sum(Total Goles)')).show(1)


# ## Quinta pregunta: ¿Cúal es el equipo que tiene el record de más goles como local? ¿ y cómo visitante?

# Record como local

# In[179]:

record_goles_local = df_interact.select('Local', 'GolLocal')
record_goles_local.groupBy('Local').sum().sort(desc('sum(GolLocal)')).show(5)


# Record como visitante

# In[180]:

record_goles_vis = df_interact.select('Visitante', 'GolVisitante')
record_goles_vis.groupBy('Visitante').sum().sort(desc('sum(GolVisitante)')).show(5)


# ## Sexta pregunta: ¿Cúales son las 3 décadas en las que más goles se metieron?

# In[156]:

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf

def getDecade(x):
    res = (x // 10) * 10
    return res

split_date = split(df_interact.Date, '/')

#Creamos la columna del año
df_interact_date_split = df_interact.withColumn('Year', split_date.getItem(2)).select('GolLocal', 'GolVisitante', 'Year')

#Cambiamos el tipo de la columna a INT
df_interact_date_split = df_interact_date_split.withColumn('Year', df_interact_date_split.Year.cast('int'))

#Creamos nuevo DF con el total de goles por fila
df_dt_decades = df_interact_date_split                .withColumn('Total_Goles', df_interact_date_split.GolLocal + df_interact_date_split.GolVisitante)

#funcion definida por el usuario
conv_to_decade = udf(getDecade, IntegerType())

#Aplicamos la funcion a la columna Year y la guardamos en Decade
df_dt_decades = df_dt_decades.withColumn('Decade',conv_to_decade(df_dt_decades['Year'])).select('Decade', 'Total_Goles')


df_dt_decades.groupBy('Decade').sum().sort(desc('sum(Total_Goles)'))            .select('Decade', col('sum(Total_Goles)').alias('Goles totales'))            .show()


# ##  Séptima pregunta: ¿Cúal es la media de victorias de los equipos que han estado menos de 10 temporadas en primera división?

# Primero vamos a obtener las medias siendo Local, para ello nos definimos una UDF que pasaremos a las columnas GolLocal y GolVisitante con el objetivo de determinar si ganó (1) o perdió(0), además de un contador de 'apariciones' que servirá para filtrar aquellos que han aparecido menos de 10 veces

# In[137]:

import time

def getVictory(x, y):
    res = 0
    #Si el primero que le pasemos 
    #ha metido mas goles que el segundo
    if x > y:
        res = 1
        return res
    else:
        return res

#Registramos la funcion udf
get_victory = udf(getVictory, IntegerType())

#Preparamos el DataFrame completo
media_victorias = df_interact.select('Jornada', 'GolLocal', 'GolVisitante', 'Temporada', 'Local', 'Visitante', 'Temporada')
media_victorias = media_victorias.withColumn('LocalWon', get_victory(media_victorias.GolLocal, media_victorias.GolVisitante))
media_victorias = media_victorias.withColumn('VisWon', get_victory(media_victorias.GolVisitante, media_victorias.GolLocal))


# In[140]:

#Obtenemos por parte del equipo Local
t0 = time.time()

mv_filter_local = media_victorias.select('Local', 'Temporada', 'LocalWon')                                 .withColumn('AparicionesTotales', lit(1))                                 .groupBy(['Local', 'Temporada'])                                 .sum()                                 .withColumn('Por_temporada', lit(1))                                 .groupBy('Local')                                 .sum()                                 .sort('Local')

mv_filter_local.filter(mv_filter_local['sum(Por_temporada)'] < 10)                .withColumn('Media_victorias', mv_filter_local['sum(sum(LocalWon))'] / mv_filter_local['sum(sum(AparicionesTotales))'])                .select('Local', col('sum(Por_temporada)').alias('Numero de apariciones totales'), 'Media_victorias')                .show()

print('Tiempo total de ejecución: {}'.format(time.time() - t0))


# Ahora vamos a obtener las medias por Visitante aplicando lo que hemos hecho previamente

# In[141]:

#Obtenemos por parte del equipo Local
t0 = time.time()

mv_filter_vis = media_victorias.select('Visitante', 'Temporada', 'VisWon')                                 .withColumn('AparicionesTotales', lit(1))                                 .groupBy(['Visitante', 'Temporada'])                                 .sum()                                 .withColumn('Por_temporada', lit(1))                                 .groupBy('Visitante')                                 .sum()                                 .sort('Visitante')

mv_filter_vis.filter(mv_filter_vis['sum(Por_temporada)'] < 10)                .withColumn('Media_victorias', mv_filter_vis['sum(sum(VisWon))'] / mv_filter_vis['sum(sum(AparicionesTotales))'])                .select('Visitante', col('sum(Por_temporada)').alias('Numero de apariciones totales'), 'Media_victorias')                .show()
            
print('Tiempo total de ejecución: {}'.format(time.time() - t0))


# ##  Octava pregunta: ¿Cúal es la media de goles como visitante por partido?

# Para esta pregunta vamos a comparar el numero de goles que marca cada visitante en cada partido con el total de goles que ha marcado ese equipo en todo el dataset como visitante.

# In[159]:

media_goles_visitante = df_interact.select('GolVisitante', 'Visitante')
media_goles_visitante = media_goles_visitante.groupBy('Visitante')            .agg(sum('GolVisitante').alias('Goles_totales'))
        
goles_por_partido = df_interact.select('GolVisitante', 'Visitante')


goles_por_partido.join(media_goles_visitante, 'Visitante')                    .withColumn('Media_partido', goles_por_partido.GolVisitante / media_goles_visitante.Goles_totales)                    .show()

