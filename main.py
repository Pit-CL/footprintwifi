# -*- coding: utf-8 -*-
"""Tarea BigData Analytics.

Reunir información sobre footprint wifi y en base a la información predecir.
Se recomienda instalar los requerimientos ya que son librerías que suelen
generar conflictos por sus versiones.
Además se debe tener instalado spark si se desea ejecutar en una máquina local.

Alumno: Rafael Farias.
Profesor: Oscar Peredo.
"""
import findspark
from pyspark import SparkContext, SparkConf, SQLContext
import csv
import geopandas as gpd
from pyspark.sql.functions import substring, expr
import pandas as pd
from shapely import wkt
findspark.init()  # Con este no me tira error de JVM.

# Se setea el Master y se le da nombre a la aplicación.
conf = SparkConf().setMaster("local").setAppName("Tarea Analisis de BigData")

# Se inicia el cluster Spark.
sc = SparkContext.getOrCreate(conf=conf)

# Se inicia SQLContext desde el cluster de Spark.
sqlContext = SQLContext(sc)

# TODO: Tratar de hacer un loop mas automatico para la lectura.
FilePath = '/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/data'

FileName1 = 'wifi_2017.csv'
FileName2 = 'wifi_2018.csv'
FileName3 = 'wifi_2019.csv'

FullPath1 = FilePath + '/' + FileName1
FullPath2 = FilePath + '/' + FileName2
FullPath3 = FilePath + '/' + FileName3

df_2017 = sqlContext.read.csv(FullPath1, header=True)
df_2018 = sqlContext.read.csv(FullPath2, header=True)
df_2019 = sqlContext.read.csv(FullPath3, header=True)

df_2017 = df_2017.withColumnRenamed('data_source', 'data')

df_2017 = df_2017.drop('range', 'created')

# Ahora ordeno las columnas para que todas tengan el mismo orden.
df_2017 = df_2017.select('id', 'bssid', 'lat', 'lon', 'updated', 'data')

df_unidos = ((df_2017.union(df_2018)).union(df_2019)).distinct()
print('El dataframe que contiene todos los csv es el siguiente:\n')
df_unidos.show(truncate=False)
df_unidos = df_unidos.drop('updated', 'data')

# Ahora creo el df solo con la RM según coordenadas de google maps.
df_stgo = df_unidos.filter((df_unidos.lat >= -33.65) &
                           (df_unidos.lat <= -33.28) &
                           (df_unidos.lon >= -70.81) &
                           (df_unidos.lon <= -70.50))

# Credo el df final de Stgo.
# Separo la columna bssid en una que contendrá Media_mac y otra que contendrá
# Id_fabricante.
df_stgo = df_stgo.withColumn('Id_fabricante', expr('substring(bssid,1,length(bssid)-6)')).withColumn(
    'Media_mac', expr('substring(bssid,7,length(bssid)-6)')).drop('bssid')
print('El dataframe de Santiago es el siguiente:\n')
df_stgo.show()

# Ahora trabajo con el archivo de texto.
dict_vendor_id = dict()

for lig in open('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/data/'
                'oui.txt'):
    if 'base 16' in lig:
        num, sep, txt = lig.strip().partition('(base 16)')
        dict_vendor_id[num.strip()] = txt.strip()

# Transformo el diccionario en csv para mejor manipulación.
with open('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/oui.csv',
          'w') as f:
    w = csv.writer(f)
    w.writerows(dict_vendor_id.items())

# Creo el df
df_oui = sqlContext.read.csv('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/'
                             'sprint1/oui.csv',
                             header=False)
print('El dataframe del archivo OUI.txt es el siguiente:\n')
df_oui.show(truncate=False)

# Ahora me preocupo de revisar los archivos de geolocalización.
# En el Archivo Manzana_Precensal.shp se encuentra toda la info solicitada
# salvo por la ciudad.
# TODO: Falta agregar ciudad y ver si puedo trabajar solo con stgo.
Manzana_Precensal = gpd.read_file('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/'
                                  'sprint1/data/'
                                  'Manzana_Precensal.shp')

# Elimino columnas innecesarias.
Manzana_Precensal = Manzana_Precensal.drop(['DES_REGI', 'MANZENT', 'COMUNA',
                                            'PROVINCIA', 'DES_PROV', 'REGION',
                                            'COD_DIS'], axis=1)
# https://sparkbyexamples.com/pyspark/convert-pandas-to-pyspark-dataframe/
print('El shape que contiene los datos solicitados es Manzana Precensal:\n')
print(Manzana_Precensal)

# Construcción de futures.
# Fabricante.
# Uno los df_stgo y df_oui a través de un join y además le solicito que lo haga
# en donde Id_fabricante sea idéntico a _c0 del df_oui.
# TODO: Preguntarle al profe si es necesaria la función.
df_stgo = df_stgo.join(df_oui).where(df_stgo["Id_fabricante"] == df_oui["_c0"])
df_stgo = df_stgo.drop('_c0')
df_stgo = df_stgo.withColumnRenamed('_c1', 'Fabricante')
print('El dataframe con el primer future es el siguiente:\n')
df_stgo.show(truncate=False)

# Información geográfica (ciudad, comuna, zona censal, manzana)
# Necesito el df en pandas para trabajarlo.
df_manzana = pd.DataFrame(Manzana_Precensal)
# TODO: Revisar geopandas para poder ver el tema geometry con join

