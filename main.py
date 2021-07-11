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
# Carpeta donde están guardados los archivos.
FilePath = '/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/data'

# Nombres de los archivo.
FileName1 = 'wifi_2017.csv'
FileName2 = 'wifi_2018.csv'
FileName3 = 'wifi_2019.csv'

# Combinando
FullPath1 = FilePath + '/' + FileName1
FullPath2 = FilePath + '/' + FileName2
FullPath3 = FilePath + '/' + FileName3

# Dataframes
df_2017 = sqlContext.read.csv(FullPath1, header=True)
df_2018 = sqlContext.read.csv(FullPath2, header=True)
df_2019 = sqlContext.read.csv(FullPath3, header=True)

# Mostrando los data frame
df_2017.show()
df_2018.show()
df_2019.show()

# Muestro el esquema.
df_2017.printSchema()
df_2018.printSchema()
df_2019.printSchema()


# Continuo con los csv

# Cambiando el nombre a la columna data_source por data.
df_2017 = df_2017.withColumnRenamed('data_source', 'data')

df_2017.printSchema()

df_2017.show()

# Noto que hay columnas extras en un dataset, en primera instancia las elimino.
df_2017 = df_2017.drop('range', 'created')

df_2017.printSchema()

df_2017.show()

# Ahora ordeno las columnas para que todas tengan el mismo orden.
df_2017 = df_2017.select('id', 'bssid', 'lat', 'lon', 'updated', 'data')

df_2017.printSchema()

df_2017.show()

# Ahora uno los df_2017, df_2018 y df_2019.
df_2017_2018_2019 = (df_2017.union(df_2018)).union(df_2019)
df_2017_2018_2019.show(truncate=False)

# Elimino las columnas innecesarias.
df_2017_2018_2019 = df_2017_2018_2019.drop('updated', 'data')

# TODO: Preguntarle al profe si es que debo hacer un drop o no.
# Elimino las duplicadas según bssid
# df_all = df_2017_2018_2019.dropDuplicates(['bssid'])
# print('bssid únicas : '+str(df_all.count()))
# df_all.show(truncate=False)

df_all = df_2017_2018_2019

# Ahora creo el df solo con la RM según coordenadas de google maps.
df_stgo = df_all.filter((df_all.lat >= -33.65) &
                        (df_all.lat <= -33.28) &
                        (df_all.lon >= -70.81) &
                        (df_all.lon <= -70.50))
df_stgo.show(truncate=False)

# Credo el df final de Stgo.
# Separo la columna bssid en una que contendrá Media_mac y otra que contendrá
# Id_fabricante.
df_stgo = df_stgo.withColumn('Media_mac', expr('substring(bssid,1,length(bssid)-6)')).withColumn(
    'Id_fabricante', expr('substring(bssid,7,length(bssid)-6)')).drop('bssid')
df_stgo.show()

# Ordeno el dataset por lat.
# dropDupDF.sort(desc('lat')).show()

# Ahora trabajo con el archivo de texto.
# Inicio el diccionario que contendra el id y el nombre del vendor.
result = dict()

for lig in open('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/data/'
                'oui.txt'):
    if 'base 16' in lig:
        num, sep, txt = lig.strip().partition('(base 16)')
        result[num.strip()] = txt.strip()

# Transformo el diccionario en csv para mejor manipulación.
with open('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/oui.csv',
          'w') as f:
    w = csv.writer(f)
    w.writerows(result.items())

# Creo el df
df_oui = sqlContext.read.csv('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/'
                             'sprint1/oui.csv',
                             header=False)

# Muestro el df
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
                                            'COD_DIS'],
                                           axis=1)
# https://sparkbyexamples.com/pyspark/convert-pandas-to-pyspark-dataframe/
print(Manzana_Precensal)

# Construcción de futures.
# Fabricante.
# Uno los df_stgo y df_oui a través de un join y además le solicito que lo haga
# en donde Id_fabricante sea idéntico a _c0 del df_oui.
# TODO: Preguntarle al profe si es necesaria la función.
df_stgo = df_stgo.join(df_oui).where(df_stgo["Id_fabricante"] == df_oui["_c0"])
df_stgo.show()

# Elimino la columna _c0
df_stgo = df_stgo.drop('_c0')
df_stgo.show(truncate=False)

# Información geográfica (ciudad, comuna, zona censal, manzana)
# Necesito el df en pandas para trabajarlo.
df_manzana = pd.DataFrame(Manzana_Precensal)
df_manzana.head()

# Transformo a string la columna geometry.
df_manzana['str_geom'] = df_manzana.geometry.apply(lambda x: wkt.dumps(x))
df_manzana.head()

# Le hago drop a la columna geometry.
# df_manzana.drop('geometry')

# split_data = df_manzana["str_geom"].str.split(" ")
# data = split_data.to_list()
# names = ["Capital", "State"]
# new_df = pd.DataFrame(data, columns=names)
# TODO: Tengo creado el string ahora debo separarlo para obtener 4 columnas.
