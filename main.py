# -*- coding: utf-8 -*-
"""Tarea BigData Analytics.

Reunir información sobre footprint wifi y en base a la información predecir
por ejemplo que Fabricante se instalará en una cierta zona.
Se recomienda instalar los requerimientos en un entorno aislado ya que son
librerías que suelen generar conflictos por sus versiones.

Debes descargar desde https://www.mylnikov.org/download los últimos archivos
del año 2017, 2018 y 2019. Luego debes nombrar los tres archivos como
wifi_XXXX.csv en donde XXXX corresponde al año , debes descargar el archivo
out.txt desde http://standards-oui.ieee.org/oui/oui.txt

Dentro del script no olvides indicar la ruta al path que contiene los archivos
que descargaste, la variable dentro del script está nombrada como FilePath.

Todos estos archivos deben estar guardados en la misma carpeta.
Por último recuerda que si lo corres en tu máquina local debes tener habilitado
Spark.

Alumno: Rafael Farias.
Profesor: Oscar Peredo.
"""
import findspark
from pyspark import SparkContext, SparkConf, SQLContext
import csv
import geopandas as gpd
from pyspark.sql.functions import substring, expr, countDistinct
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
f1_fabricante = df_unidos.filter((df_unidos.lat >= -33.65) &
                                 (df_unidos.lat <= -33.28) &
                                 (df_unidos.lon >= -70.81) &
                                 (df_unidos.lon <= -70.50))

# Credo el df final de Stgo.
# Separo la columna bssid en una que contendrá Media_mac y otra que contendrá
# Id_fabricante.
f1_fabricante = f1_fabricante.\
    withColumn('Id_fabricante', expr('substring(bssid,1,length(bssid)-6)'))\
    .withColumn('Media_mac', expr('substring(bssid,7,length(bssid)-6)')).\
    drop('bssid')

print('El dataframe de Santiago es el siguiente:\n')
f1_fabricante.show()

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
Manzana_Precensal = gpd.read_file('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/'
                                  'sprint1/data/'
                                  'Manzana_Precensal.shp')

# Elimino columnas innecesarias.
Manzana_Precensal = Manzana_Precensal.drop(['DES_REGI', 'MANZENT', 'COMUNA',
                                            'PROVINCIA', 'DES_PROV', 'REGION',
                                            'COD_DIS'], axis=1)

print('El shape que contiene los datos solicitados es Manzana Precensal:\n')
print(Manzana_Precensal)

# Construcción de futures.
# Fabricante.
# Uno los df_stgo y df_oui a través de un join y además le solicito que lo haga
# en donde Id_fabricante sea idéntico a _c0 del df_oui.
f1_fabricante = f1_fabricante.join(df_oui).\
    where(f1_fabricante["Id_fabricante"] == df_oui["_c0"])

f1_fabricante = f1_fabricante.drop('_c0')

f1_fabricante = f1_fabricante.withColumnRenamed('_c1', 'Fabricante')
print('El dataframe con el primer future es el siguiente:\n')
f1_fabricante.show(truncate=False)

# Información geográfica (ciudad, comuna, zona censal, manzana)
# Transformo df_stgo a pandas para poder trabajarlo con geopandas
df_stgo_pandas = f1_fabricante.toPandas()

# Ahora creo un geopandas para indicarle los points de lat y lon.
df_stgo_geop = gpd.GeoDataFrame(df_stgo_pandas,
                                geometry=gpd.points_from_xy
                                (df_stgo_pandas.lon, df_stgo_pandas.lat))

df_stgo_geop = df_stgo_geop.drop(columns=['lat', 'lon'])

# Le indico el CRS para que quede igual a Manzana_Precensal.
df_stgo_geop.crs = 'EPSG:4674'

# Ahora hago un join con geopandas entre df_stgo_geop y Manzana_Precensal.
join_stgo_manzana = gpd.sjoin(df_stgo_geop, Manzana_Precensal, op='within',
                              how='inner')

join_stgo_manzana = join_stgo_manzana.drop(columns=['index_right'])
join_stgo_manzana['str_geom'] = join_stgo_manzana.geometry.apply(lambda x: wkt.
                                                                 dumps(x))
join_stgo_manzana = (join_stgo_manzana.drop(columns=['geometry'])
                     ).rename(columns={'str_geom': 'geometry',
                                       'COD_ZON': 'Zona_Censal',
                                       'COD_ENT': 'Manzana_Censal',
                                       'DES_COMU': 'Comuna'})

# Ahora lo vuelvo a pasar a pyspark.
f1_georeferencia = sqlContext.createDataFrame(join_stgo_manzana)

print('Dataset con el nuevo future de información geográfica agregado\n')
f1_georeferencia.show(truncate=False)

# Cuento el número de fabricantes distintos.
df_temp1 = f1_georeferencia.select(countDistinct("Fabricante"))
df_temp1.show()

# Reviso si es que hay valores nulos.
# from pyspark.sql.functions import isnan, when, count, col
# fab_info_geo.select([count(when(isnan(c), c)).alias(c) for c in fab_info_geo.
#                      columns]).show()

# Cuento las ocurrencias de cada fabricante.
f1_georeferencia.groupBy('Fabricante').count().orderBy('count', 
                                                       ascending=False)\
                                                      .show(truncate=False)

# Con la info de arriba tomo la decisión de agregar los 3 primeros fabricantes
# como futures.
# Paso a pandas para aplicar apply y lambda y poner un 1 donde encuentra el
# nombre del fabricante y 0 en otros casos.
f1_georeferencia = f1_georeferencia.toPandas()

f1_georeferencia['q_ARRIS_Group'] = f1_georeferencia.\
    apply(lambda x: 1 if (x["Fabricante"]) == 'ARRIS Group, Inc.' else 0,
          axis=1)

f1_georeferencia['q_Cisco_Systems_Inc'] = f1_georeferencia.\
    apply(lambda x: 1 if (x["Fabricante"]) == 'Cisco Systems, Inc' else 0,
          axis=1)

f1_georeferencia['q_Technicolor'] = f1_georeferencia.\
    apply(lambda x: 1 if (x["Fabricante"]) == 'Technicolor CH USA Inc.' else 0,
          axis=1)

suma = f1_georeferencia['q_ARRIS_Group'].sum() +\
    f1_georeferencia['q_Cisco_Systems_Inc'].sum() +\
    f1_georeferencia['q_Technicolor'].sum()

# Generar lo otros futures que corresponden a la proporción.
f1_georeferencia['p_ARRIS_Group'] = f1_georeferencia.\
    apply(lambda x: 1/suma if (x["Fabricante"]) == 'ARRIS Group, Inc.' else 0,
          axis=1)

f1_georeferencia['p_Cisco_Systems_Inc'] = f1_georeferencia.\
    apply(lambda x: 1/suma if (x["Fabricante"]) == 'Cisco Systems, Inc' else 0,
          axis=1)

f1_georeferencia['p_Technicolor'] = f1_georeferencia.\
    apply(lambda x: 1/suma if (x["Fabricante"]) == 'Technicolor CH USA Inc.'
          else 0, axis=1)

f2_sum_prop = sqlContext.createDataFrame(f1_georeferencia)
print('El df resultante que incluye los futures del sprint dos es:\n')
f2_sum_prop.show()

# f2_sum_prop.groupBy('Comuna').count().orderBy('count', ascending=False)\
#                                                       .show(truncate=False)
