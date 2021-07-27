# -*- coding: utf-8 -*-
from os import truncate
import findspark
from pyspark import SparkContext, SparkConf, SQLContext
import csv
import geopandas as gpd
from pyspark.sql.functions import expr, countDistinct, lit
from shapely import wkt
findspark.init()  # Con este no me tira error de JVM.

# Naming the Master and de app.
conf = SparkConf().setMaster("local").setAppName("Tarea Analisis de BigData")

# Starting Spark Cluster.
sc = SparkContext.getOrCreate(conf=conf)

# Starting SqlContext from sc.
sqlContext = SQLContext(sc)

# I/O
# We can use only 2019, because it is contain all data from past years.
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

# Sorting columns.
df_2017 = df_2017.select('id', 'bssid', 'lat', 'lon', 'updated', 'data')

print('==============')
print('Sprint 1')
print('==============')
df_unidos = ((df_2017.union(df_2018)).union(df_2019)).distinct()
print('El dataframe que contiene todos los csv es el siguiente:\n')
df_unidos.show(truncate=False)
df_unidos = df_unidos.drop('updated', 'data')


# Df for Santiago only.
def solo_santiago(df_unidos):
    """It's get only Santiago city from dataset.

    Args:
        df_unidos (Spark dataframe): It's contain all data.

    Returns:
        Spark Dataframe: Spark dataframe that only contain Santiago.
    """
    f1_fabricante = df_unidos.filter((df_unidos.lat >= -33.65) &
                                     (df_unidos.lat <= -33.28) &
                                     (df_unidos.lon >= -70.81) &
                                     (df_unidos.lon <= -70.50))
    return f1_fabricante


f1_fabricante = solo_santiago(df_unidos)


# Final df of Stgo.
# Dividing column bssid into Media_mac and Id_fabricante.
def mac_y_fabricante(f1_fabricante):
    """It's create two new columns with Id_fabricante and Media_mac.

    Args:
        f1_fabricante (Spark dataframe): Spark dataframe that only contain
        Santiago.

    Returns:
        Spark dataframe: Spark dataframe than contain in two separated columns
        the Id_fabricante and Media_mac
    """
    f1_fabricante = f1_fabricante.\
        withColumn('Id_fabricante',
                   expr('substring(bssid,1,length(bssid)-6)'))\
        .withColumn('Media_mac', expr('substring(bssid,7,length(bssid)-6)')).\
        drop('bssid')

    print('El dataframe de Santiago es el siguiente:\n')
    f1_fabricante.show()
    return f1_fabricante


f1_fabricante = mac_y_fabricante(f1_fabricante)

# Working with text file.
dict_vendor_id = dict()

for lig in open('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/data/'
                'oui.txt'):
    if 'base 16' in lig:
        num, sep, txt = lig.strip().partition('(base 16)')
        dict_vendor_id[num.strip()] = txt.strip()

# Creating a csv file for better manipulation.
with open('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/oui.csv',
          'w') as f:
    w = csv.writer(f)
    w.writerows(dict_vendor_id.items())

# Create text file's df
df_oui = sqlContext.read.csv('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/'
                             'sprint1/oui.csv',
                             header=False)
print('El dataframe del archivo OUI.txt es el siguiente:\n')
df_oui.show(truncate=False)

# Opening shape file.
Manzana_Precensal = gpd.read_file('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/'
                                  'sprint1/data/'
                                  'Manzana_Precensal.shp')

# Drop unnecessary columns.
Manzana_Precensal = Manzana_Precensal.drop(['DES_REGI', 'MANZENT', 'COMUNA',
                                            'PROVINCIA', 'DES_PROV', 'REGION',
                                            'COD_DIS'], axis=1)

print('El shape que contiene los datos solicitados es Manzana Precensal:\n')
print(Manzana_Precensal)

print('==============')
print('Sprint 2')
print('==============')


# Making futures.
# Fabricante.
# Join the df_stgo and df_oui through a join function and also make where
# manufacturer_id is identical to _c0 of the df_oui.
def future_georef(sqlContext, f1_fabricante, df_oui, Manzana_Precensal):
    """It's get the geo future.

    Args:
        sqlContext (context): Pyspark environment
        f1_fabricante (Spark dataframe): Spark dataframe that only contain
        Santiago.
        df_oui (Dataframe): It's contain the Id_fabricante and the name.
        Manzana_Precensal (Shape file): Georeference dataframe.

    Returns:
        Spark dataframe: Spark dataframe that contain the geo future.
    """
    f1_fabricante = f1_fabricante.join(df_oui).\
        where(f1_fabricante["Id_fabricante"] == df_oui["_c0"])

    f1_fabricante = f1_fabricante.drop('_c0')

    f1_fabricante = f1_fabricante.withColumnRenamed('_c1', 'Fabricante')
    print('El dataframe con el primer future es el siguiente:\n')
    f1_fabricante.show(truncate=False)

    # Tranform df_stgo to pandas dataframe to work with geopandas.
    df_stgo_pandas = f1_fabricante.toPandas()

    # Creating a geopandas to indicate lat and lon points.
    df_stgo_geop = gpd.GeoDataFrame(df_stgo_pandas,
                                    geometry=gpd.points_from_xy
                                    (df_stgo_pandas.lon, df_stgo_pandas.lat))

    df_stgo_geop = df_stgo_geop.drop(columns=['lat', 'lon'])

    # CRS.
    df_stgo_geop.crs = 'EPSG:4674'

    # Joining with geopandas  df_stgo_geop and Manzana_Precensal.
    join_stgo_manzana = gpd.sjoin(df_stgo_geop, Manzana_Precensal, op='within',
                                  how='inner')

    join_stgo_manzana = join_stgo_manzana.drop(columns=['index_right'])
    join_stgo_manzana['str_geom'] = join_stgo_manzana.geometry.\
        apply(lambda x: wkt.dumps(x))
    join_stgo_manzana = (join_stgo_manzana.drop(columns=['geometry'])
                         ).rename(columns={'str_geom': 'geometry',
                                           'COD_ZON': 'Zona_Censal',
                                           'COD_ENT': 'Manzana_Censal',
                                           'DES_COMU': 'Comuna'})

    # Converting to pyspark.
    f1_georeferencia = sqlContext.createDataFrame(join_stgo_manzana)

    print('Dataset con el nuevo future de información geográfica agregado\n')
    f1_georeferencia.show(truncate=False)
    return f1_georeferencia


df_union2 = future_georef(sqlContext, f1_fabricante, df_oui,
                          Manzana_Precensal)


# Reviewing the numbers of different makers.
# df_temp1 = f1_georeferencia.select(countDistinct("Fabricante"))
# df_temp1.show()

# Reviewing null values.
# from pyspark.sql.functions import isnan, when, count, col
# fab_info_geo.select([count(when(isnan(c), c)).alias(c) for c in fab_info_geo.
#                      columns]).show()

# Counting the occurrence of every maker.
# f1_georeferencia.groupBy('Fabricante').count().orderBy('count',
#                                                        ascending=False)\
#                                                       .show(truncate=False)

# With the info above I made the decision to add the first 3 manufacturers
# as futures.Step to pandas to apply the apply function and lambda function
# and put a 1 where it finds the manufacturer's name and 0 in other cases.
def lamba_rellenar(sqlContext, f1_georeferencia):
    """It's get the quantity and proportion of all wifi makers plus the above
    future.

    Args:
        sqlContext (context): Pyspark environment.
        f1_georeferencia (Spark dataframe): Spark dataframe that contain the
        geo future.

    Returns:
        Spark dataframe: Spark dataframe that contain the geo future plus the
        above future.
    """
    f1_georeferencia = f1_georeferencia.toPandas()

    f1_georeferencia['q_ARRIS_Group'] = f1_georeferencia.\
        apply(lambda x: 1 if (x["Fabricante"]) ==
              'ARRIS Group, Inc.' else 0, axis=1)

    f1_georeferencia['q_Cisco_Systems_Inc'] = f1_georeferencia.\
        apply(lambda x: 1 if (x["Fabricante"]) ==
              'Cisco Systems, Inc' else 0, axis=1)

    f1_georeferencia['q_Technicolor'] = f1_georeferencia.\
        apply(lambda x: 1 if (x["Fabricante"]) ==
              'Technicolor CH USA Inc.' else 0, axis=1)

    # Suming.
    suma = f1_georeferencia['q_ARRIS_Group'].sum() +\
        f1_georeferencia['q_Cisco_Systems_Inc'].sum() +\
        f1_georeferencia['q_Technicolor'].sum()

    # Proportion.
    f1_georeferencia['p_ARRIS_Group'] = f1_georeferencia.\
        apply(lambda x: 1/suma if (x["Fabricante"]) ==
              'ARRIS Group, Inc.' else 0, axis=1)

    f1_georeferencia['p_Cisco_Systems_Inc'] = f1_georeferencia.\
        apply(lambda x: 1/suma if (x["Fabricante"]) ==
              'Cisco Systems, Inc' else 0, axis=1)

    f1_georeferencia['p_Technicolor'] = f1_georeferencia.\
        apply(lambda x: 1/suma if (x["Fabricante"]) ==
              'Technicolor CH USA Inc.' else 0, axis=1)

    f2_sum_prop = sqlContext.createDataFrame(f1_georeferencia)
    print('El df resultante que incluye los futures del sprint dos es:\n')
    f2_sum_prop.show(truncate=False)
    return f2_sum_prop


lamba_rellenar(sqlContext, df_union2)

print('==============')
print('Sprint 3')
print('==============')

# Year 2018.
print('==============')
print('Steps for 2018')
print('==============')
df_2018 = df_2018.drop('updated', 'data')
df_2018 = solo_santiago(df_2018)
f1_fab_2018 = mac_y_fabricante(df_2018)
f1_geo_2018 = future_georef(sqlContext, f1_fab_2018, df_oui, Manzana_Precensal)
f2_2018 = lamba_rellenar(sqlContext, f1_geo_2018).distinct()
f2_2018 = f2_2018\
    .withColumnRenamed('q_Arris_Group', 'q2018_Arris_Group')\
    .withColumnRenamed('q_Cisco_Systems_Inc', 'q2018_Cisco_Systems_Inc')\
    .withColumnRenamed('q_Technicolor', 'q2018_Technicolor')\
    .withColumnRenamed('p_ARRIS_Group', 'p2018_ARRIS_Group')\
    .withColumnRenamed('p_Cisco_Systems_Inc', 'p2018_Cisco_Systems_Inc')\
    .withColumnRenamed('p_Technicolor', 'p2018_Technicolor')

# Year 2019.
print('==============')
print('Steps for 2019')
print('==============')
df_2019 = df_2019.drop('updated', 'data')
df_2019 = solo_santiago(df_2019)
f1_fab_2019 = mac_y_fabricante(df_2019)
f1_geo_2019 = future_georef(sqlContext, f1_fab_2019, df_oui, Manzana_Precensal)
f2_2019 = lamba_rellenar(sqlContext, f1_geo_2019).distinct()
f2_2019 = f2_2019\
    .withColumnRenamed('q_Arris_Group', 'q2019_Arris_Group')\
    .withColumnRenamed('q_Cisco_Systems_Inc', 'q2019_Cisco_Systems_Inc')\
    .withColumnRenamed('q_Technicolor', 'q2019_Technicolor')\
    .withColumnRenamed('p_ARRIS_Group', 'p2019_ARRIS_Group')\
    .withColumnRenamed('p_Cisco_Systems_Inc', 'p2019_Cisco_Systems_Inc')\
    .withColumnRenamed('p_Technicolor', 'p2019_Technicolor')

# Final dataframes for years 2018 and 2019.
print('Final 2018 dataframe after drop duplicates:\n')
f2_2018.show(truncate=False)
print('Final 2019 dataframe after drop duplicates:\n')
f2_2019.show(truncate=False)


def differences(sqlContext, f2_2018, f2_2019):
    """It's create the future for differences between 2018 and 2019 regarding
    quantity and proportion.

    Args:
        sqlContext (context): Pyspark environment.
        f2_2018 (Spark dataframe): It's contain all 2018 with above futures
        f2_2019 (Spark dataframe): It's contain all 2019 with above futures.

    Returns:
    Spark dataframe: Spark dataframe that contain futures before differences
    between years.
    """
    # Now i have to add the new futures.
    # f2_2019.subtract(f2_2018) gets the difference of f2_2018
    # from f2_2019. So the rows that are present in f2_2019
    # but not present in f2_2018 will be returned
    in_2019_not_2018 = f2_2019.subtract(f2_2018)
    print('This df contain the difference between 2018 and 2019 years')
    in_2019_not_2018.show()

    # Create the missing columns in both df's using lit function.
    for column in [column for column in f2_2018.
                   columns if column not in in_2019_not_2018.columns]:
        in_2019_not_2018 = in_2019_not_2018.withColumn(column, lit(None))

    for column in [column for column in in_2019_not_2018.
                   columns if column not in f2_2018.columns]:
        f2_2018 = f2_2018.withColumn(column, lit(None))

    # Create a new df with 2018 plus all 2019 that doesn't exist in 2018.
    df_union3 = in_2019_not_2018.unionByName(f2_2018)
    df_union3 = df_union3.na.fill(0)
    print('This df contains all data to process the differences')
    df_union3.show(truncate=False)

    # Suming 2018 q's.
    df_union3 = df_union3.toPandas()
    suma2 = df_union3['q2018_Arris_Group'].sum() +\
        df_union3['q2018_Cisco_Systems_Inc'].sum() +\
        df_union3['q2018_Technicolor'].sum()

    # Apply the correct formula to get de p value for 2018.
    df_union3['p2018_ARRIS_Group'] = df_union3.\
        apply(lambda x: 1/suma2 if (x["q2018_Arris_Group"]) == 1 else 0,
              axis=1)

    df_union3['p2018_Cisco_Systems_Inc'] = df_union3.\
        apply(lambda x: 1/suma2 if (x["q2018_Cisco_Systems_Inc"]) == 1 else 0,
              axis=1)

    df_union3['p2018_Technicolor'] = df_union3.\
        apply(lambda x: 1 /
              suma2 if (x["q2018_Technicolor"]) == 1 else 0, axis=1)

    # Suming 2019 q's.
    suma3 = df_union3['q2019_Arris_Group'].sum() +\
        df_union3['q2019_Cisco_Systems_Inc'].sum() +\
        df_union3['q2019_Technicolor'].sum()

    # Apply the correct formula to get de p value for 2019.
    df_union3['p2019_ARRIS_Group'] = df_union3.\
        apply(lambda x: 1/suma3 if (x["q2019_Arris_Group"]) == 1 else 0,
              axis=1)

    df_union3['p2019_Cisco_Systems_Inc'] = df_union3.\
        apply(lambda x: 1/suma3 if (x["q2019_Cisco_Systems_Inc"]) == 1 else 0,
              axis=1)

    df_union3['p2019_Technicolor'] = df_union3.\
        apply(lambda x: 1 /
              suma3 if (x["q2019_Technicolor"]) == 1 else 0, axis=1)

    # I create now the new columns indicating the differences between q.
    df_union3['difq_ARRIS_Group'] = df_union3['q2019_Arris_Group']\
        - df_union3['q2018_Arris_Group']

    df_union3['difq_Cisco_Systems_Inc'] = df_union3['q2019_Cisco_Systems_Inc']\
        - df_union3['q2018_Cisco_Systems_Inc']

    df_union3['difq_Technicolor'] = df_union3['q2019_Technicolor']\
        - df_union3['q2018_Technicolor']

    # I create now the new columns indicating the differences between p.
    df_union3['difp_ARRIS_Group'] = df_union3['p2019_ARRIS_Group']\
        - df_union3['q2018_Arris_Group']

    df_union3['difp_Cisco_Systems_Inc'] = df_union3['p2019_Cisco_Systems_Inc']\
        - df_union3['q2018_Cisco_Systems_Inc']

    df_union3['difp_Technicolor'] = df_union3['p2019_Technicolor']\
        - df_union3['q2018_Technicolor']

    # Transforming to spark dataframe.
    df_union3 = sqlContext.createDataFrame(df_union3)
    print('Final df with all differences between 2018 and 2019:\n')
    df_union3.show(truncate=False, n=100)
    return df_union3


differences(sqlContext, f2_2018, f2_2019)
