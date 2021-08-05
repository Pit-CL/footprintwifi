# -*- coding: utf-8 -*-
from os import truncate
import findspark
from pyspark import SparkContext, SparkConf, SQLContext
import csv
import geopandas as gpd
from pyspark.sql.functions import expr, lit, udf
from shapely import wkt
from pyspark.ml.feature import MinMaxScaler, VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
import lightgbm as lgb

findspark.init()  # Con este no me tira error de JVM.


def context():
    """Environment context.
    """

    # Naming the Master and de app.
    conf = SparkConf().setMaster('local').setAppName('Tarea Analisis de'
                                                     'BigData')

    # Starting Spark Cluster.
    sc = SparkContext.getOrCreate(conf=conf)

    # Starting SqlContext from sc.
    sqlContext = SQLContext(sc)
    return sqlContext


sqlContext = context()

FilePath = '/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/data'


def open_files(sqlContext, FilePath):
    """Opening files.

    Args:
        sqlContext (Context): Pyspark environment.
        FilePath (String): Path where the files are.

    Returns:
        Spark dataframe: Spark dataframes of 2017, 2018 and 2019.
    """
    FileName1 = 'wifi_2017.csv'
    FileName2 = 'wifi_2018.csv'
    FileName3 = 'wifi_2019.csv'

    FullPath1 = FilePath + '/' + FileName1
    FullPath2 = FilePath + '/' + FileName2
    FullPath3 = FilePath + '/' + FileName3

    df_2017 = sqlContext.read.csv(FullPath1, header=True)
    df_2018 = sqlContext.read.csv(FullPath2, header=True)
    df_2019 = sqlContext.read.csv(FullPath3, header=True)
    return df_2017, df_2018, df_2019


df_2017, df_2018, df_2019 = open_files(sqlContext, FilePath)


def clean_2017(df_2017):
    """Cleaning 2017.

    Args:
        df_2017 (Spark dataframe): Spark dataframe than contain 2017.
    """
    df_2017 = df_2017.withColumnRenamed('data_source', 'data')

    df_2017 = df_2017.drop('range', 'created')

# Sorting columns.
    df_2017 = df_2017.select('id', 'bssid', 'lat', 'lon', 'updated', 'data')
    return df_2017


df_2017 = clean_2017(df_2017)

print('==============')
print('Sprint 1')
print('==============')


def union_original_df(df_2017, df_2018, df_2019):
    """Applying union to all df's.

    Args:
        df_2017 (Spark dataframe): It's contain data regarding 2017.
        df_2018 (Spark dataframe): It's contain data regarding 2018.
        df_2019 (Spark dataframe): It's contain data regarding 2019.

    Returns:
        Spark dataframe: Union between above df's.
    """
    df_unidos = ((df_2017.union(df_2018)).union(df_2019)).distinct()
    print('All csv files dataframe:\n')
    df_unidos.show(truncate=False)
    df_unidos = df_unidos.drop('updated', 'data')
    return df_unidos


df_unidos = union_original_df(df_2017, df_2018, df_2019)


def santiago_only(df_unidos):
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


f1_fabricante = santiago_only(df_unidos)


def mac_maker(f1_fabricante):
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

    print('Santiago´s dataframe:\n')
    f1_fabricante.show()
    return f1_fabricante


f1_fabricante = mac_maker(f1_fabricante)

# Full path that contain oui.txt
oui_path = '/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/data/oui.txt'


def dict_maker_id():
    """It's generate a dictionary that contains the id and the makers names.

    Returns:
        Dictionary: It's contain the id and the makers name.
    """
    dict_vendor_id = dict()

    for lig in open(oui_path):
        if 'base 16' in lig:
            num, sep, txt = lig.strip().partition('(base 16)')
            dict_vendor_id[num.strip()] = txt.strip()
    return dict_vendor_id


dict_vendor_id = dict_maker_id()

# Creating a csv file that contain the Id_Fabricante and Media_mac
csv_path = '/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/oui.csv'


def to_csv(dict_vendor_id):
    """Converting dictionary to csv.

    Args:
        dict_vendor_id (Dictionary): It's contain the vendor id and the name.
    """
    with open(csv_path, 'w') as f:
        w = csv.writer(f)
        w.writerows(dict_vendor_id.items())


to_csv(dict_vendor_id)


def oui_dataframe(sqlContext):
    """It's create the oui spark dataframe

    Args:
        sqlContext (context): Pyspark environment.

    Returns:
        Spark dataframe: It's contain the id and the name of the makers.
    """
    df_oui = sqlContext.read.csv(csv_path, header=False)
    print('OUI.txt´s dataframe:\n')
    df_oui.show(truncate=False)
    return df_oui


df_oui = oui_dataframe(sqlContext)

# Full path for shape file Manzana Precensal.
shape_path = ('/home/rafa/Dropbox/Linux_MDS/BDAnalytics/sprint1/data/'
              'Manzana_Precensal.shp')


def shape_file():
    """ Open and clean.
    """
    Manzana_Precensal = gpd.read_file(shape_path)

    # Drop unnecessary columns.
    Manzana_Precensal = Manzana_Precensal.drop(['DES_REGI', 'MANZENT',
                                                'COMUNA',
                                                'PROVINCIA', 'DES_PROV',
                                                'REGION', 'COD_DIS'], axis=1)

    print('Shape file')
    print(Manzana_Precensal)
    return Manzana_Precensal


Manzana_Precensal = shape_file()

print('==============')
print('Sprint 2')
print('==============')


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

    # Join the df_stgo and df_oui through a join function and also make where
    # manufacturer_id is identical to _c0 of the df_oui.
    f1_fabricante = f1_fabricante.join(df_oui).\
        where(f1_fabricante["Id_fabricante"] == df_oui["_c0"])

    f1_fabricante = f1_fabricante.drop('_c0')

    f1_fabricante = f1_fabricante.withColumnRenamed('_c1', 'Fabricante')
    print('First future dataframe:\n')
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

    print('Dataframe with geo features\n')
    f1_georeferencia.show(truncate=False)
    return f1_georeferencia


df_union2 = future_georef(sqlContext, f1_fabricante, df_oui,
                          Manzana_Precensal)


def quantity_proportion(sqlContext, f1_georeferencia):
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
    print('Final df for Sprint 2:\n')
    f2_sum_prop.show(truncate=False)
    return f2_sum_prop


quantity_proportion(sqlContext, df_union2)

print('==============')
print('Sprint 3')
print('==============')

# Year 2018.
print('==============')
print('Steps for 2018')
print('==============')


def eighteen(sqlContext, df_2018, solo_santiago, mac_y_fabricante, df_oui,
             Manzana_Precensal, future_georef, lamba_rellenar):
    """Get the features for sprint 2.

    Args:
        sqlContext (Context): Pyspark environment.
        df_2018 (Spark dataframe): It's contain 2018 info.
        solo_santiago (Function): It's create a df for Santiago only.
        mac_y_fabricante (Function): It's create two new columns with
        Id_fabricante and Media_mac.
        df_oui (Spark dataframe): It's contain the id and name of the makers.
        Manzana_Precensal (Geopandas): It's contain the info of the shapefile.
        future_georef (Function): It's create the future with the geo.
        lamba_rellenar (Function): It's get the quantity and proportion of
        all wifi makers plus the above future.

    Returns:
        Spark dataframe: It's contain all features for sprint 2.
    """
    df_2018 = df_2018.drop('updated', 'data')
    df_2018 = solo_santiago(df_2018)
    f1_fab_2018 = mac_y_fabricante(df_2018)
    f1_geo_2018 = future_georef(
        sqlContext, f1_fab_2018, df_oui, Manzana_Precensal)
    f2_2018 = lamba_rellenar(sqlContext, f1_geo_2018).distinct()
    f2_2018 = f2_2018\
        .withColumnRenamed('q_Arris_Group', 'q2018_Arris_Group')\
        .withColumnRenamed('q_Cisco_Systems_Inc', 'q2018_Cisco_Systems_Inc')\
        .withColumnRenamed('q_Technicolor', 'q2018_Technicolor')\
        .withColumnRenamed('p_ARRIS_Group', 'p2018_ARRIS_Group')\
        .withColumnRenamed('p_Cisco_Systems_Inc', 'p2018_Cisco_Systems_Inc')\
        .withColumnRenamed('p_Technicolor', 'p2018_Technicolor')

    return f2_2018


f2_2018 = eighteen(sqlContext, df_2018, santiago_only, mac_maker,
                   df_oui, Manzana_Precensal, future_georef,
                   quantity_proportion)

# Year 2019.
print('==============')
print('Steps for 2019')
print('==============')


def nineteen(sqlContext, df_2019, solo_santiago, mac_y_fabricante, df_oui,
             Manzana_Precensal, future_georef, lamba_rellenar, f2_2018):
    """Get the features for sprint 2.

    Args:
        sqlContext (Context): Pyspark environment.
        df_2019 (Spark dataframe): It's contain 2019 info.
        solo_santiago (Function): It's create a df for Santiago only.
        mac_y_fabricante (Function): It's create two new columns with
        Id_fabricante and Media_mac.
        df_oui (Spark dataframe): It's contain the id and name of the makers.
        Manzana_Precensal (Geopandas): It's contain the info of the shapefile.
        future_georef (Function): It's create the future with the geo.
        lamba_rellenar (Function): It's get the quantity and proportion of
        all wifi makers plus the above future.

    Returns:
        Spark dataframe: It's contain all features for sprint 2.
    """
    df_2019 = df_2019.drop('updated', 'data')
    df_2019 = solo_santiago(df_2019)
    f1_fab_2019 = mac_y_fabricante(df_2019)
    f1_geo_2019 = future_georef(
        sqlContext, f1_fab_2019, df_oui, Manzana_Precensal)
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
    return f2_2019


f2_2019 = nineteen(sqlContext, df_2019, santiago_only, mac_maker,
                   df_oui, Manzana_Precensal, future_georef,
                   quantity_proportion,
                   f2_2018)


def differences(sqlContext, f2_2018, f2_2019):
    """It's create the future for differences between 2018 and 2019 regarding
    quantity and proportion.

    Args:
        sqlContext (context): Pyspark environment.
        f2_2018 (Spark dataframe): It's contain all 2018 with above features
        f2_2019 (Spark dataframe): It's contain all 2019 with above features.

    Returns:
    Spark dataframe: Spark dataframe that contain features before differences
    between years.
    """

    # f2_2019.subtract(f2_2018) gets the difference of f2_2018
    # from f2_2019. So the rows that are present in f2_2019
    # but not present in f2_2018 will be returned
    in_2019_not_2018 = f2_2019.subtract(f2_2018)
    print('This df contain the row differences between 2018 and 2019 years')
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

    # Filling all 2019.
    df_union3['q2019_Arris_Group'] = df_union3.\
        apply(lambda x: 1 if (x["Fabricante"]) == 'ARRIS Group, Inc.' else 0,
              axis=1)

    df_union3['q2019_Cisco_Systems_Inc'] = df_union3.\
        apply(lambda x: 1 if (x["Fabricante"]) == 'Cisco Systems, Inc' else 0,
              axis=1)

    df_union3['q2019_Technicolor'] = df_union3.\
        apply(lambda x: 1 if (x["Fabricante"]) ==
              'Technicolor CH USA Inc.' else 0, axis=1)

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
    df_union3.show(truncate=False, n=20)
    return df_union3


differences(sqlContext, f2_2018, f2_2019)


# Creating at least 10 new features.
# UDF for converting column type from vector to double type
df_to_scale = differences(sqlContext, f2_2018, f2_2019)
unlist = udf(lambda x: round(float(list(x)[0]), 4), DoubleType())

# Iterating over columns to be scaled
for i in ['q2019_Arris_Group', 'q2019_Cisco_Systems_Inc',
          'q2019_Technicolor', 'p2019_ARRIS_Group',
          'p2019_Cisco_Systems_Inc', 'p2019_Technicolor',
          'q2018_Arris_Group', 'q2018_Cisco_Systems_Inc',
          'q2018_Technicolor', 'p2018_ARRIS_Group',
          'p2018_Cisco_Systems_Inc', 'p2018_Technicolor',
          'difq_ARRIS_Group', 'difq_Cisco_Systems_Inc',
          'difq_Technicolor', 'difp_ARRIS_Group',
          'difp_Cisco_Systems_Inc', 'difp_Technicolor']:

    # VectorAssembler Transformation - Converting column to vector type
    assembler = VectorAssembler(inputCols=[i], outputCol=i+'_Vect')

    # MinMaxScaler Transformation
    scaler = MinMaxScaler(inputCol=i+'_Vect', outputCol=i+'_Scaled')

    # Pipeline of VectorAssembler and MinMaxScaler
    pipeline = Pipeline(stages=[assembler, scaler])

    # Fitting pipeline on dataframe
    df_to_scale = pipeline.fit(df_to_scale).transform(df_to_scale)\
        .withColumn(i+'_Scaled', unlist(i+'_Scaled')).drop(i+'_Vect')

print('Final df Sprint3 after Scaling :')
df_to_scale.show(n=20)

# Converting categorical data to string indexed format.
indexer = StringIndexer(inputCols=('Comuna', 'geometry', 'Id_fabricante'),
                        outputCols=('ComunaIndex', 'geoIndex', 'FabIndex'))
unlist = udf(lambda x: round(float(list(x)[0]), 4), DoubleType())
df_indexed = indexer.fit(df_to_scale).transform(df_to_scale)

# Iterating over columns to be scaled
for i in ['ComunaIndex', 'geoIndex', 'FabIndex', 'Zona_Censal',
          'Manzana_Censal']:
    # VectorAssembler Transformation - Converting column to vector type
    assembler = VectorAssembler(inputCols=[i], outputCol=i+'_Vect')

    # MinMaxScaler Transformation
    scaler = MinMaxScaler(inputCol=i+'_Vect', outputCol=i+'_Scaled')

    # Pipeline of VectorAssembler and MinMaxScaler
    pipeline = Pipeline(stages=[assembler, scaler])

    # Fitting pipeline on dataframe
    df_indexed = pipeline.fit(df_indexed).transform(df_indexed)\
        .withColumn(i+'_Scaled', unlist(i+'_Scaled')).drop(i+'_Vect')
print('Df  after Indexing strings and all data scaled :')
df_indexed.show()

# Sort the DF to work in predictions.
final_df = df_indexed.select('q2019_Arris_Group_Scaled',
                             'q2019_Cisco_Systems_Inc_Scaled',
                             'q2019_Technicolor_Scaled',
                             'p2019_ARRIS_Group_Scaled',
                             'p2019_Cisco_Systems_Inc_Scaled',
                             'p2019_Technicolor_Scaled',
                             'q2018_Arris_Group_Scaled',
                             'q2018_Cisco_Systems_Inc_Scaled',
                             'q2018_Technicolor_Scaled',
                             'p2018_ARRIS_Group_Scaled',
                             'p2018_Cisco_Systems_Inc_Scaled',
                             'p2018_Technicolor_Scaled',
                             'difq_ARRIS_Group_Scaled',
                             'difq_Cisco_Systems_Inc_Scaled',
                             'difq_Technicolor_Scaled',
                             'difp_ARRIS_Group_Scaled',
                             'difp_Cisco_Systems_Inc_Scaled',
                             'difp_Technicolor_Scaled',
                             'ComunaIndex_Scaled',
                             'geoIndex_Scaled',
                             'FabIndex_Scaled',
                             'Zona_Censal_Scaled',
                             'Manzana_Censal_Scaled')

print('Final tableu to use with LightGBM')
final_df.show()

# Applying LightGBM
# Dependant variable = 'difq_Cisco_Systems_Inc_Scaled'


def LightGBM(final_df):
    """Applying LightGBM.

    Args:
        final_df (Spark Dataframe): Final Tableu.
    """
    final_df = final_df.toPandas()

    X = final_df[['q2019_Arris_Group_Scaled',
                  'q2019_Cisco_Systems_Inc_Scaled',
                  'q2019_Technicolor_Scaled',
                  'p2019_ARRIS_Group_Scaled',
                  'p2019_Cisco_Systems_Inc_Scaled',
                  'p2019_Technicolor_Scaled',
                  'q2018_Arris_Group_Scaled',
                  'q2018_Cisco_Systems_Inc_Scaled',
                  'q2018_Technicolor_Scaled',
                  'p2018_ARRIS_Group_Scaled',
                  'p2018_Cisco_Systems_Inc_Scaled',
                  'p2018_Technicolor_Scaled',
                  'difq_Technicolor_Scaled',
                  'difp_ARRIS_Group_Scaled',
                  'difp_Cisco_Systems_Inc_Scaled',
                  'difp_Technicolor_Scaled',
                  'difq_ARRIS_Group_Scaled',
                  'ComunaIndex_Scaled',
                  'geoIndex_Scaled',
                  'FabIndex_Scaled',
                  'Zona_Censal_Scaled',
                  'Manzana_Censal_Scaled']]

    y = final_df['difq_Cisco_Systems_Inc_Scaled']

    X_train, X_test, y_train, y_test = train_test_split(X, y,
                                                        test_size=0.2,
                                                        random_state=0)

    # Build the lightgbm model
    clf = lgb.LGBMClassifier()
    clf.fit(X_train, y_train)

    # Predict the results
    y_pred = clf.predict(X_test)
    Sum = sum(y_pred)
    print('Wifi hotspot difference for next Year', Sum)
    print('The predictions are', y_pred)

    # View accuracy
    print('LightGBM Model accuracy score: {0:0.4f}'.format(accuracy_score
                                                           (y_test, y_pred)))


LightGBM(final_df)
