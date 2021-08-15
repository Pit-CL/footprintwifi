# INFORME RESUMEN.
Alumno: **Rafael Farias.**
Profesor: **Oscar Peredo.**


## Introducción.

Este es un problema planteado por el profesor de la asignatura sin un objetivo claro, por lo que el planteamiento de las respuestas a resolver dependía de cada alumno.

En segundo punto es preciso aclarar que en el código se encuentra cada función utilizada explicada en detalle, por lo que este informe es sólo un complemente simple y muy resumido de todo lo indicado en el código, por lo que se recomienda guiarse por el [código.](https://github.com/Rfariaspoblete/footprintwifi/blob/main/main.py)

Las conclusiones son bastante básicas ya que el ejercicio no busca obtener respuesta alguna del modelo utilizado, simplemente entregar un conjunto de datos a un modelo para obtener un resultado cualquiera sea este.

Como conclusión principal se pueden indicar que PYSPARK es una herramienta bastante potente para exploración y limpieza de datos, sin embargo en su parte de aplicar modelos de Machine Learning carecen de la simpleza y de la amplia literatura en su aplicación comparado con su utilización en PANDAS.

A pesar de que pyspark debiese haber sido más rápido en temas bigdata, no se obtuvieron los resultados esperados, ya que todo era más rápido, simple y fácil trabajando en pandas.  Incluso al final del ejercicio al momento de querer ejecutar XGboost , el modelo siempre reclamó por falta de memoria,  a pesar de que se modificaron varios parámetros en el spark-submit. Es preciso aclara que sólo se utilizó pandas para utilizar el dataframe en el modelo de predicción y en algunas funciones lambdas.

Dado que existe una nueva librería nombrada como [KOALAS](https://docs.databricks.com/languages/koalas.html) que trabaja con una adopción bastante similar a PANDAS es que no se recomienda la utilización de pyspark, salvo que sea extremadamente necesario.


## Sprint 2

Es preciso mencionar que el sprint 1 fue dado por el profesor, en donde indica que hay que bajar ciertos archivos en distintos formatos.

Para el sprint 2 se tiene que trabajar los archivos que vienen en diferentes formatos, el principal desafío fue poder obtener la data necesaria desde el archivo oui.txt que contiene los nombres y códigos de cada fabricante de hotpost. Para el resto de los archivos se utiliza PYSPARK y GEOPANDAS.

La idea de este sprint es obtener un dataframe spark que contenga las cantidades de hotspot de los fabricantes, diferencia promedio entre un período y el siguiente;  al mismo tiempo que su código, nombre y punto geográfico.


El df que contiene el nombre del fabricante es el siguiente:
![Fabricante](https://paper-attachments.dropbox.com/s_AC07C0A7F1F5752FBA703741F29DC6B9C70179F9AC0AD118046E81A926798E35_1629052580817_image.png)


El df que contiene la información geográfica es el siguiente:
![Ubicación geográfica.](https://paper-attachments.dropbox.com/s_AC07C0A7F1F5752FBA703741F29DC6B9C70179F9AC0AD118046E81A926798E35_1629052595952_image.png)


El df que contiene las diferencias es el siguiente:
![Diferencias.](https://paper-attachments.dropbox.com/s_AC07C0A7F1F5752FBA703741F29DC6B9C70179F9AC0AD118046E81A926798E35_1629052606434_image.png)


El df final lo pueden encontrar en la salida del código en el archivo [out.txt](https://github.com/Rfariaspoblete/footprintwifi/blob/main/out.txt)


## Sprint 3

Para el sprint 3 se necesitan dos períodos de tiempo que en este caso corresponde a la diferencia entre el año 2018 y 2019, para esto se construyen los spark dataframes de ambos años con todas los features del sprint anterior.

Luego a través de fórmulas aritméticas que pueden revisadas en el [código](https://github.com/Rfariaspoblete/footprintwifi/blob/main/main.py) se obtienen las diferencias entre estos períodos de tiempo.

Los resultados pueden ser observados en el archivo [out.txt](https://github.com/Rfariaspoblete/footprintwifi/blob/main/out.txt)

Luego de haber obtenido estas diferencias, se procede a escalar e indexar aquellas variables en donde es necesario realizarlo y se obtiene el siguiente dataframe  con  el siguiente esquema.

El esquema del df final del sprint 3 es el siquiente:
![Esquema final sprint 3](https://paper-attachments.dropbox.com/s_AC07C0A7F1F5752FBA703741F29DC6B9C70179F9AC0AD118046E81A926798E35_1629052987692_image.png)



## Sprint 4

Para el sprint 4 se tuvo principalmente problemas de memoria al ejecutar XGboost y GBT que al parecer spark no maneja de la mejor manera ya que es un error bastante conocido. Dado este problema, el dataframe spark fue transformado a pandas y se le realizo LightGBM. Los resultados son los esperados, es decir un accuracy muy alto, debido principalmente a que la data está muy desbalanceada, impidiendo que el modelo pueda clasificar de buena manera datos nuevos.

El dataframe que ingresó al modelo LightGBM es el siguiente

![Dataframe que ingresa al modelo](https://paper-attachments.dropbox.com/s_AC07C0A7F1F5752FBA703741F29DC6B9C70179F9AC0AD118046E81A926798E35_1629053225355_image.png)


Y los resultados obtenidos del modelo LightGBM son los siguientes:
![Resultado final del modelo LightGBM.](https://paper-attachments.dropbox.com/s_AC07C0A7F1F5752FBA703741F29DC6B9C70179F9AC0AD118046E81A926798E35_1629053251321_image.png)






