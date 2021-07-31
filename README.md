# Tarea BigData Analytics.
### Alumno: Rafael Farias.
### Profesor: Oscar Peredo.

Reunir información sobre footprint wifi y en base a la información predecir
por ejemplo, qué Fabricante se instalará.
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

Sprint 2: Datos preparados I
Crear funciones que permitan construir los siguientes features para cada red
WiFi:
Fabricante
Información geográfica (ciudad, comuna, zona censal, manzana)
Crear funciones que permitan construir los siguientes features para cada
 ciudad, comuna, zona censal o cada manzana:
Cantidad de redes WiFi del fabricante X
Proporcion de redes WiFi del fabricante X

Sprint 3: Datos preparados II
En base a las funciones creadas en el sprint 2, implementar funciones para
crear los siguientes features para cada ciudad, comuna, zona censal o manzana:
Diferencia en cantidad de redes wifi del fabricante F entre dos 
dumps YY-ZZ-XXXX y AA-BB-CCCC.
Diferencia en proporción de redes wifi del fabricante F entre dos 
dumps YY-ZZ-XXXX y AA-BB-CCCC. 
Implementar al menos 20 features adicionales, a criterio de cada grupo.

Sprint 4: Datos mapeados x algoritmo I
Implementar función que genere la variable dependiente a predecir. Se debe
definir si la predicción va a ser sólo espacial, o espacio-temporal.
Construir el “tablón” final que ingresará al algoritmo (particionando por
train/test/validación)
Entrenar el modelo usando el algoritmo Gradient Boosting Trees 
(en particular usando las implementaciones XGBoost o LightGBM),
para un set de parámetros fijo (exploratorio).

------------------------------------------------------------------------------

Gather information about wifi footprint and based on the information predict
for example, which Manufacturer will be installed.
It is recommended to install the requirements in an isolated environment as they are
libraries that usually generate conflicts for their versions.

You must download the latest files from https://www.mylnikov.org/download
of the year 2017, 2018 and 2019. Then you must name the three files as
wifi_XXXX.csv where XXXX corresponds to the year, you must download the file
out.txt from http://standards-oui.ieee.org/oui/oui.txt

Inside the script do not forget to indicate the path to the path that contains the files
that you downloaded, the variable inside the script is named FilePath.

All these files must be stored in the same folder.
Finally remember that if you run it on your local machine you must have it enabled
Spark - to sparkle.

Sprint 2: Data Ready I
Create functions to perform build the following features for each network
Wifi:
Maker
Geographic information (city, commune, census zone, block)
Create functions to perform build the following characteristics for each
 city, commune, census zone or each block:
Number of Wi-Fi networks from manufacturer X
Proportion of Wi-Fi networks of manufacturer X

Sprint 3: Data Ready II
Based on the functions created in sprint 2, implement functions to
create the following characteristics for each city, commune, census tract or block:
Difference in number of Wi-Fi networks of manufacturer F between two
YY-ZZ-XXXX and AA-BB-CCCC landfills.
Difference in proportion of Wi-Fi networks of manufacturer F between two
YY-ZZ-XXXX and AA-BB-CCCC landfills.
Implement at least 20 additional features, at the discretion of each group.

Sprint 4: Data mapped x algorithm I
Implement a function that generates the dependent variable to predict. Must be
define if the prediction is going to be only spatial, or spatio-temporal.
Build the final “board” that will enter the algorithm (partitioning by
train / test / validation)
Train the model using the Gradient Boosting Trees algorithm
(in particular using the XGBoost or LightGBM implementations),
for a fixed (exploratory) parameter set.
