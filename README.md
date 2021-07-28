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




