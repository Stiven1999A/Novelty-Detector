**ESTADÍSTICAS DEL CONSUMO DE MIPS.**

**Área:** DevSecOps.

**Analista Ágil:** Estiben Andrey González Londoño.

**Dominio:** Gestion de MIPS.

**Proyecto:** Detección de Anomalías y Predicciones del Consumo de MIPS.

**Palabras Clave:** MIPS, Anomalías, Atípicos, Novedades, Predicción, PROPHET, Pruebas.

**Infraestructura de despliegue:** OKS.

**Sistemas Origen:** SQL Server; WLMDB_TEMPO.

**Sistemas Destino:** SQL Server; Consumos-PrediccionesMIPS.

**Tipo desarrollo:** Modelo.

**Versión Lenguaje:** Python.

**URL Consumo:** 

Tabla de contenido

- [Descripción de la necesidad](#descripción-de-la-necesidad)
- [Diagrama de la necesidad](#diagrama-de-la-necesidad)
- [Clasificacion de las Interfaces](#clasificacion-de-las-interfaces)
- [Atributos de calidad de la solucion](#atributos-de-calidad-de-la-solucion)
- [Diagrama de componentes de la Interfaz](#diagrama-de-componentes-de-la-interfaz)
- [Consideraciones](#consideraciones)
- [Mapeo de datos](#mapeo-de-datos)
  - [Mapeo Movil_Exito_Bolsillo](#mapeo_movil_exito_bolsillo)
  - [Mapeo Movil_Exito_Gestores](#mapeo_movil_exito_gestores)
  - [Mapeo Movil_Exito_Tipoajuste](#mapeo_movil_exito_tipoajuste)
- [Características técnicas de la Interfaz](#características-técnicas-de-la-interfaz)
- [Manejo de Errores](#manejo-de-errores)
- [Manejo de reproceso](#manejo-de-reproceso)
- [Manual de despliegue](#manual-de-despliegue)

* [Inventario de Artefactos](#inventario-de-artefactos)
* [Topologías](#topologías)
* [Directorios](#directorios)
* [Operaciones de la Interfaz (Servicio)](#Operaciones-de-la-Interfaz-(Servicio))

#### Descripción de la necesidad.

En el año 2020, el Grupo Éxito firmó un contrato con Unisys, un proveedor de tecnología, para renovar la plataforma ClearPath, que soporta el sistema SINCO (Sistema de Información Comercial), el CORE comercial de la compañía. Este contrato, con una duración de 84 meses, incluyó una asignación de 284.000 MIPS (Million Instructions Per Second), una métrica que mide el procesamiento de las operaciones que pasan por el ClearPath. Cada unidad de MIPS tiene un costo de \$95 USD, lo que sitúa el valor total del contrato en aproximadamente 26.9 millones de dólares.

Dada la magnitud de esta inversión, es fundamental para la compañía implementar una gestión rigurosa y un monitoreo constante del consumo de MIPS, además de desarrollar estrategias que mejoren la visibilidad de los procesos para prevenir consumos excesivos y lograr ahorros significativos. Según las proyecciones, la cantidad de MIPS habilitados podría agotarse en febrero de 2027, tres años antes de lo deseado.

El equipo de Infraestructura y Operaciones ha desarrollado herramientas analíticas para gestionar este consumo como tableros en Power BI y Microstrategy. Sin embargo, estos tableros tienen limitaciones. Por ejemplo, una de las tareas que surgen diariamente es el conocer qué procesos, de todos los catalogados en la base de datos (9680), tuvieron un consumo anómalo para un día particular; se tiene en especial consideración los días en los que el consumo total de MIPS supera su media de consumo diario (para el 2024 la media del consumo de MIPS es de 97 MIPS, este valor puede variar dependiendo del día de la semana). Por lo anterior, se presenta una primera necesidad de construir una herramienta que permita ejecutar este análisis de forma automática y diaria de tal forma que los tableros tengan el valor añadido de poder listar aquellos procesos con consumos anómalos, agilizando el proceso de análisis y buscando informar a tiempo las novedades a los equipos de Infraestructura y Operaciones para que ellos puedan tomar las medidas adecuadas.

Dado que el contrato con Unisys tiene un número limitado de MIPS para consumir, surgen las siguientes preguntas: teniendo en cuenta el histórico de consumo y el ritmo de consumo actual ¿cuántos MIPS se consumirán en el siguiente mes, en el siguiente semestre, en el siguiente año, ...? ¿La cantidad de MIPS habilitados hoy día es suficiente para suplir la demanda hasta la fecha esperada? En caso negativo, ¿cuál debe ser la media de consumo diario para que los recursos sean suficientes? Por lo anterior, la segunda necesidad es un modelo predictivo que tenga en cuenta las tendencias y patrones del pasado y permita a los equipos ver hacia el futuro para planificar estrategias de negocio efectivas, asegurando que la compañía mantenga un desempeño óptimo en la ejecución de su Sistema de Información Comercial (SINCO) y en el uso eficiente de sus recursos tecnológicos.

| **Nombre de la interfaz:** | ESTADÍSTICAS DEL CONSUMO DE MIPS                                                                                                                                                                                                                                                                                                                                                                                                         |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Qué**                   | Necesidad de tener tableros con información más detallada sobre el consumo de MIPS.                                                                                                                                                                                                                                                                                                                                                    |
| **Porqué**                | - Porque los análisis de novedades se hacen de manera manual, lo que consume energía y tiempo<br /> que puede ser invertido en tareas de mayor prioridad sin descuidar el monitoreo del consumo de MIPS. <br />- Porque la simple intuición y las proyecciones usando aritmética son poco fiables a la hora de predecir<br /> los consumos, especialmente si el periodo de tiempo hacía adelante que se quiere predecir es grande. |
| **Para qué**              | - Para que los equipos de Infraestructura y Operaciones tengan acceso a las novedades diarias de forma<br />entendible, simple, concisa y fiable haciendo sus operaciones más agiles.<br />- Asimismo, para que los equipos tenga una idea del comportamiento futuro de los consumos basandose<br />en un modelo de Machine Learning de confianza.                                                                                       |

#### Diagrama de la necesidad

![Diagrama](./Diagrama_Necesidad.drawio.png)

El desarrollo que se llevó a cabo en este repositorio logra solucionar la problemática expuesta anteriormente. 

#### Atributos de calidad de la solución

Estas preguntas se puedes realizar enfocadas en el proceso de negocio que se está analizando para identificar los atributos de calidad:

1. ¿Con qué frecuencia se generan los mensajes o registros en un día?
2. ¿Qué cantidad de registros o mensajes se generan?
3. ¿Cuál es el crecimiento esperado para la generación de estos registros o mensajes?
4. ¿Cuánto tiempo se necesita para ejecutar el proceso?
5. ¿Cuál es el tiempo en que se espera tener la información?
6. ¿Cómo se comporta la información en un evento especial? En promociones, un día, hora en particular?

En la siguiente tabla se relacionan los atributos de calidad asociados a la solución:


| **Seguridad**                                                       |                        |  |
| ------------------------------------------------------------------------- | ---------------------- | - |
| **Característica**                                                 | **Observación** |  |
| Identificación y Autenticación                                          |                        |  |
| Autorización                                                             |                        |  |
| Confidencialidad                                                          |                        |  |
| Integridad                                                                |                        |  |
| Auditabilidad                                                             |                        |  |
| **Desempeño**                                                      |                        |  |
| **Característica**                                                 | **Observación** |  |
| Transacciones por Segundo                                                 |                        |  |
| Tiempo de Respuesta Máximo en Segundos                                   |                        |  |
| Tiempo de Respuesta Promedio en Segundos                                  |                        |  |
| Frecuencia                                                                |                        |  |
| Registros entregados en orden                                             |                        |  |
| **Escalamiento**                                                    |                        |  |
| **Característica**                                                 | **Observación** |  |
| Cantidad Estimada de Transacciones por Día/Mes/Año                      |                        |  |
| Porcentaje de Crecimiento Estimado de Transacciones por Día/Mes/Año (%) |                        |  |
| **Disponibilidad**                                                  |                        |  |
| **Característica**                                                 | **Observación** |  |
| Horario de Disponibilidad de la Solución                                 |                        |  |
| Contingencia                                                              |                        |  |
| **Manejo de errores**                                               |                        |  |
| **Característica**                                                 | **Observación** |  |
| Trazabilidad                                                              |                        |  |
| Endpoint o Partition Key                                                  |                        |  |
| Errores                                                                   |                        |  |
| Alertamiento                                                              |                        |  |
| Monitoreo                                                                 |                        |  |
| Reintentos                                                                |                        |  |

#### Arquitectura de la Solución

En el siguiente diagrama de componentes se muestra el diseño de la integración y la relación con los diferentes componentes:
Diagrama ejemplo para ODI:

![Diagrama](./Arquitectura_Solucion.drawio.png)

Diagrama ejemplo para Azure:

![Diagrama](./Documentacion/General/diagrama-arquitectura-azure.png)

| **Nombre Componente**    | **Descripción del componente** | **Responsabilidad**                                                                                                       | **Tipo** | **Herramienta** |
| ------------------------------ | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | -------------- | --------------------- |
| Bolsillo                       | Tabla fuente de SIME                  | Almacena la información de SIME con respecto a los Bolsillos                                                                   | Tabla          | Microsoft SQL Server  |
| TipoBolsillo                   | Tabla fuente de SIME                  | Almacena la información de SIME con respecto a los TipoBolsillos                                                               | Tabla          | Microsoft SQL Server  |
| Gestor                         | Tabla fuente de SIME                  | Almacena la información de SIME con respecto a los Gestores                                                                    | Tabla          | Microsoft SQL Server  |
| TipoAjuste                     | Tabla fuente de SIME                  | Almacena la información de SIME con respecto a los TipoAjustes                                                                 | Tabla          | Microsoft SQL Server  |
| PKG_SIME_TERA                  | Paquete principal de ODI.             | Paquete el cual invoca la ejecución de los paquetes PKG_SIME_TERA_BOLSILLO,  PKG_SIME_TERA_GESTORES y PKG_SIME_TERA_TIPOAJUSTE | Paquete        | ODI                   |
| PKG_SIME_TERA_BOLSILLO         | Paquete secundario de ODI.            | Paquete de ODI encargado de transportar la información de los bolsillos desde la aplicación SIME hacia Teradata.              | Paquete        | ODI                   |
| PKG_SIME_TERA_GESTORES         | Paquete secundario de ODI.            | Paquete de ODI encargado de transportar la información de los gestores desde la aplicación SIME hacia Teradata.               | Paquete        | ODI                   |
| PKG_SIME_TERA_TIPOAJUSTE       | Paquete secundario de ODI.            | Paquete de ODI encargado de transportar la información de los tipos de ajuste desde la aplicación SIME hacia Teradata.        | Paquete        | ODI                   |
| DEFINED_VALUE_TYPE_NEW         | Tabla destino Teradata                | Tabla de TERADATA que almacena los datos de Bolsillos junto con los Tipos de bolsillos                                          | Tabla          | Teradata              |
| GESTORES_TELEFONIA_NEW         | Tabla destino Teradata                | Tabla de TERADATA que almacena los datos de Gestores telefonia                                                                  | Tabla          | Teradata              |
| TIPO_AJUSTE_RECARGAS_TELEFONIA | Tabla destino Teradata                | Tabla de TERADATA que almacena los datos de los ajustes de las recargas                                                         | Tabla          | Teradata              |

#### Consideraciones

    En esta sección especificar  las consideraciones relevantes de la solución a construir.

- Este diagrama no es necesario cuando se construye un servicio simple, se crearía el diagrama de secuencia.
- Este diagrama debe contemplar el reproceso de la información y el punto desde donde reenviaría la información.
- Especificar si es una cola, un servicio que se debe definir.

#### Mapeo de datos

En las siguientes tablas se relacionan el mapeo de datos entre el mensaje interno capa integración y el mensaje de los proveedores de la Interfaz:

| **Mapeo de datos**                                                   |                           |                     |                        |                                                  |
| -------------------------------------------------------------------------- | ------------------------- | ------------------- | ---------------------- | ------------------------------------------------ |
| **Nombre del componente:** escriba el nombre del componente          |                           |                     |                        |                                                  |
| **Nombre Del destino:** especifique el nombre del componente destino |                           |                     |                        |                                                  |
| **Enrutamiento:** /                                                  |                           |                     |                        |                                                  |
| **Campo Destino**                                                    | **Transformación** | **Origen**    | **Campo Origen** | **Comentarios**                            |
| nombre campo destino 1                                                     | tipo de transformación   | Tipo campo origen 1 | Nombre campo origen 1  | Especifique algun comentario releventa del campo |

#### Características técnicas de la Interfaz

Las hojas técnicas de infraestructura son relacionadas en la siguiente tabla:

| **Características Técnicas Desarrollo** |                          |                               |                |                   |                    |                              |
| ----------------------------------------------- | ------------------------ | ----------------------------- | -------------- | ----------------- | ------------------ | ---------------------------- |
| **Sistema**                               | **Tipo**           | **Host**                | **Port** | **Usuario** | **Password** | **Nombre del recurso** |
| Staging de ODI                                  | Base de datos            | OSBDLLODB                     | 1541           | ODIV12213         |                    | ODIDEV12                     |
| Servidor de aplicaciones de ODI                 | Servidor de aplicaciones | ODIDLLO                       |                |                   |                    |                              |
| SIME                                            | Base de datos            | 296SQLP03                     | 1433           | usrodidev         |                    | SIME_CUBE                    |
| TERADATA                                        | Base de datos            | exitocol6-3-2.grupo-exito.com |                | usrodidev         |                    | bd_rdb                       |

| **Características Técnicas Calidad** |                          |                               |                |                   |                    |                              |
| -------------------------------------------- | ------------------------ | ----------------------------- | -------------- | ----------------- | ------------------ | ---------------------------- |
| **Sistema**                            | **Tipo**           | **Host**                | **Port** | **Usuario** | **Password** | **Nombre del recurso** |
| Staging de ODI                               | Base de datos            | BDQA                          | 1521           | ODIV12213         |                    | ODIQASTG                     |
| Servidor de aplicaciones de ODI              | Servidor de aplicaciones | odiqa01                       |                |                   |                    |                              |
| SIME                                         | Base de datos            | 296SQLP03                     | 1433           | usrodiqa          |                    | SIME_CUBE                    |
| TERADATA                                     | Base de datos            | exitocol6-3-2.grupo-exito.com |                | usrodiqa          |                    | bd_rdb                       |

**Nota: las aplicaciones SIME y TERADATA no cuentan con ambiente de calidad.**

Esta integración esta orquestada por OPCON:

| **Parámetros OPCON**  |                                        |                  |                   |
| ---------------------------- | -------------------------------------- | ---------------- | ----------------- |
| **Nombre de la Malla** | NFORMACION-MAESTRAS-MOVIL-EXITO-A-TERA |                  |                   |
| **Tipo de Ejecución** | Secuencial                             |                  |                   |
| **Frecuencia**         | Diario - 8:00 am - 12:00 pm - 5:00 pm  |                  |                   |
| **Escenario**          | **Proyecto**                     | **Fuente** | **Destino** |
| PKG_SIME_TERA_BOLSILLO       | MOVIL EXITO                            | BOLSILLOS        | TERADATA          |
| PKG_SIME_TERA_GESTORES       | MOVIL EXITO                            | GESTORES         | TERADATA          |
| PKG_SIME_TERA_TIPOAJUSTES    | MOVIL EXITO                            | TIPO AJUSTE      | TERADATA          |

#### Manejo de Errores

|                | **Si/No** | **Cómo se realiza**                             |
| -------------- | --------------- | ------------------------------------------------------ |
| Notificaciones | Si              | A través de Opcon                                     |
| Reintentos     | Si              | Se ejecuta de nuevo el proceso manualmente desde Opcon |
| Trazabilidad   | Si              | En la tabla TBL_TRAZABILIDAD_ODI del satging de ODI.   |
| Contingencia   | No              |                                                        |

#### Manejo de reproceso

Especificar los puntos en que se debe hacer el reproceso y cómo hacerlo.

| **Punto**           | **Como se reprocesa**                                 | **Aplicabilidad** |
| ------------------------- | ----------------------------------------------------------- | ----------------------- |
| PKG_SIME_TERA_BOLSILLO    | Se debe ejecutar manualmente el paquete de ODI desde Opcon. |                         |
| PKG_SIME_TERA_GESTORES    | Se debe ejecutar manualmente el paquete de ODI desde Opcon. |                         |
| PKG_SIME_TERA_TIPOAJUSTES | Se debe ejecutar manualmente el paquete de ODI desde Opcon. |                         |

Las siguientes operaciones serán ofrecidas por el servicio:

#### Nombre de la operación

Especificar que realiza la operación.

| **Mensajes de la Operación**       |                                            |
| ----------------------------------------- | ------------------------------------------ |
| **Descripción del mensaje origen** | **Descripción del mensaje destino** |
| Especifique el mensaje origen             | Especifique el mensaje destino o respuesta |

Especifique un ejemplo de como consumir el servicio y cual seria la respuesta

## Mensajes de la interfaz

### Mensaje en el origen

La información en el origen es extraída de una tabla a continuación un pequeño ejemplo de un registro

| **UPL_X11_CONTRECEIVED** |                |
| ------------------------------ | -------------- |
| UPLRID                         | 19389488       |
| WHSEID                         | 013            |
| ADDDATE                        | 20201104110629 |
| ADDWHO                         | lpperez        |
| CONTAINERID                    | 0419001001     |
| ORDERID                        | 01460041464546 |
| ORDERTYPE                      | XPR            |
| CARRIERKEY                     |                |
| LOADID                         |                |
| STORERKEY                      | EXITO          |
| CUSTOMERKEY                    |                |
| DROPID                         |                |
| GRP1                           |                |
| GRP6                           |                |
| STOP                           |                |

### Mensaje en el destino

**Mensaje enviado a la trazabilidad - Entrada:**

Especifique el mensaje de Entrada enviada a ELK para la trazabilidad.

**Mensaje que se envía al destino RabbitMQ**

Especifique el mensaje de Entrada salida a ELK para la trazabilidad.

### Instructivo despliegue

#### Prerrequisitos

- Verificar el repositorio y asignación de políticas

| Nombre            | Ruta                                                                   | Rama   |
| ----------------- | ---------------------------------------------------------------------- | ------ |
| sce-x11-wmsupload | https://grupo-exito.visualstudio.com/GCIT-Agile/_git/sce-x11-wmsupload | Master |
|                   |                                                                        |        |

- Creación de directorios

| Servidor | Ruta                                                            | Permisos          |
| -------- | --------------------------------------------------------------- | ----------------- |
| oks-pdn  | /data1/logs/pdn-wmsupload/sce-publisher/sce-x11-wmsupload/log   | Lectura/Escritura |
| oks-pdn  | /data1/logs/pdn-wmsupload/sce-publisher/sce-x11-wmsupload/error | Lectura/Escritura |
| oks-qa   | /data1/logs/qa-wmsupload/sce-publisher/sce-x11-wmsupload/log    | Lectura/Escritura |
| oks-qa   | /data1/logs/qa-wmsupload/sce-publisher/sce-x11-wmsupload/error  | Lectura/Escritura |

- Verificar la creación y conexión de los usuarios. Se deben repisar los secrets en las variables del pipeline.

| **Usuario** | Variable Secrets | **Tipo**       | **Servidor**                                                        |
| ----------------- | ---------------- | -------------------- | ------------------------------------------------------------------------- |
| pdn_integracion   | trg_pass         | Aplicación RabbitMQ | [bossy-hedgehog.rmq.cloudamqp.com](http://bossy-hedgehog.rmq.cloudamqp.com/) |
| USRMSPRD          | src_pass         | Base de datos oracle | wmsdb-scan.grupo-exito.com                                                |

- Asignación de los valores adecuados para el micro servicio

| Namespace     | Ambiente    | MemoryLimits | CpuLimits |
| ------------- | ----------- | ------------ | --------- |
| dev-wmsupload | Desarrollo  | 400Mi        | 400m      |
| qa-wmsupload  | QA          | 400Mi        | 400m      |
| pdn-wmsupload | Producción | 400Mi        | 400m      |

- Validación de las variables y propiedades de los pipelines.

##### Pipelines

| Yaml            | Ruta             |
| --------------- | ---------------- |
| configMap.yaml  | charts\templates |
| deployment.yaml | charts\templates |
| monitoring.yaml | charts\templates |
| secrets.yaml    | charts\templates |
| Chart.yaml      | charts           |
| values-dev.yaml | charts           |
| values-qa.yaml  | charts           |
| values-pdn.yaml | charts           |

Estas son las variables que se deben modificar en cada despliegue con los datos correspondientes al ambiente donde se realiza dicho despliegue.

| Variables                         | Descripción                                           |
| --------------------------------- | ------------------------------------------------------ |
| imagePullSecrets                  | Valor del acr-auth                                     |
| secrets.trg_pass                  | Contraseña de rabbit como destino                     |
| secrets.src_pass                  | Contraseña de base de datos origen                    |
| secrets.elk_pass                  | Contraseña de rabbit para ELK                         |
| config.TARGET_RABBIT.virtual_host | Virtual Host del gestor de colas destino               |
| config.TARGET_RABBIT.username     | Usuario del gestor de colas destino                    |
| config.TARGET_RABBIT.port         | Port del gestor de colas destino                       |
| config.TARGET_RABBIT.host         | Host del gestor de colas destino                       |
| config.SOURCE_ORACLE.port         | Port base de datos origen                              |
| config.SOURCE_ORACLE.hostname     | Host de la base de datos origen                        |
| config.SOURCE_ORACLE.service_name | Service Name de la base de datos origen                |
| config.SOURCE_ORACLE.user         | Usuario de la base de datos origen                     |
| config.RABBIT_ELK.host            | Host del gestor de colas para trazabilidad ELK         |
| config.RABBIT_ELK.port            | Port del gestor de colas para trazabilidad ELK         |
| config.RABBIT_ELK.virtual_host    | Virtual Host del gestor de colas para trazabilidad ELK |
| config.RABBIT_ELK.username        | Usuario del gestor de colas para trazabilidad ELK      |

#### Inventario de Artefactos

| Tipo de artefacto | **Nombre del artefacto** | **Descripción**                                                                                  |
| ----------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------- |
| Micro servicio    | sce-x11-wmsupload              | Micro servicio desarrollado en Python ejecutado por el OKS (Azure Kubernetes Service)                   |
| Código fuente    | Fuente_Movil_Exito.zip         | Contiene los XML de los paquetes desarrollados en ODI.                                                  |
| Solución de ODI  | SOL_SLN_DLG_MOVIL_EXITO.zip    | Contiene la solución de ODI con los escenarios que se desplegarán en el ambiente de QA y Producción. |
| Diagrama          | Diseño_MOVILEXITO.png         | Diagrama de diseño de los componentes de la integración                                               |
| Diagrama          | Diagrama_necesidad.PNG         | Diagrama de necesidad de la integración                                                                |
| Script de SQL     | INSERT_ODIV12213.sql           | Inserts para ingresar en la tabla TBL_CONTROL_CARGAS                                                    |

#### Secuencia de ejecución de despliegue de Artefactos

| **Secuencia** | Tipo de artefacto | **Nombre del artefacto** | **Servidor** | **Observaciones**                                                                                          |
| ------------------- | ----------------- | ------------------------------ | ------------------ | ---------------------------------------------------------------------------------------------------------------- |
| 1                   | Micro servicio    | sce-x11-wmsupload              | OKS-PDN            | Se debe aprobar el pull request del repositorio y verificar que se ejecute el pipeline release sce-x11-wmsupload |
|                     |                   |                                |                    |                                                                                                                  |

**Nota:** los datos de conexión descritos en esta sección del manual de despliegue son del ambiente desarrollo. Para el despliegue en el ambiente de Producción se debe contactar al responsable de cada aplicación para que proporcionen los datos de conexión de este ambiente.

#### Topologías (Aplica para ODI)

| **Data Server S**IME_DLLO |                   |                              |                                          |                                          |                            | **t**                 |
| ------------------------------- | ----------------- | ---------------------------- | ---------------------------------------- | ---------------------------------------- | -------------------------- | --------------------------- |
| **Tecnología**           | **Usuario** | **Contraseña**        | **JDBC Driver**                    | **JDBC URL**                       | **Array Fetch Size** | **Batch Update Size** |
| Microsoft SQL Server            | usrodidev         |                              | weblogic.jdbc.sqlserver.SQLServerDriver  | jdbc:weblogic:sqlserver://296SQLP03:1433 | 1000                       | 1000                        |
| **Esquemas**              |                   |                              |                                          |                                          |                            |                             |
| **Base de datos**         | **Esquema** | **Esquema de trabajo** | **Propietario esquema de trabajo** | **Esquema Lógico**                |                            |                             |
| SIME_CUBE                       | dbo               | SIME_CUBE                    | dbo                                      | LS_SIME_CUBE                             |                            |                             |

| **Data Server S**IME_QA |                   |                              |                                          |                                          |                            | **t**                 |
| ----------------------------- | ----------------- | ---------------------------- | ---------------------------------------- | ---------------------------------------- | -------------------------- | --------------------------- |
| **Tecnología**         | **Usuario** | **Contraseña**        | **JDBC Driver**                    | **JDBC URL**                       | **Array Fetch Size** | **Batch Update Size** |
| Microsoft SQL Server          | usrodiqa          |                              | weblogic.jdbc.sqlserver.SQLServerDriver  | jdbc:weblogic:sqlserver://296SQLP03:1433 | 1000                       | 1000                        |
| **Esquemas**            |                   |                              |                                          |                                          |                            |                             |
| **Base de datos**       | **Esquema** | **Esquema de trabajo** | **Propietario esquema de trabajo** | **Esquema Lógico**                |                            |                             |
| SIME_CUBE                     | dbo               | SIME_CUBE                    | dbo                                      | LS_SIME_CUBE                             |                            |                             |

| **Data Server TERADATA_DLLO** |                              |                           |                             |                                                                    |                            | **t**                 |
| ----------------------------------- | ---------------------------- | ------------------------- | --------------------------- | ------------------------------------------------------------------ | -------------------------- | --------------------------- |
| **Tecnología**               | **Usuario**            | **Contraseña**     | **JDBC Driver**       | **JDBC URL**                                                 | **Array Fetch Size** | **Batch Update Size** |
| TERADATA                            | usrodidev                    |                           | com.ncr.teradata.TeraDriver | jdbc:teradata://exitocol6-3-2.grupo-exito.com/bd_rdb,CHARSET=UTF16 | 1037                       | 1037                        |
| **Esquemas**                  |                              |                           |                             |                                                                    |                            |                             |
| **Esquema**                   | **Esquema de trabajo** | **Esquema lógico** |                             |                                                                    |                            |                             |
| BD_RDB                              | BD_METADATA                  | LS_TERADATA_ENLACE        |                             |                                                                    |                            |                             |

| **Data Server TERADATA_QA** |                              |                           |                             |                                                                    |                            | **t**                 |
| --------------------------------- | ---------------------------- | ------------------------- | --------------------------- | ------------------------------------------------------------------ | -------------------------- | --------------------------- |
| **Tecnología**             | **Usuario**            | **Contraseña**     | **JDBC Driver**       | **JDBC URL**                                                 | **Array Fetch Size** | **Batch Update Size** |
| TERADATA                          | usrodiqa                     |                           | com.ncr.teradata.TeraDriver | jdbc:teradata://exitocol6-3-2.grupo-exito.com/bd_rdb,CHARSET=UTF16 | 1037                       | 1037                        |
| **Esquemas**                |                              |                           |                             |                                                                    |                            |                             |
| **Esquema**                 | **Esquema de trabajo** | **Esquema lógico** |                             |                                                                    |                            |                             |
| BD_RDB                            | BD_METADATA                  | LS_TERADATA_ENLACE        |                             |                                                                    |                            |                             |

### **Monitoreo**

#### Depuración de archivos

| **Directorio**                                            | **Si/No** | **Cómo se realiza**                             | **Frecuencia** |
| --------------------------------------------------------------- | --------------- | ------------------------------------------------------ | -------------------- |
| /data1/logs/pdn-wmsupload/sce-publisher/sce-x11-wmsupload/log   | Si              | Se borra lo anterior a 5 días, a cargo de operaciones | Diaria               |
| /data1/logs/pdn-wmsupload/sce-publisher/sce-x11-wmsupload/error | Si              | Se borra lo anterior a 5 días, a cargo de operaciones | Diaria               |
| /data1/logs/qa-wmsupload/sce-publisher/sce-x11-wmsupload/log    | Si              | Se borra lo anterior a 5 días, a cargo de operaciones | Diaria               |
| /data1/logs/qa-wmsupload/sce-publisher/sce-x11-wmsupload/error  | Si              | Se borra lo anterior a 5 días, a cargo de operaciones | Diaria               |

#### Comportamientos Atípicos de la integración

| Comportamiento                                                   | Acción                                     |
| ---------------------------------------------------------------- | ------------------------------------------- |
| Crash o loopback en el pod del micro servicio sce-x11-wmsupload. | Notificar y/o escalar al área responsable. |
| Reinicios en el pod del micro servicio sce-x11-wmsupload.        | Notificar y/o escalar al área responsable. |
| Archivos represados en la carpeta de input.                      | Notificar y/o escalar al área responsable. |

#### Caracterización de eventos

| Id Tipo de servicio (Metrica) | % Warning | % Critical | Id Tipo de umbral | Frecuencia de chequeo (Horas) | Causa del Evento                     | Notificación Alarma   | Acción Crítica                                                                                     | Escalamiento 1                 | Escalamiento 2    | Escalamiento 3      |
| ----------------------------- | --------- | ---------- | ----------------- | ----------------------------- | ------------------------------------ | ---------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------ | ----------------- | ------------------- |
| CPU                           | >=80      | >=80       | %                 | 0.5                           | Capacidad/Rendimiento                | Alerta en Monitor: CPU | Alto Uso de CPU: Notificar al disponible de aplicaciones                                             | Analista Disponible Middleware | Juan Esteban Luna | Yurani Rojas        |
| Transaccion con errores       | N/A       | N/A        | Error             | 9                             | Error en los datos                   | Error en mensaje       | Notificar al analista funcional                                                                      | Super usuario                  | Líder técnico   | Equipo Arquitectura |
| Sin mensajes                  | N/A       | N/A        |                   | 3                             | No se detecta movimiento informacion | No llegan mensajes     | Validar que el microservicio este operando con normalidad y no tenga datos represados en las fuentes | Analista Disponible Middleware | Juan Esteban Luna | Yurani Rojas        |
|                               |           |            |                   |                               |                                      |                        |                                                                                                      |                                |                   |                     |

#### Recomendaciones

1. Validar puntos de monitoreo funcional con la analista de soluciones (Jorge Albeiro Gomez).
2. Revisar monitoreo de los Logs de publicador y suscriptores, que no se encuentren en estado de Crash.
3. Realizar monitoreo diario para ver el estado de las integraciones durante las próximas dos semanas, en lo posible cada 12 horas.
4. Realizar monitoreo de los recursos del pod.
5. Remitir correo de informe del estado de los microservicios a las siguientes personas:

- Analista de soluciones (Jorge Albeiro Gomez)
- Jefe de cadena de abastecimiento (Esteban Diez)
- Arquitecto responsable de la integración (Dario Sousa)
- Jefe de arquitectura (Walter Franco)
- Analista de operaciones (Juan Marulanda)
- Equipo de desarrollo (SETI)
- Operaciones de la Interfaz (Servicio)
