# DETECTOR-DE-NOVEDADES

### `README.md`

Este archivo README contiene la documentación del proyecto. Proporciona una visión general del proyecto, incluyendo su propósito, características, instrucciones de instalación, pautas de uso y cualquier otra información relevante que ayude a los usuarios a entender y utilizar el proyecto de manera efectiva.

### Contenidos

1. **Visión General del Proyecto**: Una breve descripción del proyecto, sus objetivos y sus principales características.
2. **Instrucciones de Instalación**: Instrucciones paso a paso sobre cómo instalar y configurar el proyecto.
3. **Pautas de Uso**: Información sobre cómo usar el proyecto, incluyendo ejemplos y casos de uso comunes.
4. **Contribuciones**: Pautas para contribuir al proyecto, incluyendo cómo reportar problemas y enviar solicitudes de extracción.
5. **Licencia**: Información sobre la licencia del proyecto y cualquier consideración legal.
6. **Información de Contacto**: Detalles sobre cómo ponerse en contacto con los mantenedores del proyecto para soporte o consultas.

## Visión General
DETECTOR-DE-NOVEDADES es un proyecto diseñado para gestionar y predecir datos de consumo de MIPS. Incluye funcionalidades para crear y actualizar tablas de bases de datos, obtener e insertar datos, y generar pronósticos utilizando el modelo Prophet.

## Estructura del Proyecto

```
DETECTOR-DE-NOVEDADES/
    ├── .env 
    ├── .gitignore 
    ├── database_tools/ 
        │ ├── init.py 
        │ ├── pycache/ 
        │ ├── connections.py 
        │ ├── create_tables.py 
        │ ├── delete_tables.py 
        │ └── update_tables.py 
    ├── forecast_tools/ 
        │ ├── init.py 
        │ ├── pycache/ 
        │ └── metrics.py
    ├── scripts/ 
        | ├── pycache/ 
        | ├── forecasting.py 
        | └── insertingdata.py
    ├── main.py 
    ├── pruebas.py 
    ├── README.md 
    └── requirements.txt 

```

## Archivos y Directorios

### `.env`
Contiene variables de entorno para el proyecto.

### `.gitignore`
Especifica archivos y directorios que deben ser ignorados por Git.

### `database_tools/`
Contiene scripts para gestionar conexiones y operaciones de bases de datos.

- **`connections.py`**: Gestiona las conexiones a la base de datos.
- **`create_tables.py`**: Contiene la función [`create_tables`](database_tools/create_tables.py) para crear las tablas necesarias en la base de datos.
- **`delete_tables.py`**: Contiene la función [`delete_tables`](database_tools/delete_tables.py) para eliminar tablas de la base de datos.
- **`update_tables.py`**: Contiene funciones para actualizar varias tablas en la base de datos.

### `forecast_tools/`
Contiene herramientas para la previsión.

- **`metrics.py`**: Contiene funciones para calcular varias métricas de previsión.

### `scripts/`
Contiene scripts para la previsión e inserción de datos.

- **`forecasting.py`**: Contiene la función [`forecast_and_insert`](scripts/forecasting.py) para prever e insertar datos en la base de datos.
- **`insertingdata.py`**: Contiene funciones para verificar si existen tablas, obtener nuevos datos y actualizar la base de datos.

### `main.py`
El punto de entrada principal del proyecto. Conecta a la base de datos, obtiene nuevos datos, actualiza la base de datos, etiqueta consumos atípicos e imprime los datos etiquetados. La función principal llama a varias otras funciones:
- [`check_tables_exist`](scripts/insertingdata.py)
- [`fetch_new_data`](scripts/insertingdata.py)
- [`update_database`](scripts/insertingdata.py)
- [`predictions_orchestrator`](scripts/forecasting.py)
- [`label_atypical_consumptions`](database_tools/update_tables.py)

### `pruebas.py`
Contiene scripts de prueba para el proyecto.

### `requirements.txt`
Lista las dependencias necesarias para el proyecto.

## Uso

### Configuración
1. Clona el repositorio.
2. Crea un entorno virtual y actívalo.
3. Instala las dependencias:
    ```sh
    pip install -r requirements.txt
    ```
4. Configura las variables de entorno en el archivo `.env`.

### Ejecutar el Proyecto
Para ejecutar el proyecto, ejecuta el archivo `main.py`:
```sh
python [main.py](http://_vscodecontentref_/#%7B%22uri%22%3A%7B%22%24mid%22%3A1%2C%22path%22%3A%22%2Fmnt%2Fc%2FUsers%2Fegonzalez570%2FOneDrive_Grupo_exito.com%2FEscritorio%2FDetector-de-Novedades%2Fmain.py%22%2C%22scheme%22%3A%22vscode-remote%22%2C%22authority%22%3A%22wsl%2BUbuntu-22.04%22%7D%7D)
```
Funciones
database_tools/create_tables.py
create_tables(conn): Crea las tablas necesarias en la base de datos e inserta datos iniciales.
scripts/insertingdata.py
check_tables_exist(conn): Verifica si existen tablas en la base de datos y las crea si no existen.
fetch_new_data(conn_insert, conn_fetch): Obtiene nuevos datos de la base de datos.
update_database(conn, df): Actualiza la base de datos con el DataFrame proporcionado.
scripts/forecasting.py
forecast_and_insert(max_id_fecha, conn, engine): Prevé e inserta datos en la base de datos.
Contribuciones
¡Las contribuciones son bienvenidas! Por favor, haz un fork del repositorio y envía una solicitud de extracción.

Licencia
Este proyecto está licenciado bajo la Licencia MIT.