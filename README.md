# DETECTOR-DE-NOVEDADES

### `README.md`

This README file contains the documentation of the project. It provides an overview of the project, including its purpose, features, installation instructions, usage guidelines, and any other relevant information that helps users understand and utilize the project effectively.

### Contents

1. **Project Overview**: A brief description of the project, its goals, and its main features.
2. **Installation Instructions**: Step-by-step instructions on how to install and set up the project.
3. **Usage Guidelines**: Information on how to use the project, including examples and common use cases.
4. **Contributing**: Guidelines for contributing to the project, including how to report issues and submit pull requests.
5. **License**: Information about the project's license and any legal considerations.
6. **Contact Information**: Details on how to get in touch with the project maintainers for support or inquiries.

## Overview
DETECTOR-DE-NOVEDADES is a project designed to manage and forecast MIPS consumption data. It includes functionalities for creating and updating database tables, fetching and inserting data, and generating forecasts using the Prophet model.

## Project Structure

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

## Files and Directories

### `.env`
Contains environment variables for the project.

### `.gitignore`
Specifies files and directories to be ignored by Git.

### `database_tools/`
Contains scripts for managing database connections and operations.

- **`connections.py`**: Manages database connections.
- **`create_tables.py`**: Contains the [`create_tables`](database_tools/create_tables.py) function to create necessary tables in the database.
- **`delete_tables.py`**: Contains the [`delete_tables`](database_tools/delete_tables.py) function to delete tables from the database.
- **`update_tables.py`**: Contains functions to update various tables in the database.

### `forecast_tools/`
Contains tools for forecasting.

- **`metrics.py`**: Contains functions to calculate various forecasting metrics.

### `scripts/`
Contains scripts for forecasting and inserting data.

- **`forecasting.py`**: Contains the [`forecast_and_insert`](scripts/forecasting.py) function to forecast and insert data into the database.
- **`insertingdata.py`**: Contains functions to check if tables exist, fetch new data, and update the database.

### `main.py`
The main entry point of the project. It connects to the database, fetches new data, updates the database, labels atypical consumptions, and prints the labeled data. The main function calls several other functions:
- [`check_tables_exist`](scripts/insertingdata.py)
- [`fetch_new_data`](scripts/insertingdata.py)
- [`update_database`](scripts/insertingdata.py)
- [`predictions_orchestrator`](scripts/forecasting.py)
- [`label_atypical_consumptions`](database_tools/update_tables.py)

### `pruebas.py`
Contains test scripts for the project.

### `requirements.txt`
Lists the dependencies required for the project.

## Usage

### Setting Up
1. Clone the repository.
2. Create a virtual environment and activate it.
3. Install the dependencies:
    ```sh
    pip install -r requirements.txt
    ```
4. Set up the environment variables in the `.env` file.

### Running the Project
To run the project, execute the `main.py` file:
```sh
python [main.py](http://_vscodecontentref_/#%7B%22uri%22%3A%7B%22%24mid%22%3A1%2C%22path%22%3A%22%2Fmnt%2Fc%2FUsers%2Fegonzalez570%2FOneDrive_Grupo_exito.com%2FEscritorio%2FDetector-de-Novedades%2Fmain.py%22%2C%22scheme%22%3A%22vscode-remote%22%2C%22authority%22%3A%22wsl%2BUbuntu-22.04%22%7D%7D)
```
Functions
database_tools/create_tables.py
create_tables(conn): Creates necessary tables in the database and inserts initial data.
scripts/insertingdata.py
check_tables_exist(conn): Checks if tables exist in the database and creates them if they do not.
fetch_new_data(conn_insert, conn_fetch): Fetches new data from the database.
update_database(conn, df): Updates the database with the provided DataFrame.
scripts/forecasting.py
forecast_and_insert(max_id_fecha, conn, engine): Forecasts and inserts data into the database.
Contributing
Contributions are welcome! Please fork the repository and submit a pull request.

License
This project is licensed under the MIT License.