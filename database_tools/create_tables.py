""""DETECTOR-DE-NOVEDADES/database_tools/create_tables.py"""
def create_tables(conn):
    """
    Creates the necessary tables for the database and inserts initial data.

    The following tables are created:
    - Atipicos: Stores atypical categories.
    - Procesos: Stores process information.
    - Grupos: Stores group information.
    - ProcesosGrupos: Stores the relationship between processes and groups.
    - Fechas: Stores unique dates.
    - DiaSemana: Stores days of the week.
    - ConsumosMIPS: Stores MIPS consumption data.
    - PrediccionesMIPS: Stores MIPS prediction data.
    - MetricasPredicciones: Stores prediction metrics data.
    - CategoriasMetricas: Stores metric categories.

    Initial data is inserted into the Atipicos and DiaSemana tables.

    Raises:
        Any exceptions raised by the database connection or cursor operations.
    """
    print("Creating tables...")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE Atipicos (
        IdAtipico INT PRIMARY KEY,
        Categoria NVARCHAR(50)
    )
    """)

    cursor.execute("""
    CREATE TABLE CategoriasMetricas (
        IdCategoriaMetrica INT PRIMARY KEY,
        Categoria NVARCHAR(50)
    )
    """)

    cursor.execute("""
    CREATE TABLE Procesos (
        IdProceso INT PRIMARY KEY,
        NombreProceso NVARCHAR(100)
    )
    """)

    cursor.execute("""
    CREATE TABLE Grupos (
        IdGrupo INT PRIMARY KEY,
        NombreGrupo NVARCHAR(100)
    )
    """)

    cursor.execute("""
    CREATE SEQUENCE proceso_grupo_seq
    START WITH 1
    INCREMENT BY 1;
    """)

    cursor.execute("""
    CREATE TABLE ProcesosGrupos (
        IdProcesoGrupo INT PRIMARY KEY,
        IdProceso INT,
        IdGrupo INT,
        FOREIGN KEY (IdProceso) REFERENCES Procesos(IdProceso),
        FOREIGN KEY (IdGrupo) REFERENCES Grupos(IdGrupo)
    )
    """)

    cursor.execute("""
    CREATE TABLE Fechas (
        IdFecha INT PRIMARY KEY,
        Fecha DATE UNIQUE
    )
    """)

    cursor.execute("""
    CREATE TABLE DiaSemana (
        IdDiaSemana INT PRIMARY KEY,
        DiaSemana NVARCHAR(50)
    )
    """)

    cursor.execute("""
    CREATE TABLE ConsumosMIPS (
        IdConsumo INT PRIMARY KEY,
        IdProceso INT,
        IdGrupo INT,
        IdFecha INT,
        IdDiaSemana INT,
        IdAtipico INT,
        ConsumoMIPS FLOAT,
        FOREIGN KEY (IdProceso) REFERENCES Procesos(IdProceso),
        FOREIGN KEY (IdGrupo) REFERENCES Grupos(IdGrupo),
        FOREIGN KEY (IdFecha) REFERENCES Fechas(IdFecha),
        FOREIGN KEY (IdDiaSemana) REFERENCES DiaSemana(IdDiaSemana),
        FOREIGN KEY (IdAtipico) REFERENCES Atipicos(IdAtipico)
    )
    """)

    cursor.execute("""
    CREATE SEQUENCE predicciones_seq
    START WITH 1
    INCREMENT BY 1;
    """)

    cursor.execute("""
    CREATE TABLE PrediccionesMIPS (
        IdPrediccion INT PRIMARY KEY,
        IdFecha INT,
        IdDiaSemana INT,
        Prediccion FLOAT,
        LimInf FLOAT,
        LimSup FLOAT,
        FOREIGN KEY (IdFecha) REFERENCES Fechas(IdFecha),
        FOREIGN KEY (IdDiaSemana) REFERENCES DiaSemana(IdDiaSemana)
    )
    """)

    cursor.execute("""
    CREATE SEQUENCE metricas_seq
    START WITH 1
    INCREMENT BY 1;
    """)

    cursor.execute("""
    CREATE TABLE MetricasPredicciones (
        IdMetrica INT PRIMARY KEY,
        IdFecha INT,
        IdCategoriaMetrica INT,
        MAE FLOAT,
        MSE FLOAT,
        RMSE FLOAT,
        MAPE FLOAT,
        sMAPE FLOAT,
        FOREIGN KEY (IdFecha) REFERENCES Fechas(IdFecha),
        FOREIGN KEY (IdCategoriaMetrica) REFERENCES CategoriasMetricas(IdCategoriaMetrica)
    )
    """)

    cursor.execute("""
    INSERT INTO Atipicos (IdAtipico, Categoria) VALUES
    (-1, 'Inferior'),
    (0, 'NoAtipico'),
    (1, 'Superior')
    """)

    cursor.execute("""
    INSERT INTO DiaSemana (IdDiaSemana, DiaSemana) VALUES
    (1, 'Lunes'),
    (2, 'Martes'),
    (3, 'Miércoles'),
    (4, 'Jueves'),
    (5, 'Viernes'),
    (6, 'Sábado'),
    (7, 'Domingo')
    """)

    cursor.execute("""
    INSERT INTO CategoriasMetricas (IdCategoriaMetrica, Categoria) VALUES
    (0, 'Mensual'),
    (1, 'Historica')
    """)

    conn.commit()
    cursor.close()
    print("Tables created successfully.")
