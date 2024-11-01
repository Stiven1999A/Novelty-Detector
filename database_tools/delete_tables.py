"""DETECTOR-DE-NOVEDADES/database/delete_tables.py"""

def delete_tables(conn):
    """
    Deletes all the tables created by the create_tables function.

    The following tables are deleted:
    - Atipicos
    - Procesos
    - Grupos
    - ProcesosGrupos
    - Fechas
    - DiaSemana
    - ConsumoMIPS
    - PrediccionesMIPS
    - MetricasPredicciones
    - CategoriasMetricas
    - The sequences proceso_grupo_seq and predicciones_seq

    Raises:
        Any exceptions raised by the database connection or cursor operations.
    """
    print("Deleting tables...")
    cursor = conn.cursor()

    cursor.execute("IF OBJECT_ID('PrediccionesMIPS', 'U') IS NOT NULL DROP TABLE PrediccionesMIPS")
    cursor.execute("IF OBJECT_ID('ConsumosMIPS', 'U') IS NOT NULL DROP TABLE ConsumosMIPS")
    cursor.execute("IF OBJECT_ID('MetricasPredicciones', 'U') IS NOT NULL DROP TABLE MetricasPredicciones")
    cursor.execute("IF OBJECT_ID('DiaSemana', 'U') IS NOT NULL DROP TABLE DiaSemana")
    cursor.execute("IF OBJECT_ID('Fechas', 'U') IS NOT NULL DROP TABLE Fechas")
    cursor.execute("IF OBJECT_ID('ProcesosGrupos', 'U') IS NOT NULL DROP TABLE ProcesosGrupos")
    cursor.execute("IF OBJECT_ID('Grupos', 'U') IS NOT NULL DROP TABLE Grupos")
    cursor.execute("IF OBJECT_ID('Procesos', 'U') IS NOT NULL DROP TABLE Procesos")
    cursor.execute("IF OBJECT_ID('Atipicos', 'U') IS NOT NULL DROP TABLE Atipicos")
    cursor.execute("IF OBJECT_ID('CategoriasMetricas', 'U') IS NOT NULL DROP TABLE CategoriasMetricas")
    cursor.execute("IF OBJECT_ID('proceso_grupo_seq', 'SO') IS NOT NULL DROP SEQUENCE proceso_grupo_seq")
    cursor.execute("IF OBJECT_ID('predicciones_seq', 'SO') IS NOT NULL DROP SEQUENCE predicciones_seq")
    cursor.execute("IF OBJECT_ID('metricas_seq', 'SO') IS NOT NULL DROP SEQUENCE metricas_seq")

    conn.commit()
    cursor.close()
    print("Tables deleted.")
