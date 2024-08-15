import pandas as pd
import numpy as np
import matplotlib as plt
from pandas_profiling import ProfileReport
import sqlite3


# Conectar a la base de datos SQLite
conn = sqlite3.connect('bancolombia_panama.db')

# Definir la consulta SQL
consulta_sql = """
WITH CLIENTES_CTE AS (
    SELECT * FROM clientes
),
PRODUCTOS_CTE AS (
    SELECT * FROM productos
),
TRANSACCIONES_CTE AS (
    SELECT * FROM transacciones
),
UNION_CTE AS (
    SELECT
        C.CODIGO AS CLIENTE_CODIGO,
        C.TIPO_CLIENTE,
        DATE(T.FECHA_TRANSACCION) AS FECHA_TRANSACCION,
        C.PEP,
        C.RIESGO,
        C.PAIS AS CLIENTE_PAIS,
        P.CODIGO AS PRODUCTO_CODIGO,
        P.CUENTA,
        P.TIPO_CUENTA,
        P.ESTADO_CUENTA,
        P.PERFIL_WIRES_IN_MONTO,
        P.PERFIL_WIRES_IN_FRECUENCIA,
        P.PERFIL_WIRES_OUT_MONTO,
        P.PERFIL_WIRES_OUT_FRECUENCIA,
        T.FECHA_TRANSACCION,
        T.TIPO_TRANSACCION,
        T.MONTO,
        T.PAIS_ORIGEN_TRANSACCION,
        T.PAIS_DESTINO_TRANSACCION
    FROM CLIENTES_CTE C
    INNER JOIN PRODUCTOS_CTE P ON C.CODIGO = P.CODIGO
    INNER JOIN TRANSACCIONES_CTE T ON P.CUENTA = T.CUENTA
)
SELECT * FROM UNION_CTE;
"""

# Ejecutar la consulta y cargar el resultado en un DataFrame
df = pd.read_sql_query(consulta_sql, conn)

# Cerrar la conexión a la base de datos
conn.close()

# Mostrar las primeras filas del DataFrame
print(df.head())

profile = ProfileReport(df, title = 'Analisis Exploratorio Prueba Técnica Bancolombia Panama',sort = 'ascending', html = {'style':{'full_width': True}})
print("Termino de hacer el profile")

profile.to_widgets()
print("Termino de hacer el to_widget")

profile.to_notebook_iframe()
print("Termino de hacer el to_notebook_iframe")

profile.to_file(output_file="DataFrame.pdf")
print("Termino de hacer el pdf")

profile.to_file(output_file="DataFrame.html")
print("Termino de hacer el html")