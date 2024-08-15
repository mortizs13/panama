import pandas as pd
import sqlite3
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt

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

# Función para detectar anomalías utilizando DBSCAN
def detectar_anomalias_dbscan(df):
    X = df[['MONTO', 'PERFIL_WIRES_IN_MONTO', 'PERFIL_WIRES_OUT_MONTO']].values

    # Configuración de DBSCAN
    dbscan = DBSCAN(eps=0.5, min_samples=5)
    df['anomaly'] = dbscan.fit_predict(X)

    # Anomalías detectadas (etiqueta -1)
    anomalies = df[df['anomaly'] == -1]
    
    # Guardar las anomalías detectadas en un archivo Excel
    #anomalies.to_excel('salida_anomalias_dbscan.xlsx', index=False)
    anomalies.to_csv('salida_anomalias_dbscan.csv', index=False)
    
    # Visualización y guardado de gráficos de anomalías

    # Gráfico 1: PERFIL_WIRES_IN_MONTO vs MONTO
    plt.figure(figsize=(10, 6))
    plt.scatter(X[:, 1], X[:, 0], c=df['anomaly'], cmap='coolwarm', marker='o')
    plt.xlabel('PERFIL_WIRES_IN_MONTO')
    plt.ylabel('MONTO')
    plt.title('Detección de Anomalías (DBSCAN) - Wires In Monto vs Monto')
    plt.colorbar()
    plt.savefig('anomalias_dbscan_wires_in_vs_monto.png')
    plt.close()

    # Gráfico 2: PERFIL_WIRES_OUT_MONTO vs MONTO
    plt.figure(figsize=(10, 6))
    plt.scatter(X[:, 2], X[:, 0], c=df['anomaly'], cmap='coolwarm', marker='o')
    plt.xlabel('PERFIL_WIRES_OUT_MONTO')
    plt.ylabel('MONTO')
    plt.title('Detección de Anomalías (DBSCAN) - Wires Out Monto vs Monto')
    plt.colorbar()
    plt.savefig('anomalias_dbscan_wires_out_vs_monto.png')
    plt.close()

    # Gráfico 3: PERFIL_WIRES_IN_MONTO vs PERFIL_WIRES_OUT_MONTO
    plt.figure(figsize=(10, 6))
    plt.scatter(X[:, 1], X[:, 2], c=df['anomaly'], cmap='coolwarm', marker='o')
    plt.xlabel('PERFIL_WIRES_IN_MONTO')
    plt.ylabel('PERFIL_WIRES_OUT_MONTO')
    plt.title('Detección de Anomalías (DBSCAN) - Wires In vs Wires Out')
    plt.colorbar()
    plt.savefig('anomalias_dbscan_wires_in_vs_wires_out.png')
    plt.close()

    print("Anomalías detectadas guardadas en 'salida_anomalias_dbscan.xlsx'")
    print("Gráficos de anomalías guardados en archivos PNG")

# Ejecutar la función de detección de anomalías
detectar_anomalias_dbscan(df)