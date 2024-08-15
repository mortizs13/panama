import pandas as pd
import sqlite3
import re
import os

def corregir_archivo_clientes():
    file_path = './media/CLIENTES.csv'

    # Leer las líneas del archivo original
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Corregir las líneas
    corrected_lines = []
    for line in lines:
        line = re.sub(r'MEDIO ALTO', 'MEDIO_ALTO', line)
        line = re.sub(r'MEDIO BAJO', 'MEDIO_BAJO', line)
        corrected_lines.append(line)

    # Guardar las líneas corregidas en un archivo temporal
    temp_file_path = 'CLIENTES_corrected.csv'
    with open(temp_file_path, 'w', encoding='utf-8') as file:
        file.writelines(corrected_lines)

    # Leer el archivo corregido con pandas usando espacio como delimitador
    df = pd.read_csv(temp_file_path, delimiter=' ', encoding='utf-8')

    # Verificar la estructura del DataFrame
    print(df.head()) 

    # Ajustar los nombres de las columnas
    df.columns = [
        'CODIGO', 
        'TIPO_CLIENTE', 
        'FECHA_ACTUALIZACION', 
        'PEP', 
        'RIESGO', 
        'PAIS'
    ]

    return df

def cargar_datos():
    # Cargar los datos de clientes
    df_clientes = corregir_archivo_clientes()
    return df_clientes

def cargar_archivo_productos():
    file_path = './media/PRODUCTO.csv'

    # Leer el archivo CSV usando comas como delimitador
    df = pd.read_csv(file_path, delimiter=',', encoding='utf-8')

    # Verificar la estructura del DataFrame
    print(df.head())  # Verificar la estructura del DataFrame

    # Ajustar los nombres de las columnas según el DataFrame actual
    df.columns = [
        'CODIGO', 
        'CUENTA', 
        'TIPO_CUENTA', 
        'ESTADO_CUENTA', 
        'PERFIL_WIRES_IN_MONTO', 
        'PERFIL_WIRES_IN_FRECUENCIA', 
        'PERFIL_WIRES_OUT_MONTO', 
        'PERFIL_WIRES_OUT_FRECUENCIA'
    ]

    return df

def cargar_archivo_transacciones():
    file_path = './media/TRANSACCIONES.csv'

    # Leer el archivo CSV usando tabuladores como delimitador
    df = pd.read_csv(file_path, delimiter='\t', encoding='utf-8')

    # Eliminar el carácter ' en el campo CUENTA
    df['CUENTA'] = df['CUENTA'].str.replace("'", "")
    
    df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].str.replace("'", "")
    
    df['PAIS_ORIGEN_TRANSACCION'] = df['PAIS_ORIGEN_TRANSACCION'].str.replace("'", "")
    
    df['PAIS_DESTINO_TRANSACCION'] = df['PAIS_DESTINO_TRANSACCION'].str.replace("'", "")

    # Verificar la estructura del DataFrame
    print(df.head())

    # Ajustar los nombres de las columnas según el DataFrame actual
    df.columns = [
        'CUENTA', 
        'FECHA_TRANSACCION', 
        'TIPO_TRANSACCION', 
        'MONTO', 
        'PAIS_ORIGEN_TRANSACCION', 
        'PAIS_DESTINO_TRANSACCION'
    ]

    return df

def main():
    # Cargar los datos de clientes
    df_clientes = cargar_datos()

    # Cargar los datos de productos
    df_productos = cargar_archivo_productos()

    # Cargar los datos de transacciones
    df_transacciones = cargar_archivo_transacciones()
    
    # Nombre de la base de datos
    db_name = 'bancolombia_panama.db'

    # Conectar a la base de datos SQLite (se creará si no existe)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Crear las tablas 'clientes', 'productos' y 'transacciones' si no existen
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clientes (
            CODIGO TEXT,
            TIPO_CLIENTE TEXT,
            FECHA_ACTUALIZACION TEXT,
            PEP TEXT,
            RIESGO TEXT,
            PAIS TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos (
            CODIGO TEXT,
            CUENTA TEXT,
            TIPO_CUENTA TEXT,
            ESTADO_CUENTA TEXT,
            PERFIL_WIRES_IN_MONTO REAL,
            PERFIL_WIRES_IN_FRECUENCIA INTEGER,
            PERFIL_WIRES_OUT_MONTO REAL,
            PERFIL_WIRES_OUT_FRECUENCIA INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transacciones (
            CUENTA TEXT,
            FECHA_TRANSACCION TEXT,
            TIPO_TRANSACCION TEXT,
            MONTO REAL,
            PAIS_ORIGEN_TRANSACCION TEXT,
            PAIS_DESTINO_TRANSACCION TEXT
        )
    ''')

    # Cargar los datos en las tablas correspondientes
    df_clientes.to_sql('clientes', conn, if_exists='replace', index=False)
    df_productos.to_sql('productos', conn, if_exists='replace', index=False)
    df_transacciones.to_sql('transacciones', conn, if_exists='replace', index=False)
    
    # Cerrar la conexión a la base de datos
    conn.close()

if __name__ == '__main__':
    main()