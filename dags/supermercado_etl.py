import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sqlalchemy.engine import URL
from sqlalchemy import create_engine


# Cargar parametros de la conexion a la base de datos
dotenv_path = os.path.join('/opt/airflow/data', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Configurar parametros de coneccion a la base de datos
url_object = URL.create(
    'postgresql+psycopg2',
    username=os.getenv('USER_NAME'),
    password=os.getenv('PASSWORD'),
    host=os.getenv('HOST'),
    database=os.getenv('DB_NAME')
)

# Crear motor con SQLAlchemy para conectarse a la base de datos
engine = create_engine(url_object)


def extraer_datos():
    data = pd.read_csv(
        '/opt/airflow/data/customer_shopping_data.csv', 
        sep=',',
        dtype={
            'invoice_no': str,
            'customer_id': str,
            'gender': str,
            'age': int,
            'category': str,
            'quantity': int,
            'price': float,
            'payment_method': str,
            'shopping_mall': str
        }
    )
    return data


def procesar_datos(data):
    # Ajuste del campo de fecha
    data['invoice_date'] = data['invoice_date'].apply(lambda x: pd.Timestamp(x).date())

    # Reemplazar espacios en blanco en el dataset inicial por NaN
    data = data.replace(r'^\s+$', np.nan, regex=True)

    # Ajuste de nombre de las columnas
    data = data.rename(columns={
        'invoice_no': 'id_factura',
        'customer_id': 'id_cliente',
        'gender': 'genero',
        'age': 'edad',
        'category': 'categoria',
        'quantity': 'cantidad',
        'price': 'precio_unitario',
        'payment_method': 'metodo_pago',
        'invoice_date': 'fecha_factura',
        'shopping_mall': 'centro_comercial'
    })

    # Definir variables para el procesamiento de los datos
    num_cols = ['edad', 'cantidad', 'precio_unitario']

    cat_cols = [
        'genero',
        'categoria',
        'metodo_pago',
        'centro_comercial'
    ]

    valores_validos = {
        'genero': ['Female', 'Male'],
        'categoria': ['Clothing', 'Shoes', 'Books', 'Cosmetics', 'Food & Beverage', 'Toys', 'Technology', 'Souvenir'],
        'metodo_pago': ['Credit Card', 'Debit Card', 'Cash'],
        'centro_comercial': [
            'Kanyon',
            'Forum Istanbul',
            'Metrocity',
            'Metropol AVM',
            'Istinye Park',
            'Mall of Istanbul',
            'Emaar Square Mall',
            'Cevahir AVM', 
            'Viaport Outlet', 
            'Zorlu Center'
        ]
    }

    # Se convierten en nulos los valores atípicos identificados
    for var in cat_cols:
        data[var] = data[var].apply(lambda x: x if x in valores_validos[var] else np.nan)

    data['edad'] = np.where((data['edad'] >= 18) & (data['edad'] <= 70), data['edad'], np.nan)

    # Imputación por la media y moda

    # Imputacion de variables numéricas
    imp_numericas = SimpleImputer(missing_values=np.nan, strategy='mean')
    data[num_cols] = imp_numericas.fit_transform(data[num_cols])

    # Imputacion de variables categóricas
    imp_categoricas = SimpleImputer(missing_values=np.nan, strategy='most_frequent')
    data[cat_cols] = imp_categoricas.fit_transform(data[cat_cols])

    data['edad'] = data['edad'].apply(int)
    data['cantidad'] = data['cantidad'].apply(int)

    # Mapear datos al idioma español
    genero_esp = {'Female': 'Femenino', 'Male': 'Masculino'}

    categoria_esp = {
        'Clothing': 'Ropa', 
        'Shoes': 'Zapatos',
        'Books': 'Libros',
        'Cosmetics': 'Cosméticos',
        'Food & Beverage': 'Alimentos y bebidas',
        'Toys': 'Juguetes',
        'Technology': 'Tecnología',
        'Souvenir': 'Recuerdos'
    }

    metodo_pago_esp = {
        'Credit Card': 'Tarjeta de crédito', 
        'Debit Card': 'Tarjeta de débito',
        'Cash': 'Efectivo'
    }

    data['genero'] = data['genero'].map(genero_esp)
    data['categoria'] = data['categoria'].map(categoria_esp)
    data['metodo_pago'] = data['metodo_pago'].map(metodo_pago_esp)

    # Calcular total por cada transaccion
    data['venta_total'] = data['cantidad'] * data['precio_unitario']

    # Asignar claves foráneas para cargar datos a la tabla de (ventas) hechos
    ids_cetegoria = {
        'Ropa': 1, 
        'Zapatos': 2,
        'Libros': 3,
        'Cosméticos': 4,
        'Alimentos y bebidas': 5,
        'Juguetes': 6,
        'Tecnología': 7,
        'Recuerdos': 8
    }

    ids_centro_comercial = {
        'Kanyon': 1,
        'Forum Istanbul': 2,
        'Metrocity': 3,
        'Metropol AVM': 4,
        'Istinye Park': 5,
        'Mall of Istanbul': 6,
        'Emaar Square Mall': 7,
        'Cevahir AVM': 8, 
        'Viaport Outlet': 9, 
        'Zorlu Center': 10
    }

    ids_metodo_pago = {
        'Tarjeta de crédito': 1, 
        'Tarjeta de débito': 2,
        'Efectivo': 3
    }

    # Leer datos desde la dimensión de tiempo
    sql_query = 'SELECT fecha_completa, id FROM fecha'

    # Realizar la lectura de los datos
    with engine.begin() as conn:
        df_fechas = pd.read_sql(sql=sql_query, con=conn)

    df_fechas = df_fechas.rename(columns={'id': 'id_fecha'})
    df_fechas['fecha_completa'] = df_fechas['fecha_completa'].apply(lambda x: x.strftime('%Y-%m-%d'))

    df_ventas = data.copy()
    df_ventas['id_categoria'] = df_ventas['categoria'].map(ids_cetegoria)
    df_ventas['id_metodo_pago'] = df_ventas['metodo_pago'].map(ids_metodo_pago)
    df_ventas['id_centro_comercial'] = df_ventas['centro_comercial'].map(ids_centro_comercial)
    df_ventas['fecha_factura'] = df_ventas['fecha_factura'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_ventas = pd.merge(
        df_ventas,
        df_fechas,
        how='left',
        left_on='fecha_factura',
        right_on='fecha_completa'
    )

    df_ventas = df_ventas[[
        'id_factura', 
        'id_cliente', 
        'id_categoria', 
        'id_metodo_pago', 
        'id_fecha', 
        'id_centro_comercial', 
        'precio_unitario', 
        'cantidad',
        'venta_total'
    ]]

    return df_ventas


def cargar_datos(data):
    # Cargar datos procesados en la tabla de ventas
    with engine.begin() as conn:
        data.to_sql('ventas', con=conn, index=False, if_exists='append')
    print('Datos cargados exitosamente')


def ejecutar_etl():
    print('Se inicia el proceso de ETL')
    data = extraer_datos()
    datos_procesados = procesar_datos(data)
    cargar_datos(datos_procesados)