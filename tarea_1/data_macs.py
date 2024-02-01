#!/mnt/d2tb/home/jmcr/tfm/bin/python3
import os, gzip, datetime, argparse
import pandas as pd
from influxdb_client import InfluxDBClient
import pytz
import mysql.connector
import sys
from auxfunct_rangetime import consulta_meses, consulta_dias

# Argumentación por línea de comandos (https://stackoverflow.com/questions/6076690/verbose-level-with-argparse-and-multiple-voptions)
class VAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, const=None,
                 default=None, type=None, choices=None, required=False,
                 help=None, metavar=None):
        super(VAction, self).__init__(option_strings, dest, nargs, const,
                                      default, type, choices, required,
                                      help, metavar)
        self.values = 0

    def __call__(self, parser, args, values, option_string=None):
        # print('values: {v!r}'.format(v=values))
        if values is None:
            self.values += 1
        else:
            try:
                self.values = int(values)
            except ValueError:
                self.values = values.count('v') + 1
        setattr(args, self.dest, self.values)


# Configuración del cliente InfluxDB
org = 'UPM'
database = 'SSL'
retention_policy = 'autogen'
bucket = f'{database}/{retention_policy}'
tokenv2 = '1_lyKS1xcKU4NwneJiUsKTaa5gohz98YwYNzzM52LlQnUzBrMf18Tr9ujotYCVXNSkGntS9RJCUtYwBpU3cHSg=='
clnt = InfluxDBClient(url='https://apiivm78.etsii.upm.es:8086', \
                      token=tokenv2, org=org, timeout=200_000)

ap = argparse.ArgumentParser()
ap.add_argument("-f", "--from", type=str,
                help="Date for starting the analysis. Format YYYY-MM-DD HH:MM:SS. Yesterday when not provided")
ap.add_argument("-u", "--until", type=str,
                help="Date for ending the analysis. Format YYYY-MM-DD HH:MM:SS. Yesterday when not provided")
ap.add_argument("-v", "--verbose", nargs='?', action=VAction, dest='verbose',
                help="Option for detailed information")
ap.add_argument("mac", type=str,
                help="MAC address to analyze")
args = vars(ap.parse_args())
verbose = 0

if args['verbose']:
    verbose = args['verbose']

#Fechas inicio y fin de análisis
if args['from']:
    desde = args['from'].replace(' ', 'T') + '.000000000Z'
else:
    # Obtener la primera fecha almacenada en la base de datos desde hace 2 años
    query_first_date = 'from(bucket:"SSL/autogen") \
        |> range(start: -2y) \
        |> filter(fn:(r) => r._measurement == "sensoria_socks" and r._field == "Ax")\
        |> first()'
    
    result_last_date = clnt.query_api().query(org=org, query=query_first_date)

    # Encuentra la primera fecha entre todos los registros
    first_date = None
    for table in result_last_date:
        for record in table.records:
            time_str = record.get_time()
            if first_date is None or time_str < first_date:
                first_date = time_str
    desde = first_date.strftime("%Y-%m-%d %H:%M:%S").replace(' ', 'T') + '.000000000Z'
    
if args['until']:
    hasta = args['until'].replace(' ', 'T') + '.000000000Z'
else:
    # Obtener la fecha actual
    hasta = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(' ', 'T') + '.000000000Z'


#MAC
mac = args['mac']

#Llamada al script de funciones auxiliares
resultado_meses = consulta_meses(mac, desde, hasta)
resultado_dias = consulta_dias(mac, resultado_meses)

# Registra el número de muestras por mac en el rango de tiempo. 
def nmuestras_mac(mac, resultado_dias):
    df_total = pd.DataFrame()
    for start, stop in resultado_dias:
        query = f'from(bucket:"SSL/autogen") \
            |> range(start: {start}, stop: {stop}) \
            |> filter(fn:(r) => r._measurement == "sensoria_socks" and r.mac == "{mac}" and r._field == "Ax") \
            |> duplicate( column:"_time", as: "next_time") \
            |> keep(columns: ["mac", "_time", "next_time"]) \
            |> sort(columns: ["_time"]) \
            |> yield ()'
    
        try:
            result = clnt.query_api().query(org=org, query=query)
            res = pd.DataFrame()
            for i in result:
                rs = []
                for row in i.records:
                    rs.append(row.values)  
                res = pd.concat([res, pd.DataFrame(rs)], axis=0)
            res = res.drop(res.columns[[0]], axis=1)
            res.reset_index(inplace=True)
            # Desplaza una posición la columna duplicada
            res["next_time"] = res["next_time"].shift(-1)
            res["dif_time"] = res["next_time"] - res["_time"]
            # Agrupa si diferencia de tiempo es mayor a 1 segundo.
            res["group"] = (res["dif_time"].dt.total_seconds() > 1).cumsum()
            res = res.iloc[:-1] # Elimino último registro NaN del DF
            
            df = res.groupby('group').agg(
                mac=('mac', 'first'),
                desde=('_time',lambda x: x.iloc[1] if len(x) > 1 else x.iloc[0]), #salta el primer valor
                hasta=('next_time','last'),
                nmuestras=('group','count')
            ).reset_index(drop=True)
            print(df)
            df_total = pd.concat([df_total, df], ignore_index=True)
                
        except Exception as e:
            # No se obtienen datos
            print(f"Error al consultar InfluxDB: {e}")
            return None
            
    return df_total

# Transforma datos a formato diccionario 
def transformar_datos(df):
    if df is not None:
        # Convertir la columna "desde" a formato datetime
        df['desde'] = pd.to_datetime(df['desde'])
        # Convertir la columna "hasta" a formato datetime
        df['hasta'] = pd.to_datetime(df['hasta'])
        # Obtener el timezone 
        df['tz'] = df['desde'].dt.strftime('%Z%z')
        # Convertir los resultados a formato de diccionario
        resultados_dict = df.to_dict(orient='records')
        
        return resultados_dict
    else:
        return None

# Inyecta los datos a tabla d2datos en MySql
def inyectar_en_mysql(data):
    if data is not None:
        conn = mysql.connector.connect(
            host='apiivm78.etsii.upm.es',
            user='sleep',
            password='UVA#2023',
            database='sleep'
        )

        # Crear un cursor para ejecutar las consultas
        cursor = conn.cursor()
        
        # Insertar los datos en la tabla relacional
        for row in data:
            query = "INSERT INTO d2datos (mac, desde, hasta, tz, nmuestras) VALUES (%s, %s, %s, %s, %s)" \
                    "ON DUPLICATE KEY UPDATE mac=mac"
            values = (row['mac'], row['desde'], row['hasta'], row['tz'], row['nmuestras'])
            cursor.execute(query, values)
        
        # Confirmar los cambios en la base de datos
        conn.commit()
        # Cerrar la conexión
        cursor.close()
        conn.close()
    else:
        print("No se pueden escribir los datos en MySQL por un error previo.")
def main():
    
    dataframe_total = nmuestras_mac(mac, resultado_dias)
    data_dict = transformar_datos(dataframe_total)
    inyectar_en_mysql(data_dict)

if __name__ == "__main__":
    main()