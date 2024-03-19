#!/usr/bin/python3
import os, gzip, datetime, argparse
import pandas as pd
from influxdb_client import InfluxDBClient
import pytz
import mysql.connector
import sys
import numpy as np
import matplotlib.pyplot as plt
from scipy.fftpack import fft, fftfreq
from scipy.signal import find_peaks


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
ap.add_argument("mac_izq", type=str,
                help="MAC1 address to analyze")
#ap.add_argument("mac_der", type=str,
                #help="MAC2 address to analyze")
args = vars(ap.parse_args())

#Fechas inicio y fin de análisis
desde = args['from'].replace(' ', 'T') + '.000000000Z'
hasta = args['until'].replace(' ', 'T') + '.000000000Z'
mac_izq = args['mac_izq']
#mac_der = args['mac_der']

# Obtener dataframe de datos 
def datos_dataframe_izq(desde, hasta, mac_izq):
        query = f'from(bucket:"SSL/autogen") \
            |> range(start: {desde}, stop: {hasta}) \
            |> filter(fn:(r) => r._measurement == "sensoria_socks" and r.mac == "{mac_izq}") \
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") \
            |> keep(columns: ["_time", "lat", "lng", "pos", "Ax", "Ay", "Az", "S0", "S1", "S2"]) \
            |> yield ()'
        
        clnt = InfluxDBClient(url='https://apiivm78.etsii.upm.es:8086', token=tokenv2, org=org)
        result = clnt.query_api().query(org=org, query=query)
        res = pd.DataFrame()
        if result:
            for i in result:
                rs = []
                for row in i.records:
                    rs.append(row.values)
                res = pd.concat([res, pd.DataFrame(rs)], axis=0)
            res = res.drop(res.columns[[0]], axis=1)
            res.reset_index(drop=True, inplace=True)
            pd.set_option('display.max_columns', None)
            return res
        else:
            print("No se encontraron resultados para la consulta.")
            return None

df = datos_dataframe_izq(desde, hasta, mac_izq)

df['Mod_A'] = np.sqrt(df['Ax']**2 + df['Ay']**2 + df['Ax']**2)

df.to_csv('data1.csv')