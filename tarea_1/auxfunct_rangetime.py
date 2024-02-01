#!/usr/bin/python3
from influxdb_client import InfluxDBClient
from datetime import timedelta

# Configuración del cliente InfluxDB
org = 'UPM'
database = 'SSL'
retention_policy = 'autogen'
bucket = f'{database}/{retention_policy}'
tokenv2 = '1_lyKS1xcKU4NwneJiUsKTaa5gohz98YwYNzzM52LlQnUzBrMf18Tr9ujotYCVXNSkGntS9RJCUtYwBpU3cHSg=='
clnt = InfluxDBClient(url='https://apiivm78.etsii.upm.es:8086', \
                      token=tokenv2, org=org, timeout=200_000)

# Se obtiene rango de fechas donde en meses donde hay datos
def consulta_meses(mac, desde, hasta):
    query = f'from(bucket:"SSL/autogen") \
        |> range(start: {desde}, stop: {hasta}) \
        |> filter(fn:(r) => r._measurement == "sensoria_socks"  and r.mac == "{mac}" and r._field == "Ax") \
        |> keep(columns: ["_time", "_value"]) \
        |> aggregateWindow(every: 1mo, column: "_value", fn:count, timeSrc: "_start", createEmpty: false) \
        |> keep(columns: ["_time", "_value"]) \
        |> yield ()'
    
    try:
        result = clnt.query_api().query(org=org, query=query)
        months = []
        for i in result:
            for row in i.records:
                time = row.values["_time"]
                months.append(time)     
        month_range = []
        for month_year in months:
            inicio = month_year
            # Calcula el final del mes
            if inicio.month == 12:
            # Si el mes es diciembre, incrementa el año y establecer el mes a enero (1)
                end_date = inicio.replace(year=inicio.year + 1, month=1, day=1) - timedelta(seconds=1)
            else:
            # De lo contrario, simplemente incrementar el mes
                end_date = inicio.replace(month=inicio.month + 1, day=1) - timedelta(seconds=1)
            inicio = inicio.strftime("%Y-%m-%d %H:%M:%S").replace(' ', 'T') + '.000000000Z'
            end_date = end_date.strftime("%Y-%m-%d %H:%M:%S").replace(' ', 'T') + '.000000000Z'
            month_range.append((inicio,end_date))
        return month_range
   
    except Exception as e:
        # No se obtienen datos
        print(f"Error al consultar InfluxDB: {e}")
        return None

# Se obtiene rango de fechas donde en días por mes donde hay datos
def consulta_dias(mac, month_range):
    all_days=[]
    for desde, hasta in month_range:
        query_2= f'from(bucket:"SSL/autogen") \
            |> range(start: {desde}, stop: {hasta}) \
            |> filter(fn:(r) => r._measurement == "sensoria_socks"  and r.mac == "{mac}" and r._field == "Ax") \
            |> keep(columns: ["_time", "_value"]) \
            |> aggregateWindow(every: 1d, column: "_value", fn:count, timeSrc: "_start", createEmpty: false) \
            |> keep(columns: ["_time", "_value"]) \
            |> yield ()'
        
        result_2 = clnt.query_api().query(org=org, query=query_2)
        days = []
        for i in result_2:
            for row in i.records:
                time = row.values["_time"]
                time_new = time.strftime("%Y-%m-%d %H:%M:%S").replace(' ', 'T') + '.000000000Z'
                end_hour = (time + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(microseconds=1)
                end_hour = end_hour.strftime("%Y-%m-%d %H:%M:%S").replace(' ', 'T') + '.000000000Z'
                days.append((time_new,end_hour))
        all_days.append(days)
    day_range = []
    for i in all_days:
        day_range.extend(i)
    return day_range
