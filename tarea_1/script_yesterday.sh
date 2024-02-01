#!/bin/bash

# Lista de direcciones MAC
macs=("ED:83:61:15:45:7D" "E8:83:AD:D2:66:3F" "FB:A9:21:8E:78:69" "E6:6C:1C:F6:A3:4A" "E0:52:B2:8B:2A:C2" "C9:7B:84:76:32:14" "D7:9E:C9:C4:72:DF" "FA:06:67:D3:5B:79" "FC:E0:9D:61:36:F0" "E0:52:B2:8B:2A:C2" "C6:19:A6:5A:92:F8" "C6:D1:AE:0B:26:84")  # Agrega tus direcciones MAC aquí

# Fecha de ayer en formato "YYYY-MM-DD"
yesterday=$(date -d "1 day ago" +"%Y-%m-%d")

# Ruta a los scripts
data_macs_script="data_macs.py"

# Bucle para cada MAC
for mac in "${macs[@]}"; do
    # Obtener el rango de fechas para ayer
    start_time="${yesterday} 00:00:00"
    end_time="${yesterday} 23:59:59"

    # Ejecutar data_macs.py para obtener número de muestras ayer
    echo "Running funaux.py for MAC: $mac, Date Range: $start_time - $end_time"
    python3 "$data_macs_script" "$start_time" "$end_time" "$mac"
done