Generador de rutas ACC LECB E
Script Python que procesa un Excel con cruces del ACC LECB E para generar un mapa interactivo y métricas de tráfico aéreo.

Características principales
Resuelve aeropuertos mediante un índice CSV/ICAO y asigna cada vuelo a su región ICAO.
Traza rutas completas y muestra tooltips/popup con datos de origen, destino, longitud, tipo de perfil vertical y procedencia de la geometría.
Clasifica los vuelos en ascensos, descensos y sobrevuelos; crea capas separadas y un panel lateral con estadísticas de regiones.
Detecta direcciones predominantes de entrada al ACC, agrupa rutas con rumbos similares y dibuja flujos principales con grosor proporcional al volumen (filtrados cuando <10 vuelos).
Añade capas auxiliares: región ICAO global, geometría oficial del ACC LECB E, TMAs que interfieren (LEBL, LEGE, LERS) y clusters de flujos siempre al frente.
Opcionalmente genera gráficas y resúmenes (CSV) sobre regiones, macro-regiones, duración en sector y perfiles de altitud.

Ejecución básica
python3 rutas_ACC_LECB_E.py \
  --excel-path EntryList_Original_Crossing_ACC_LECBCTAE_0000_2400.xlsx \
  --out rutas_lecbe.html
  
Argumentos relevantes
Flag	Descripción
--excel-path	Excel con las parejas origen/destino (obligatorio).
--out	Ruta del HTML de salida.
--airports-csv	Índice de aeropuertos (default iata-icao.csv).
--sector-geojson	Cache con la geometría oficial (sector_lecbe.json).
--route-samples	Puntos para interpolar trayectorias geodésicas.
--plots-dir	Carpeta donde guardar gráficos analíticos (opcional).
--region-threshold	Umbral mínimo de vuelos por región en gráficas.
--opensky-*	Credenciales para recuperar tracks ADS-B (opcional).


Resultados
HTML interactivo con capas:
Rutas completas (apagadas por defecto para revisión).
Flujos principales + flujos por perfil vertical.
Capas informativas: Regiones ICAO, ACC LECB E, TMAs y aeropuertos.
CSV con el recuento de parejas ORIGEN–DESTINO (<out>.csv).
Archivos analíticos en analitica/ cuando se usa --plots-dir.
Datos externos
iata-icao.csv: índice de aeropuertos con coordenadas y país.
icao_prefijos_pais_region.xlsx: mapeo ICAO → región para inferencias.
sector_lecbe.json: cache local con polígonos oficiales del ACC.
world_countries.geojson: soporte para pintar regiones/countries.
Nota: la capa “ACC LECB E” se mantiene encima visualmente pero es no interactiva, permitiendo que los flujos subyacentes reciban clics y muestren su información.
