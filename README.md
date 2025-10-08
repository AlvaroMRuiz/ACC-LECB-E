# 🗺️ Generador de rutas ACC LECB E

Script en **Python** que procesa un Excel con cruces del **ACC LECB E** para generar un **mapa interactivo** y **métricas de tráfico aéreo**.

---

## ✈️ Características principales

- 🔎 **Resolución de aeropuertos** mediante un índice CSV/ICAO, asignando cada vuelo a su región ICAO.  
- 🧭 **Trazado de rutas completas** con tooltips/popup informativos (origen, destino, longitud, tipo de perfil vertical y procedencia de la geometría).  
- 📊 **Clasificación automática** de vuelos en **ascensos**, **descensos** y **sobrevuelos**, con capas separadas y panel lateral de estadísticas por región.  
- 🧩 **Análisis de direcciones predominantes**: agrupa rutas con rumbos similares y dibuja **flujos principales** con grosor proporcional al volumen (filtrando <10 vuelos).  
- 🌍 **Capas auxiliares**: región ICAO global, geometría oficial del ACC LECB E, TMAs interferentes (LEBL, LEGE, LERS) y clusters de flujos siempre al frente.  
- 📈 **Opcionalmente genera gráficas y resúmenes (CSV)** sobre regiones, macro-regiones, duración en sector y perfiles de altitud.

---

## ⚙️ Ejecución básica

\```bash
python3 rutas_ACC_LECB_E.py \
  --excel-path EntryList_Original_Crossing_ACC_LECBCTAE_0000_2400.xlsx \
  --out rutas_lecbe.html
\```

---

## 🔧 Argumentos relevantes

| Flag | Descripción |
|------|--------------|
| `--excel-path` | Excel con las parejas origen/destino (**obligatorio**). |
| `--out` | Ruta del HTML de salida. |
| `--airports-csv` | Índice de aeropuertos (por defecto `iata-icao.csv`). |
| `--sector-geojson` | Cache con la geometría oficial (`sector_lecbe.json`). |
| `--route-samples` | Número de puntos para interpolar trayectorias geodésicas. |
| `--plots-dir` | Carpeta donde guardar gráficos analíticos (**opcional**). |
| `--region-threshold` | Umbral mínimo de vuelos por región en las gráficas. |
| `--opensky-*` | Credenciales para recuperar tracks ADS-B (**opcional**). |

---

## 📁 Resultados

Genera un **HTML interactivo** con las siguientes capas:

- 🛫 **Rutas completas** (apagadas por defecto para revisión).  
- 🧭 **Flujos principales** + flujos por perfil vertical.  
- 🗺️ **Capas informativas**: Regiones ICAO, ACC LECB E, TMAs y aeropuertos.  
- 📄 **CSV** con el recuento de parejas ORIGEN–DESTINO (`<out>.csv`).  
- 📂 Archivos analíticos en la carpeta `analitica/` cuando se usa `--plots-dir`.

---

## 🌐 Datos externos

| Archivo | Descripción |
|----------|--------------|
| `iata-icao.csv` | Índice de aeropuertos con coordenadas y país. |
| `icao_prefijos_pais_region.xlsx` | Mapeo **ICAO → región** para inferencias. |
| `sector_lecbe.json` | Cache local con polígonos oficiales del ACC. |
| `world_countries.geojson` | Soporte para pintar regiones y países. |

> 💡 **Nota:** la capa *“ACC LECB E”* se mantiene visualmente encima, pero es **no interactiva**, permitiendo que los flujos subyacentes reciban clics y muestren su información.
