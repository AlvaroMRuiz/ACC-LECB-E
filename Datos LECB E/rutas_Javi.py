import pandas as pd
import matplotlib.pyplot as plt

# --- 1. Leer regiones ICAO ---
def get_regions():
    icao_regions = {1: {}, 2: {}}
    with open("icao_regions.txt", 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            code = parts[0]
            name = ' '.join(parts[1:])
            if len(code) == 1:
                icao_regions[1][code] = name
            else:
                icao_regions[2][code] = name
    return icao_regions

# --- 2. Buscar la región ICAO ---
def get_icao_region(icao):
    if not isinstance(icao, str) or len(icao) < 2:
        return "Desconocido"
    icao = icao.upper()
    if icao[0] in ICAO_REGIONS[1]:
        return ICAO_REGIONS[1][icao[0]]
    return ICAO_REGIONS[2].get(icao[:2], "Desconocido")

# --- 3. Agrupación macro-regional ---
def get_macro_region(icao):
    """
    Clasifica un código ICAO en una macro-región:
    - Asia
    - América del Norte
    - América del Sur
    - África
    - Europa (Portugal / Norte-Francia / Este-Mediterráneo)
    Devuelve 'Desconocido' si el código no coincide con ninguna categoría.
    """
    if not isinstance(icao, str) or len(icao) < 2:
        return "Desconocido"

    icao = icao.strip().upper()
    prefix = icao[:2]
    first = icao[0]

    # --- Asia ---
    # O = Medio Oriente, R = Japón/Corea, V = Sudeste asiático, W = Indonesia/Malasia, Z = China
    if first in ['O', 'R', 'V', 'W', 'Z']:
        return "Asia"

    # --- América del Norte ---
    # K = USA, C = Canadá, M = México/Centroamérica, P/T = islas del Pacífico/Caribe norte
    if first in ['K', 'C', 'M', 'P', 'T'] and prefix not in [
        'SA','SB','SC','SD','SE','SF','SG','SK','SL','SM','SN','SO','SP','SS','SU','SV','SW','SY'
    ]:
        return "América del Norte"

    # --- América del Sur ---
    if prefix in [
        'SA','SB','SC','SD','SE','SF','SG','SK','SL','SM','SN','SO','SP','SS','SU','SV','SW','SY'
    ]:
        return "América del Sur"

    # --- África ---
    # D = Norte de África, F/G/H = África subsahariana
    if first in ['D', 'F', 'G', 'H']:
        return "África"

    # --- Europa subdividida ---
    # L = Sur/Este, E = Norte (Alemania, Francia, UK, etc.), B = Bélgica
    if first in ['L', 'E', 'B']:
        # Portugal
        if prefix == 'LP':
            return "Europa - Portugal"
        # Norte / Francia
        elif prefix in [
            'LE','LF','EB','ED','EH','EG','EK','EN','EI','BI','EL','LO','LS','LX','ET'
        ]:
            return "Europa - Norte/Francia"
        # Resto: Este / Mediterráneo
        else:
            return "Europa - Este/Mediterráneo"

    # --- Si no encaja en ninguna ---
    return "Desconocido"


# --- 4. Vuelos de entrada por país ---
def entry_flights(k=10):
    DF_ENTRY['Region_Entrada'] = DF_ENTRY['Origin'].apply(get_icao_region)
    entry_region = DF_ENTRY['Region_Entrada'].value_counts()
    entry_filter = entry_region[entry_region > k]
    total_entry = entry_filter.sum()

    plt.figure(figsize=(10,6))
    entry_filter.plot(kind='bar', color='green')
    plt.title(f"Regiones de entrada con más de {k} vuelos")
    plt.xlabel("Región")
    plt.ylabel("Número de vuelos")
    plt.xticks(rotation=45, ha="right")

    for i, v in enumerate(entry_filter.values):
        pct = (v / total_entry) * 100
        plt.text(i, v + 0.5, f"{v}\n{pct:.1f}%", ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.show()

# --- 5. Vuelos de salida por país ---
def depar_flights(k=10):
    DF_DEPAR['Region_Salida'] = DF_DEPAR['Destination'].apply(get_icao_region)
    depar_region = DF_DEPAR['Region_Salida'].value_counts()
    depar_filter = depar_region[depar_region > k]
    total_depar = depar_filter.sum()

    plt.figure(figsize=(10,6))
    depar_filter.plot(kind='bar', color='red')
    plt.title(f"Regiones de salida con más de {k} vuelos")
    plt.xlabel("Región")
    plt.ylabel("Número de vuelos")
    plt.xticks(rotation=45, ha="right")

    for i, v in enumerate(depar_filter.values):
        pct = (v / total_depar) * 100
        plt.text(i, v + 0.5, f"{v}\n{pct:.1f}%", ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.show()

# --- 6. Vuelos de entrada por macro-región ---
def entry_macro():
    DF_ENTRY['Macro_Entrada'] = DF_ENTRY['Origin'].apply(get_macro_region)
    entry_macro = DF_ENTRY['Macro_Entrada'].value_counts()

    plt.figure(figsize=(8,6))
    entry_macro.plot(kind='bar', color='blue')
    plt.title("Entradas por macro-región")
    plt.xlabel("Macro-Región")
    plt.ylabel("Número de vuelos")
    plt.xticks(rotation=45, ha="right")

    total = entry_macro.sum()
    for i, v in enumerate(entry_macro.values):
        pct = (v / total) * 100
        plt.text(i, v + 0.5, f"{v}\n{pct:.1f}%", ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.show()

# --- 7. Vuelos de salida por macro-región ---
def depar_macro():
    DF_DEPAR['Macro_Salida'] = DF_DEPAR['Destination'].apply(get_macro_region)
    depar_macro = DF_DEPAR['Macro_Salida'].value_counts()

    plt.figure(figsize=(8,6))
    depar_macro.plot(kind='bar', color='orange')
    plt.title("Salidas por macro-región")
    plt.xlabel("Macro-Región")
    plt.ylabel("Número de vuelos")
    plt.xticks(rotation=45, ha="right")

    total = depar_macro.sum()
    for i, v in enumerate(depar_macro.values):
        pct = (v / total) * 100
        plt.text(i, v + 0.5, f"{v}\n{pct:.1f}%", ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.show()

# --- 8. Tiempo dentro del espacio aéreo ---
def tiempo_en_espacio(df):
    # Convertir las columnas de tiempo
    df['Entry time'] = pd.to_datetime(df['Entry time'], format='%H:%M:%S', errors='coerce')
    df['Exit time'] = pd.to_datetime(df['Exit time'], format='%H:%M:%S', errors='coerce')

    # Calcular duración en minutos
    df['Duración_min'] = (df['Exit time'] - df['Entry time']).dt.total_seconds() / 60

    # Filtrar valores inválidos
    df = df.dropna(subset=['Duración_min'])
    df = df[df['Duración_min'] > 0]

    # Agrupar en intervalos de 5 minutos
    bins = range(0, int(df['Duración_min'].max()) + 10, 5)
    labels = [f"{i}-{i+5}" for i in bins[:-1]]
    df['Intervalo'] = pd.cut(df['Duración_min'], bins=bins, labels=labels, right=False)

    # Contar vuelos por intervalo
    conteo = df['Intervalo'].value_counts().sort_index()

    # --- Gráfica ---
    plt.figure(figsize=(10,6))
    bars = plt.bar(conteo.index.astype(str), conteo.values, color='skyblue', edgecolor='black')
    plt.title("Distribución del tiempo en el espacio aéreo (intervalos de 5 minutos)")
    plt.xlabel("Tiempo dentro del espacio aéreo (minutos)")
    plt.ylabel("Número de vuelos")
    plt.xticks(rotation=45, ha="right")

    # Añadir etiquetas encima de cada barra
    for bar, value in zip(bars, conteo.values):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                 f"{int(value)}", ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.show()

    return df, conteo

# --- 9. Comparación de vuelos por cambio de altitud ---
def vuelos_por_altitud(df):
    # Asegurarse de que las columnas son numéricas
    df['Entry FL'] = pd.to_numeric(df['Entry FL'], errors='coerce')
    df['Exit FL'] = pd.to_numeric(df['Exit FL'], errors='coerce')

    # Clasificar los vuelos
    df['Tipo_Vuelo'] = df.apply(
        lambda x: 'En ruta' if x['Entry FL'] == x['Exit FL'] else 'En evolución', axis=1
    )

    # Contar cada tipo
    conteo = df['Tipo_Vuelo'].value_counts()

    # --- Gráfica ---
    plt.figure(figsize=(8,6))
    bars = plt.bar(conteo.index, conteo.values, color=['lightgreen', 'salmon'], edgecolor='black')
    plt.title("Vuelos según cambio de altitud")
    plt.xlabel("Tipo de vuelo")
    plt.ylabel("Número de vuelos")

    # Añadir etiquetas encima de cada barra
    for bar, value in zip(bars, conteo.values):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                 f"{int(value)}", ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.show()

    return df, conteo



# --- 10. Main ---
if __name__ == '__main__':
    ICAO_REGIONS = get_regions()
    DF_ENTRY = pd.read_excel("EntryList_Original_Crossing_ACC_LECBCTAE_0000_2400.xlsx")
    DF_DEPAR = pd.read_excel("FlightList_Original_Crossing_ACC_LECBCTAE_0000_2400.xlsx")
    entry_flights(k=10)
    depar_flights(k=10)
    entry_macro()
    depar_macro()
    DF_ENTRY, conteo_tiempos = tiempo_en_espacio(DF_ENTRY)
    DF_ENTRY, conteo_altitud = vuelos_por_altitud(DF_ENTRY)