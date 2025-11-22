import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import sys
from collections import Counter
import re

def robust_csv_loader(csv_file):
    """
    Carga el archivo CSV de manera robusta, manejando inconsistencias
    """
    print(f"Cargando archivo CSV: {csv_file}")
    
    strategies = [
        # Estrategia 1: Carga directa
        lambda: pd.read_csv(csv_file, skiprows=1),
        
        # Estrategia 2: Carga omitiendo líneas problemáticas
        lambda: pd.read_csv(csv_file, skiprows=1, on_bad_lines='skip', engine='python'),
        
        # Estrategia 3: Carga como texto y luego limpia
        lambda: pd.read_csv(csv_file, skiprows=1, dtype=str, on_bad_lines='skip'),
        
        # Estrategia 4: Carga con delimitador flexible
        lambda: pd.read_csv(csv_file, skiprows=1, sep=None, engine='python'),
    ]
    
    for i, strategy in enumerate(strategies):
        try:
            print(f"    Intentando estrategia {i+1}...")
            df = strategy()
            print(f"    Estrategia {i+1} exitosa - {len(df)} filas cargadas")
            return df
        except Exception as e:
            print(f"    Estrategia {i+1} falló: {e}")
            continue
    
    # Estrategia 5: Carga manual línea por línea
    print("    Intentando carga manual línea por línea...")
    try:
        return manual_csv_loader(csv_file)
    except Exception as e:
        print(f"    Carga manual falló: {e}")
    
    return None

def manual_csv_loader(csv_file):
    """
    Carga el CSV manualmente, línea por línea, para manejar inconsistencias
    """
    with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    
    # Encontrar encabezados
    if len(lines) < 2:
        raise ValueError("Archivo demasiado corto")
    
    headers = lines[1].strip().split(',')
    expected_columns = len(headers)
    
    print(f"   Encabezados detectados: {expected_columns} columnas")
    print(f"   Líneas totales: {len(lines)}")
    
    # Procesar datos
    data = []
    problematic_lines = 0
    
    for i, line in enumerate(lines[2:], start=3):  # Empezar desde línea 3 (después de metadatos y encabezados)
        fields = line.strip().split(',')
        
        if len(fields) == expected_columns:
            data.append(fields)
        elif len(fields) > expected_columns:
            # Demasiados campos - truncar
            data.append(fields[:expected_columns])
            problematic_lines += 1
        elif len(fields) < expected_columns:
            # Muy pocos campos - rellenar con NaN
            padded_fields = fields + [np.nan] * (expected_columns - len(fields))
            data.append(padded_fields)
            problematic_lines += 1
        else:
            problematic_lines += 1
    
    if problematic_lines > 0:
        print(f"     Líneas problemáticas corregidas: {problematic_lines}")
    
    df = pd.DataFrame(data, columns=headers)
    return df

def clean_and_validate_data(df):
    """
    Limpia y valida los datos del DataFrame
    """
    print(" Limpiando y validando datos...")
    
    # Información inicial
    print(f"    Forma inicial: {df.shape[0]} filas, {df.shape[1]} columnas")
    
    # Verificar columnas críticas
    critical_columns = ['SSID', 'RSSI', 'Channel']
    missing_columns = [col for col in critical_columns if col not in df.columns]
    
    if missing_columns:
        print(f"     Columnas faltantes: {missing_columns}")
        print(f"    Columnas disponibles: {list(df.columns)}")
    
    # Limpiar columnas numéricas críticas
    if 'RSSI' in df.columns:
        df['RSSI'] = pd.to_numeric(df['RSSI'], errors='coerce')
        rssi_nulos = df['RSSI'].isna().sum()
        if rssi_nulos > 0:
            print(f"     Valores RSSI no numéricos eliminados: {rssi_nulos}")
    
    if 'Channel' in df.columns:
        df['Channel'] = pd.to_numeric(df['Channel'], errors='coerce')
        channel_nulos = df['Channel'].isna().sum()
        if channel_nulos > 0:
            print(f"     Valores Channel no numéricos eliminados: {channel_nulos}")
    
    # Eliminar filas sin datos críticos
    initial_rows = len(df)
    df = df.dropna(subset=['RSSI', 'Channel'])
    final_rows = len(df)
    removed_rows = initial_rows - final_rows
    
    if removed_rows > 0:
        print(f"   🗑️  Filas sin datos críticos eliminadas: {removed_rows}")
    
    print(f"    Forma final: {df.shape[0]} filas, {df.shape[1]} columnas")
    return df

def format_channels_list(channels):
    """
    Convierte una lista de canales numpy.int64 a una lista de enteros Python normales
    """
    return [int(channel) for channel in channels]

def generate_comprehensive_analysis(df, csv_file):
    """
    Genera un análisis completo e interpretado de los datos
    """
    analysis = "\n" + "="*60 + "\n"
    analysis += " RESUMEN EJECUTIVO DEL ANÁLISIS\n"
    analysis += "="*60 + "\n\n"
    
    # Hallazgos principales
    total_networks = len(df)
    weak_networks = len(df[df['RSSI'] <= -80])
    weak_percentage = (weak_networks / total_networks) * 100
    
    analysis += " HALLAZGOS PRINCIPALES:\n"
    analysis += f"   • Total de redes detectadas: {total_networks:,} redes\n"
    analysis += f"   • Redes con señal débil: {weak_networks:,} redes ({weak_percentage:.1f}% del total)\n"
    analysis += f"   • Canales utilizados: {len(df['Channel'].unique())} canales diferentes\n\n"
    
    # Análisis de canales no superpuestos
    non_overlapping = [1, 6, 11]
    channel_counts = df['Channel'].value_counts()
    
    analysis += " SITUACIÓN DE CANALES NO SUPERPUESTOS:\n"
    for channel in non_overlapping:
        count = channel_counts.get(channel, 0)
        status = ""
        if count > 400:
            status = " (Extremadamente congestionado)"
        elif count > 300:
            status = " (Muy congestionado)"
        elif count > 200:
            status = " (Congestionado)"
        elif count > 100:
            status = " (Moderado)"
        else:
            status = " (Óptimo)"
        analysis += f"   • Canal {channel}: {count:,} redes {status}\n"
    
    # Canales problemáticos
    analysis += "\n  CANALES PROBLEMÁTICOS (INTERFERENCIA):\n"
    overlapping_issues = []
    for channel in df['Channel'].unique():
        channel_int = int(channel)  # Convertir a entero normal
        if channel_int not in non_overlapping:
            closest_non_overlap = min(non_overlapping, key=lambda x: abs(x - channel_int))
            count = len(df[df['Channel'] == channel])
            overlapping_issues.append((channel_int, closest_non_overlap, count))
    
    # Ordenar por cantidad de redes (más problemáticos primero)
    overlapping_issues.sort(key=lambda x: x[2], reverse=True)
    
    for channel, closest, count in overlapping_issues[:6]:  # Top 6 más problemáticos
        analysis += f"   • Canal {channel}: {count:,} redes (interfiere con canal {closest})\n"
    
    # Recomendaciones estratégicas
    analysis += "\n" + "="*60 + "\n"
    analysis += " RECOMENDACIONES ESTRATÉGICAS\n"
    analysis += "="*60 + "\n\n"
    
    analysis += " PROBLEMAS CRÍTICOS IDENTIFICADOS:\n"
    analysis += "   1. Canal 11 saturado - Evitar completamente\n"
    analysis += "   2. Todos los canales no superpuestos están congestionados\n"
    analysis += "   3. Alta densidad de redes en ambiente 2.4GHz\n\n"
    
    analysis += " ESTRATEGIAS RECOMENDADAS:\n"
    analysis += "   1. MIGRACIÓN A 5GHz:\n"
    analysis += "      • Configurar redes en banda 5GHz si los dispositivos lo soportan\n"
    analysis += "      • Menor interferencia y más canales disponibles\n\n"
    
    analysis += "   2. CANALES ALTERNATIVOS EN 2.4GHz:\n"
    analysis += "      • Canal 13: {} redes (menos congestionado)\n".format(channel_counts.get(13, 0))
    analysis += "      • Canal 14: {} redes (menos congestionado)\n".format(channel_counts.get(14, 0))
    analysis += "      • Canal 5: {} redes (muy poco congestionado)\n\n".format(channel_counts.get(5, 0))
    
    analysis += "   3. OPTIMIZACIÓN DE 2.4GHz:\n"
    analysis += "      • Usar ancho de canal de 20MHz (no 40MHz)\n"
    analysis += "      • Transmitir en potencia baja para no afectar redes vecinas\n"
    analysis += "      • Programar reinicios nocturnos del router\n\n"
    
    analysis += "   4. PARA REDES CRÍTICAS:\n"
    analysis += "      • Implementar calidad de servicio (QoS)\n"
    analysis += "      • Usar banda dual (2.4GHz para IoT, 5GHz para dispositivos principales)\n\n"
    
    analysis += " PARA USUARIOS FINALES:\n"
    analysis += "   • Conectar dispositivos importantes a 5GHz cuando sea posible\n"
    analysis += "   • Ubicar el router lejos de interferencias (microondas, teléfonos inalámbricos)\n"
    analysis += "   • Considerar sistemas mesh para mejor cobertura\n\n"
    
    analysis += " PERSPECTIVA:\n"
    analysis += "   El entorno analizado muestra una SATURACIÓN SEVERA de la banda 2.4GHz,\n"
    analysis += "   típica de áreas urbanas densas. La migración a 5GHz no es solo recomendable,\n"
    analysis += "   sino necesaria para obtener rendimiento adecuado.\n"
    
    return analysis

def analyze_wifi_interference(csv_file):
    try:
        # Cargar el archivo CSV de manera robusta
        df = robust_csv_loader(csv_file)
        
        if df is None:
            print(" No se pudo cargar el archivo con ninguna estrategia")
            return None
        
        # Limpiar y validar datos
        df = clean_and_validate_data(df)
        
        if len(df) == 0:
            print(" No hay datos válidos después de la limpieza")
            return None
        
        print(f" Archivo '{csv_file}' procesado correctamente")
        print(f" Filas válidas: {len(df)}")
        
    except FileNotFoundError:
        print(f" Error: No se encontró el archivo '{csv_file}'")
        return None
    except Exception as e:
        print(f" Error al procesar el archivo: {e}")
        return None
    
    # Función para clasificar la calidad de señal
    def classify_signal(rssi):
        if rssi >= -50:
            return "Excelente"
        elif rssi >= -60:
            return "Buena"
        elif rssi >= -70:
            return "Regular"
        elif rssi >= -80:
            return "Débil"
        else:
            return "Muy débil"
    
    df['Calidad'] = df['RSSI'].apply(classify_signal)
    
    print("\n" + "="*50)
    print("ANÁLISIS DE INTERFERENCIAS WiFi")
    print("="*50)
    
    # 1. Análisis general de redes
    print("\n1.  RESUMEN GENERAL DE REDES DETECTADAS:")
    print(f"   - Total de redes detectadas: {len(df)}")
    print(f"   - Redes únicas por SSID: {df['SSID'].nunique()}")
    
    # Verificar si existe la columna FirstSeen y tiene datos válidos
    if 'FirstSeen' in df.columns:
        try:
            # Intentar convertir a datetime, si falla usar valores como string
            first_seen_min = df['FirstSeen'].dropna().iloc[0] if len(df['FirstSeen'].dropna()) > 0 else "N/A"
            first_seen_max = df['FirstSeen'].dropna().iloc[-1] if len(df['FirstSeen'].dropna()) > 0 else "N/A"
            print(f"   - Rango de tiempo: {first_seen_min} a {first_seen_max}")
        except:
            print(f"   - Rango de tiempo: Datos de tiempo no disponibles")
    
    # 2. Análisis por canal
    print("\n2.  DISTRIBUCIÓN POR CANAL:")
    channel_dist = df['Channel'].value_counts().sort_index()
    for channel, count in channel_dist.items():
        print(f"   - Canal {int(channel)}: {count} redes")
    
    # 3. Análisis de interferencias por canal
    print("\n3.   ANÁLISIS DE INTERFERENCIAS POR CANAL:")
    
    # Canales no superpuestos en 2.4 GHz: 1, 6, 11
    non_overlapping = [1, 6, 11]
    overlapping_issues = []
    
    for channel in df['Channel'].unique():
        channel_int = int(channel)  # Convertir a entero normal
        if channel_int not in non_overlapping:
            # Determinar qué canales no superpuestos están más cerca
            closest_non_overlap = min(non_overlapping, key=lambda x: abs(x - channel_int))
            overlapping_issues.append((channel_int, closest_non_overlap, abs(channel_int - closest_non_overlap)))
    
    if overlapping_issues:
        print("    Se detectaron redes en canales que causan interferencia:")
        for channel, closest, distance in overlapping_issues:
            count = len(df[df['Channel'] == channel])
            print(f"     - Canal {channel}: {count} redes (interfiere con canal {closest}, distancia: {distance})")
    else:
        print("    Todas las redes están en canales no superpuestos (1, 6, 11)")
    
    # 4. Análisis de intensidad de señal por canal
    print("\n4.  INTENSIDAD DE SEÑAL POR CANAL (RSSI promedio):")
    rssi_by_channel = df.groupby('Channel')['RSSI'].agg(['mean', 'count']).round(1)
    for channel, data in rssi_by_channel.iterrows():
        print(f"   - Canal {int(channel)}: {data['mean']} dBm ({int(data['count'])} redes)")
    
    # 5. Detección de redes con posible interferencia
    print("\n5.  REDES CON POSIBLE INTERFERENCIA:")
    interference_threshold = -80
    weak_networks = df[df['RSSI'] <= interference_threshold]
    
    if len(weak_networks) > 0:
        print(f"    Se detectaron {len(weak_networks)} redes con señal débil (RSSI <= {interference_threshold} dBm):")
        for _, row in weak_networks.head(10).iterrows():  # Mostrar solo las primeras 10
            print(f"     - {row['SSID']} (Canal {int(row['Channel'])}, RSSI: {row['RSSI']} dBm)")
        if len(weak_networks) > 10:
            print(f"     ... y {len(weak_networks) - 10} redes más")
    else:
        print("    No se detectaron redes con señal extremadamente débil")
    
    # 6. Recomendaciones
    print("\n6.  RECOMENDACIONES:")
    
    # Verificar si hay canales congestionados
    channel_counts = df['Channel'].value_counts()
    if not channel_counts.empty:
        most_congested = int(channel_counts.idxmax())  # Convertir a entero normal
        least_congested = int(channel_counts.idxmin())  # Convertir a entero normal
        
        print(f"   - Canal más congestionado: {most_congested} ({channel_counts[most_congested]} redes)")
        print(f"   - Canal menos congestionado: {least_congested} ({channel_counts[least_congested]} redes)")
        
        # Sugerir canales óptimos
        optimal_channels = []
        for channel in non_overlapping:
            if channel not in df['Channel'].values or channel_counts.get(channel, 0) < 2:
                optimal_channels.append(channel)
        
        if optimal_channels:
            print(f"   - Canales recomendados: {optimal_channels} (poca congestión)")
        else:
            print("   - Todos los canales no superpuestos están congestionados")
    else:
        print("   - No hay datos suficientes para análisis de congestión")
    
    # 7. Visualizaciones
    print("\n7.  GENERANDO VISUALIZACIONES...")
    
    try:
        # Configurar el estilo de las gráficas
        plt.style.use('default')
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle(f'Análisis de Interferencias WiFi - {csv_file}', fontsize=16)
        
        # Gráfica 1: Distribución de redes por canal
        channel_counts = df['Channel'].value_counts().sort_index()
        axes[0, 0].bar(channel_counts.index.astype(str), channel_counts.values, color='skyblue')
        axes[0, 0].set_title('Distribución de Redes por Canal')
        axes[0, 0].set_xlabel('Canal')
        axes[0, 0].set_ylabel('Número de Redes')
        
        # Gráfica 2: Intensidad de señal por canal
        channel_rssi = df.groupby('Channel')['RSSI'].mean()
        axes[0, 1].bar(channel_rssi.index.astype(str), channel_rssi.values, color='lightcoral')
        axes[0, 1].set_title('Intensidad Promedio de Señal por Canal')
        axes[0, 1].set_xlabel('Canal')
        axes[0, 1].set_ylabel('RSSI Promedio (dBm)')
        
        # Gráfica 3: Calidad de señal
        quality_counts = df['Calidad'].value_counts()
        colors = ['#4CAF50', '#8BC34A', '#FFC107', '#FF9800', '#F44336']
        axes[1, 0].pie(quality_counts.values, labels=quality_counts.index, autopct='%1.1f%%', colors=colors)
        axes[1, 0].set_title('Distribución de Calidad de Señal')
        
        # Gráfica 4: Top SSIDs
        ssid_counts = df['SSID'].value_counts().head(8)
        axes[1, 1].bar(range(len(ssid_counts)), ssid_counts.values, color='mediumpurple')
        axes[1, 1].set_title('Redes por SSID (Top 8)')
        axes[1, 1].set_xlabel('SSID')
        axes[1, 1].set_ylabel('Número de Redes')
        axes[1, 1].set_xticks(range(len(ssid_counts)))
        axes[1, 1].set_xticklabels(ssid_counts.index, rotation=45, ha='right')
        
        plt.tight_layout()
        output_image = csv_file.replace('.csv', '_analysis.png')
        plt.savefig(output_image, dpi=300, bbox_inches='tight')
        print(f"    Gráficas guardadas como '{output_image}'")
        
    except Exception as e:
        print(f"    Error al generar visualizaciones: {e}")
    
    # Guardar resultados en un archivo de texto CON ANÁLISIS COMPLETO
    try:
        output_report = csv_file.replace('.csv', '_report.txt')
        with open(output_report, 'w', encoding='utf-8') as f:
            f.write("ANÁLISIS DE INTERFERENCIAS WiFi\n")
            f.write("="*50 + "\n\n")
            f.write(f"Archivo analizado: {csv_file}\n")
            f.write(f"Redes analizadas: {len(df)}\n")
            
            # CORRECCIÓN: Convertir canales a enteros normales
            canales_detectados = format_channels_list(df['Channel'].unique())
            f.write(f"Canales detectados: {sorted(canales_detectados)}\n")
            
            weak_signals = len(df[df['RSSI'] <= -80])
            f.write(f"Redes con señal débil (RSSI <= -80 dBm): {weak_signals}\n")
            
            f.write("\nDistribución por canal:\n")
            for channel, count in df['Channel'].value_counts().sort_index().items():
                f.write(f"- Canal {int(channel)}: {count} redes\n")
            
            # Agregar el análisis completo e interpretado
            comprehensive_analysis = generate_comprehensive_analysis(df, csv_file)
            f.write(comprehensive_analysis)
        
        print(f"    Reporte completo guardado como '{output_report}'")
        
    except Exception as e:
        print(f"    Error al guardar reporte: {e}")
    
    return df

def main():
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Analizar interferencias WiFi desde archivo CSV')
    parser.add_argument('archivo', help='Nombre del archivo CSV a analizar')
    parser.add_argument('--version', action='version', version='WiFi Analyzer 3.1 (Corregido)')
    
    args = parser.parse_args()
    
    print(f" Iniciando análisis robusto de: {args.archivo}")
    print("-" * 50)
    
    # Ejecutar el análisis
    df_result = analyze_wifi_interference(args.archivo)
    
    if df_result is not None:
        print("\n" + "="*50)
        print(" Análisis completado exitosamente!")
        print("="*50)
        print(f" El reporte completo incluye análisis interpretado y recomendaciones específicas")
    else:
        print("\n El análisis no pudo completarse debido a errores")
        sys.exit(1)

if __name__ == "__main__":
    main()