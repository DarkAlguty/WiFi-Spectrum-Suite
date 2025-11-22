#!/usr/bin/env python3
"""
Analizador Avanzado de Wardriving WiFi - VERSIÓN COMPLETA
Script optimizado para análisis de datos de redes inalámbricas
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import folium
from folium.plugins import HeatMap
import argparse
import sys
import os
from datetime import datetime
import warnings
import csv
from typing import List, Dict, Any, Optional

# Configuración de estilo y warnings
plt.style.use('default')
sns.set_palette("husl")
warnings.filterwarnings("ignore", category=UserWarning, module="folium")
warnings.filterwarnings("ignore", category=FutureWarning)

class WardrivingAnalyzer:
    """Clase principal para análisis de datos de wardriving"""
    
    def __init__(self, archivo_csv: str):
        self.archivo_csv = archivo_csv
        self.df = None
        self.nombre_base = os.path.splitext(os.path.basename(archivo_csv))[0]
        
    def cargar_datos(self) -> bool:
        """Carga y prepara los datos del archivo CSV - VERSIÓN CORREGIDA"""
        try:
            if not os.path.exists(self.archivo_csv):
                print(f" Error: El archivo '{self.archivo_csv}' no existe")
                return False
            
            # Primero, intentamos leer el archivo con diferentes configuraciones
            try:
                # Intento 1: Leer con engine de Python que es más tolerante
                self.df = pd.read_csv(self.archivo_csv, skiprows=1, engine='python', 
                                     quoting=csv.QUOTE_MINIMAL, on_bad_lines='warn')
                print(" Datos cargados con engine de Python")
            except Exception as e:
                print(f"  Primer intento falló: {e}")
                print(" Intentando método alternativo...")
                
                # Intento 2: Leer sin skiprows y luego procesar manualmente
                temp_df = pd.read_csv(self.archivo_csv, engine='python', 
                                     quoting=csv.QUOTE_MINIMAL, on_bad_lines='skip')
                
                # Si la primera fila parece ser encabezado, la usamos
                if len(temp_df.columns) > 1 and any('SSID' in str(col) for col in temp_df.columns):
                    self.df = temp_df
                    print(" Datos cargados sin skiprows")
                else:
                    # Intento 3: Saltar primera fila manualmente
                    self.df = pd.read_csv(self.archivo_csv, skiprows=1, 
                                         error_bad_lines=False, warn_bad_lines=True)
                    print(" Datos cargados con error_bad_lines=False")
            
            if self.df is None or self.df.empty:
                print(" No se pudieron cargar datos válidos")
                return False
            
            # Limpiar nombres de columnas (eliminar espacios extra, etc.)
            self.df.columns = self.df.columns.str.strip()
            
            # Verificar columnas requeridas
            columnas_requeridas = ['SSID', 'FirstSeen', 'Channel', 'Frequency', 'RSSI', 
                                  'CurrentLatitude', 'CurrentLongitude', 'AuthMode']
            columnas_faltantes = [col for col in columnas_requeridas if col not in self.df.columns]
            
            if columnas_faltantes:
                print(f"  Columnas faltantes: {columnas_faltantes}")
                print(f" Columnas disponibles: {list(self.df.columns)}")
                
                # Intentar mapear nombres alternativos comunes
                mapeo_columnas = {
                    'SSID': ['SSID', 'ssid', 'Ssid'],
                    'FirstSeen': ['FirstSeen', 'First seen', 'firstseen', 'Timestamp'],
                    'Channel': ['Channel', 'channel', 'CH'],
                    'Frequency': ['Frequency', 'frequency', 'Freq'],
                    'RSSI': ['RSSI', 'rssi', 'Signal'],
                    'CurrentLatitude': ['CurrentLatitude', 'Latitude', 'Lat', 'latitude'],
                    'CurrentLongitude': ['CurrentLongitude', 'Longitude', 'Lon', 'longitude'],
                    'AuthMode': ['AuthMode', 'Authentication', 'Encryption', 'auth']
                }
                
                for col_requerida in columnas_faltantes:
                    for posible_nombre in mapeo_columnas.get(col_requerida, []):
                        if posible_nombre in self.df.columns:
                            self.df[col_requerida] = self.df[posible_nombre]
                            print(f" Mapeada columna '{posible_nombre}' a '{col_requerida}'")
                            break
            
            # Preparar datos
            try:
                self.df['Timestamp'] = pd.to_datetime(self.df['FirstSeen'], errors='coerce')
                
                # Limpiar y convertir datos numéricos
                if 'Channel' in self.df.columns:
                    self.df['Channel'] = pd.to_numeric(self.df['Channel'], errors='coerce').fillna(0).astype(int)
                
                if 'Frequency' in self.df.columns:
                    self.df['Frequency'] = pd.to_numeric(self.df['Frequency'], errors='coerce').fillna(0).astype(int)
                
                if 'RSSI' in self.df.columns:
                    self.df['RSSI'] = pd.to_numeric(self.df['RSSI'], errors='coerce')
                
                # Limpiar coordenadas
                if 'CurrentLatitude' in self.df.columns:
                    self.df['CurrentLatitude'] = pd.to_numeric(self.df['CurrentLatitude'], errors='coerce')
                    self.df = self.df.dropna(subset=['CurrentLatitude'])
                
                if 'CurrentLongitude' in self.df.columns:
                    self.df['CurrentLongitude'] = pd.to_numeric(self.df['CurrentLongitude'], errors='coerce')
                    self.df = self.df.dropna(subset=['CurrentLongitude'])
                
                print(f" Datos preparados: {len(self.df)} registros válidos")
                return True
                
            except Exception as e:
                print(f" Error al preparar datos: {e}")
                return False
            
        except Exception as e:
            print(f" Error al cargar datos: {e}")
            return False
    
    def analizar_general(self) -> Dict[str, Any]:
        """Realiza análisis general de los datos"""
        if self.df is None or self.df.empty:
            return {}
        
        try:
            resultados = {
                'total_registros': len(self.df),
                'periodo_captura': f"{self.df['FirstSeen'].min()} - {self.df['FirstSeen'].max()}",
                'redes_unicas': self.df['SSID'].nunique(),
                'top_redes': self.df['SSID'].value_counts().head(5).to_dict(),
                'metricas_rssi': {
                    'promedio': self.df['RSSI'].mean(),
                    'minimo': self.df['RSSI'].min(),
                    'maximo': self.df['RSSI'].max(),
                    'desviacion': self.df['RSSI'].std()
                }
            }
            
            return resultados
        except Exception as e:
            print(f"  Error en análisis general: {e}")
            return {}
    
    def generar_mapa_calor(self) -> str:
        """Genera mapa de calor de RSSI"""
        print("🌡️ Generando mapa de calor de RSSI...")
        
        try:
            centro_lat = self.df['CurrentLatitude'].mean()
            centro_lon = self.df['CurrentLongitude'].mean()
            
            mapa = folium.Map(
                location=[centro_lat, centro_lon],
                zoom_start=16,
                tiles='OpenStreetMap'
            )
            
            # Preparar datos para heatmap
            heat_data = []
            for _, row in self.df.iterrows():
                intensity = max(0.1, min(1.0, (row['RSSI'] + 100) / 40))
                heat_data.append([row['CurrentLatitude'], row['CurrentLongitude'], intensity])
            
            HeatMap(heat_data, radius=15, blur=10, max_zoom=1).add_to(mapa)
            
            archivo_mapa = f"mapa_calor_{self.nombre_base}.html"
            mapa.save(archivo_mapa)
            print(f" Mapa de calor guardado: {archivo_mapa}")
            
            return archivo_mapa
        except Exception as e:
            print(f" Error generando mapa de calor: {e}")
            return ""
    
    def generar_mapa_localizacion(self) -> str:
        """Genera mapa de localización con puntos de acceso"""
        print(" Generando mapa de localización...")
        
        try:
            centro_lat = self.df['CurrentLatitude'].mean()
            centro_lon = self.df['CurrentLongitude'].mean()
            
            mapa = folium.Map(
                location=[centro_lat, centro_lon],
                zoom_start=16,
                tiles='OpenStreetMap'
            )
            
            # Agrupar por SSID y coordenadas
            grupos = self.df.groupby(['SSID', 'CurrentLatitude', 'CurrentLongitude'])
            colores = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'darkblue']
            
            for color_idx, ((ssid, lat, lon), group) in enumerate(grupos):
                rssi_promedio = group['RSSI'].mean()
                cantidad = len(group)
                
                popup_text = f"""
                <b>{ssid}</b><br>
                Ubicación: {lat:.6f}, {lon:.6f}<br>
                RSSI: {rssi_promedio:.1f} dBm<br>
                Detecciones: {cantidad}
                """
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=8,
                    popup=popup_text,
                    color=colores[color_idx % len(colores)],
                    fill=True,
                    fillOpacity=0.6
                ).add_to(mapa)
            
            archivo_mapa = f"mapa_localizacion_{self.nombre_base}.html"
            mapa.save(archivo_mapa)
            print(f" Mapa de localización guardado: {archivo_mapa}")
            
            return archivo_mapa
        except Exception as e:
            print(f" Error generando mapa de localización: {e}")
            return ""
    
    def generar_graficos(self):
        """Genera gráficos avanzados de análisis"""
        print(" Generando gráficos avanzados...")
        
        try:
            # Usar 3x2 subplots para acomodar los 6 gráficos
            fig, axes = plt.subplots(3, 2, figsize=(15, 15))
            fig.suptitle(f'Análisis Wardriving - {self.nombre_base}', fontsize=16, fontweight='bold')
            
            # 1. Distribución de RSSI por canal (0,0)
            if 'Channel' in self.df.columns and 'RSSI' in self.df.columns:
                canales_unicos = sorted(self.df['Channel'].dropna().unique())
                datos_canales = [self.df[self.df['Channel'] == canal]['RSSI'] for canal in canales_unicos]
                
                axes[0, 0].boxplot(datos_canales, tick_labels=canales_unicos)
                axes[0, 0].set_title('Distribución de RSSI por Canal')
                axes[0, 0].set_xlabel('Canal')
                axes[0, 0].set_ylabel('RSSI (dBm)')
                axes[0, 0].grid(True, alpha=0.3)
            
            # 2. Heatmap de canales vs RSSI (0,1)
            if 'Channel' in self.df.columns and 'RSSI' in self.df.columns:
                canal_rssi = self.df.groupby('Channel')['RSSI'].mean().reset_index()
                scatter = axes[0, 1].scatter(canal_rssi['Channel'], canal_rssi['RSSI'], 
                                            c=canal_rssi['RSSI'], cmap='viridis', s=100)
                plt.colorbar(scatter, ax=axes[0, 1], label='RSSI Promedio (dBm)')
                axes[0, 1].set_title('RSSI Promedio por Canal')
                axes[0, 1].set_xlabel('Canal')
                axes[0, 1].set_ylabel('RSSI Promedio (dBm)')
                axes[0, 1].grid(True, alpha=0.3)
            
            # 3. Evolución temporal del RSSI (1,0)
            if 'Timestamp' in self.df.columns and 'RSSI' in self.df.columns:
                df_sorted = self.df.sort_values('Timestamp')
                axes[1, 0].plot(df_sorted['Timestamp'], df_sorted['RSSI'], 'o-', alpha=0.7, markersize=2)
                axes[1, 0].set_title('Evolución Temporal de la Señal')
                axes[1, 0].set_xlabel('Tiempo')
                axes[1, 0].set_ylabel('RSSI (dBm)')
                axes[1, 0].tick_params(axis='x', rotation=45)
                axes[1, 0].grid(True, alpha=0.3)
            
            # 4. Distribución de métodos de autenticación (1,1)
            if 'AuthMode' in self.df.columns:
                auth_counts = self.df['AuthMode'].value_counts()
                axes[1, 1].pie(auth_counts.values, labels=auth_counts.index, autopct='%1.1f%%')
                axes[1, 1].set_title('Métodos de Autenticación')
            
            # 5. Mapa de densidad (2,0)
            if all(col in self.df.columns for col in ['CurrentLongitude', 'CurrentLatitude', 'RSSI']):
                hexbin = axes[2, 0].hexbin(self.df['CurrentLongitude'], self.df['CurrentLatitude'], 
                                          C=self.df['RSSI'], gridsize=15, cmap='viridis', 
                                          reduce_C_function=np.mean)
                plt.colorbar(hexbin, ax=axes[2, 0], label='RSSI Promedio (dBm)')
                axes[2, 0].set_title('Densidad de Redes y Intensidad de Señal')
                axes[2, 0].set_xlabel('Longitud')
                axes[2, 0].set_ylabel('Latitud')
            
            # 6. Distribución de RSSI (2,1)
            if 'RSSI' in self.df.columns:
                axes[2, 1].hist(self.df['RSSI'], bins=20, alpha=0.7, edgecolor='black')
                axes[2, 1].set_title('Distribución de Intensidad de Señal')
                axes[2, 1].set_xlabel('RSSI (dBm)')
                axes[2, 1].set_ylabel('Frecuencia')
                axes[2, 1].grid(True, alpha=0.3)
            
            plt.tight_layout()
            archivo_graficos = f'graficos_avanzados_{self.nombre_base}.png'
            plt.savefig(archivo_graficos, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f" Gráficos guardados: {archivo_graficos}")
            return archivo_graficos
        except Exception as e:
            print(f" Error generando gráficos: {e}")
            return ""
    
    def generar_reporte(self):
        """Genera reporte detallado del análisis"""
        print("\n" + "=" * 60)
        print(f"REPORTE DETALLADO - {self.nombre_base}")
        print("=" * 60)
        
        if self.df is None or self.df.empty:
            print(" No hay datos para generar reporte")
            return
        
        # Información general
        analisis = self.analizar_general()
        if not analisis:
            print(" No se pudo realizar el análisis general")
            return
        
        print(f"\n INFORMACIÓN GENERAL:")
        print(f"Total de registros: {analisis['total_registros']}")
        print(f"Período de captura: {analisis['periodo_captura']}")
        print(f"Redes únicas detectadas: {analisis['redes_unicas']}")
        
        # Canales y frecuencias
        if 'Channel' in self.df.columns and 'Frequency' in self.df.columns:
            canales = sorted([int(c) for c in self.df['Channel'].dropna().unique() if pd.notna(c)])
            frecuencias = sorted([int(f) for f in self.df['Frequency'].dropna().unique() if pd.notna(f)])
            
            print(f"\n CANALES Y FRECUENCIAS:")
            print(f"Canales utilizados: {canales}")
            print(f"Frecuencias utilizadas: {frecuencias} MHz")
            print(f"Total canales: {len(canales)}, Total frecuencias: {len(frecuencias)}")
        
        # Métricas de señal
        print(f"\n MÉTRICAS DE SEÑAL:")
        rssi = analisis['metricas_rssi']
        print(f"RSSI promedio: {rssi['promedio']:.1f} dBm")
        print(f"RSSI mínimo: {rssi['minimo']} dBm, Máximo: {rssi['maximo']} dBm")
        
        # Top redes
        print(f"\n TOP 5 REDES:")
        for ssid, count in analisis['top_redes'].items():
            red_data = self.df[self.df['SSID'] == ssid]
            rssi_prom = red_data['RSSI'].mean() if 'RSSI' in red_data.columns else 0
            print(f"  - {ssid}: {count} detecciones | RSSI: {rssi_prom:.1f} dBm")
        
        # Análisis de seguridad
        if 'AuthMode' in self.df.columns:
            self._analizar_seguridad()
        
        # Análisis de calidad de señal
        if 'RSSI' in self.df.columns:
            self._analizar_calidad_señal()
        
        print(f"\n RECOMENDACIONES:")
        print("1. Analizar interferencias entre canales cercanos")
        print("2. Verificar seguridad de redes con encriptación débil")
        print("3  Optimizar ubicación de puntos de acceso")
        print("4. Considerar repetidores en áreas de señal débil")
    
    def _analizar_seguridad(self):
        """Análisis de seguridad de las redes"""
        print(f"\n ANÁLISIS DE SEGURIDAD:")
        
        # Redes abiertas
        redes_abiertas = self.df[self.df['AuthMode'] == 'OPEN']
        if not redes_abiertas.empty:
            print(f"  Redes abiertas detectadas: {len(redes_abiertas['SSID'].unique())}")
            for ssid in list(redes_abiertas['SSID'].unique())[:5]:  # Mostrar solo las primeras 5
                print(f"    - {ssid}")
        
        # Redes WEP (encriptación débil)
        redes_wep = self.df[self.df['AuthMode'] == 'WEP']
        if not redes_wep.empty:
            print(f"  Redes WEP (encriptación débil): {len(redes_wep['SSID'].unique())}")
            for ssid in list(redes_wep['SSID'].unique())[:5]:  # Mostrar solo las primeras 5
                print(f"    - {ssid}")
        
        # Redes con buena seguridad
        redes_seguras = self.df[self.df['AuthMode'].str.contains('WPA2', na=False)]
        if not redes_seguras.empty:
            print(f" Redes con encriptación WPA2: {len(redes_seguras['SSID'].unique())}")
    
    def _analizar_calidad_señal(self):
        """Análisis de calidad de señal"""
        print(f"\n CALIDAD DE SEÑAL:")
        
        excelente = len(self.df[self.df['RSSI'] > -65])
        buena = len(self.df[(self.df['RSSI'] >= -75) & (self.df['RSSI'] <= -65)])
        aceptable = len(self.df[(self.df['RSSI'] >= -85) & (self.df['RSSI'] < -75)])
        debil = len(self.df[self.df['RSSI'] < -85])
        
        total = len(self.df)
        
        print(f"Excelente (> -65 dBm): {excelente} ({excelente/total*100:.1f}%)")
        print(f"Buena (-65 a -75 dBm): {buena} ({buena/total*100:.1f}%)")
        print(f"Aceptable (-75 a -85 dBm): {aceptable} ({aceptable/total*100:.1f}%)")
        print(f"Débil (< -85 dBm): {debil} ({debil/total*100:.1f}%)")

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description='Analizador avanzado de datos de wardriving WiFi')
    parser.add_argument('archivo', help='Archivo CSV con datos de wardriving')
    parser.add_argument('--mapa-calor', '-mc', action='store_true', help='Generar mapa de calor')
    parser.add_argument('--mapa-localizacion', '-ml', action='store_true', help='Generar mapa de localización')
    parser.add_argument('--graficos', '-g', action='store_true', help='Generar gráficos')
    parser.add_argument('--reporte', '-r', action='store_true', help='Generar reporte')
    parser.add_argument('--todo', '-a', action='store_true', help='Ejecutar todas las opciones')
    
    args = parser.parse_args()
    
    # Crear analizador
    analyzer = WardrivingAnalyzer(args.archivo)
    
    if not analyzer.cargar_datos():
        print(" No se pudieron cargar los datos. Verifica el archivo CSV.")
        sys.exit(1)
    
    print("=" * 60)
    print(f"ANÁLISIS WARDRIVING - {analyzer.nombre_base}")
    print("=" * 60)
    
    # Ejecutar análisis seleccionado
    archivos_generados = []
    
    if args.todo or args.reporte:
        analyzer.generar_reporte()
    
    if args.todo or args.mapa_calor:
        archivo = analyzer.generar_mapa_calor()
        if archivo:
            archivos_generados.append(archivo)
    
    if args.todo or args.mapa_localizacion:
        archivo = analyzer.generar_mapa_localizacion()
        if archivo:
            archivos_generados.append(archivo)
    
    if args.todo or args.graficos:
        archivo = analyzer.generar_graficos()
        if archivo:
            archivos_generados.append(archivo)
    
    # Mostrar resumen
    if archivos_generados:
        print(f"\n ARCHIVOS GENERADOS:")
        for archivo in archivos_generados:
            print(f"   - {archivo}")
    
    print(f"\n Análisis completado exitosamente!")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Uso: python WiFi_Wardriving.py <archivo.csv> [opciones]")
        print("\nOpciones:")
        print("  --mapa-calor, -mc     Generar mapa de calor")
        print("  --mapa-localizacion, -ml  Generar mapa de localización")
        print("  --graficos, -g        Generar gráficos")
        print("  --reporte, -r         Generar reporte")
        print("  --todo, -a            Ejecutar todas las opciones")
        print("\nEjemplo: python WiFi_Wardriving.py datos.csv --todo")
        sys.exit(1)
    
    main()