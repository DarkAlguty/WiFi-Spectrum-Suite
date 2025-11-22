import pandas as pd
import numpy as np
import argparse
import sys
import os
from datetime import datetime

def analyze_date_problems(csv_file):
    """
    Analiza específicamente problemas con formatos de fecha en el archivo CSV
    """
    print(f"ANALIZANDO PROBLEMAS DE FECHA EN: {csv_file}")
    print("=" * 60)
    
    try:
        # Leer el archivo línea por línea
        with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
        
        print(f"Total de líneas en el archivo: {len(lines)}")
        
        if len(lines) < 2:
            print("El archivo está vacío o tiene muy pocas líneas")
            return None, []
        
        # Analizar estructura de encabezados
        print("\nANALIZANDO ESTRUCTURA:")
        headers = lines[1].strip().split(',')  # Segunda línea como encabezados
        print(f"Encabezados detectados ({len(headers)}): {headers}")
        
        # Buscar la columna que debería contener fechas
        date_columns = []
        for i, header in enumerate(headers):
            if any(keyword in header.upper() for keyword in ['TIME', 'DATE', 'SEEN', 'FIRST', 'LAST']):
                date_columns.append((i, header))
        
        print(f"Columnas potencialmente de fecha: {date_columns}")
        
        # Analizar muestras de datos de las columnas de fecha
        date_samples = {}
        problematic_lines = []
        
        for line_num, line in enumerate(lines[2:12], start=3):  # Primeras 10 líneas de datos
            if line_num >= len(lines):
                break
                
            fields = line.strip().split(',')
            for col_idx, col_name in date_columns:
                if col_idx < len(fields):
                    value = fields[col_idx]
                    if col_name not in date_samples:
                        date_samples[col_name] = []
                    date_samples[col_name].append(value)
                    
                    # Verificar si el valor parece una fecha
                    if not looks_like_date(value):
                        problematic_lines.append({
                            'line': line_num,
                            'column': col_name,
                            'value': value,
                            'reason': 'No parece fecha'
                        })
        
        print(f"\nMUESTRAS DE FECHAS:")
        for col_name, samples in date_samples.items():
            print(f"  {col_name}: {samples}")
        
        return headers, problematic_lines, date_columns
        
    except Exception as e:
        print(f"ERROR durante el análisis: {e}")
        return None, [], []

def looks_like_date(value):
    """
    Determina si un valor parece ser una fecha
    """
    if not value or value.strip() == '':
        return False
    
    # Patrones comunes de fecha
    date_patterns = [
        r'\d{4}-\d{2}-\d{2}',
        r'\d{2}/\d{2}/\d{4}',
        r'\d{2}-\d{2}-\d{4}',
        r'\d{4}/\d{2}/\d{2}',
    ]
    
    value_str = str(value).strip()
    
    # Verificar patrones
    for pattern in date_patterns:
        if re.search(pattern, value_str):
            return True
    
    # Verificar si contiene componentes de fecha
    date_keywords = ['2024', '2025', '2023', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 
                    'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'am', 'pm']
    
    if any(keyword in value_str.lower() for keyword in date_keywords):
        return True
    
    return False

def repair_date_issues(csv_file, output_file=None):
    """
    Repara problemas específicos de formato de fecha en el archivo CSV
    """
    if output_file is None:
        output_file = csv_file.replace('.csv', '_fixed.csv')
    
    print(f"\nREPARANDO PROBLEMAS DE FECHA")
    print("=" * 50)
    
    headers, problematic_lines, date_columns = analyze_date_problems(csv_file)
    
    if not headers:
        print("No se puede proceder con la reparación")
        return None
    
    try:
        # Leer el archivo original
        with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
        
        repaired_lines = []
        corrections_made = 0
        date_column_indices = [idx for idx, name in date_columns]
        
        print(f"\nAPLICANDO CORRECCIONES DE FECHA...")
        
        for line_num, line in enumerate(lines):
            original_line = line.strip()
            
            # Mantener las primeras dos líneas (metadatos y encabezados) sin cambios
            if line_num < 2:
                repaired_lines.append(original_line)
                continue
            
            fields = original_line.split(',')
            
            # Reparar campos de fecha
            line_corrected = False
            for col_idx in date_column_indices:
                if col_idx < len(fields):
                    original_value = fields[col_idx]
                    repaired_value = repair_date_field(original_value)
                    
                    if repaired_value != original_value:
                        fields[col_idx] = repaired_value
                        line_corrected = True
                        corrections_made += 1
                        
                        if corrections_made <= 5:  # Mostrar solo las primeras 5 correcciones
                            print(f"  Línea {line_num}: '{original_value}' → '{repaired_value}'")
            
            repaired_lines.append(','.join(fields))
            
            # Mostrar progreso cada 100 líneas
            if line_num % 100 == 0 and line_num > 0:
                print(f"  Procesadas {line_num} líneas...")
        
        # Guardar archivo reparado
        with open(output_file, 'w', encoding='utf-8') as f:
            for line in repaired_lines:
                f.write(line + '\n')
        
        print(f"\nREPARACIÓN DE FECHAS COMPLETADA")
        print("=" * 40)
        print(f"ESTADÍSTICAS:")
        print(f"   - Archivo original: {csv_file}")
        print(f"   - Archivo reparado: {output_file}")
        print(f"   - Líneas procesadas: {len(repaired_lines)}")
        print(f"   - Correcciones de fecha aplicadas: {corrections_made}")
        print(f"   - Columnas de fecha identificadas: {[name for idx, name in date_columns]}")
        
        return output_file
        
    except Exception as e:
        print(f"ERROR durante la reparación: {e}")
        return None

def repair_date_field(value):
    """
    Repara un campo individual que debería ser una fecha
    """
    if not value or value.strip() == '':
        return value
    
    value_str = str(value).strip()
    
    # Caso 1: El valor "OPEN" - reemplazar con fecha actual
    if value_str.upper() == 'OPEN':
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Caso 2: Valores que claramente no son fechas
    non_date_values = ['WPA2', 'WPA', 'WEP', 'OPN', 'OPEN', 'UNKNOWN', 'N/A', 'NULL']
    if value_str.upper() in non_date_values:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Caso 3: Fechas en formato incorrecto pero reconocible
    try:
        # Intentar parsear varios formatos de fecha
        formats_to_try = [
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y %H:%M:%S',
            '%m/%d/%Y %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%m-%d-%Y %H:%M:%S',
            '%Y%m%d%H%M%S',
        ]
        
        for fmt in formats_to_try:
            try:
                parsed_date = datetime.strptime(value_str, fmt)
                return parsed_date.strftime('%Y-%m-%d %H:%M:%S')  # Convertir a formato estándar
            except ValueError:
                continue
    except:
        pass
    
    # Si no se pudo reparar, devolver el valor original
    return value_str

def validate_date_repair(repaired_file):
    """
    Valida que las fechas en el archivo reparado sean correctas
    """
    print(f"\n🔍 VALIDANDO REPARACIÓN DE FECHAS: {repaired_file}")
    
    try:
        # Leer con pandas para validar
        df = pd.read_csv(repaired_file, skiprows=1)
        
        print("INFORMACIÓN DEL DATAFRAME REPARADO:")
        print(f"   - Filas: {len(df)}")
        print(f"   - Columnas: {list(df.columns)}")
        
        # Identificar columnas que parecen ser de fecha
        date_cols = []
        for col in df.columns:
            if any(keyword in col.upper() for keyword in ['TIME', 'DATE', 'SEEN', 'FIRST', 'LAST']):
                date_cols.append(col)
        
        print(f"   - Columnas de fecha identificadas: {date_cols}")
        
        # Analizar las columnas de fecha
        for col in date_cols:
            if col in df.columns:
                print(f"\nANÁLISIS DE LA COLUMNA '{col}':")
                
                # Mostrar tipos de datos únicos
                unique_types = df[col].apply(lambda x: type(x).__name__).unique()
                print(f"   - Tipos de datos: {unique_types}")
                
                # Mostrar algunos valores únicos
                unique_values = df[col].dropna().unique()
                print(f"   - Valores únicos (primeros 5): {unique_values[:5]}")
                
                # Intentar convertir a datetime
                try:
                    date_series = pd.to_datetime(df[col], errors='coerce', format='mixed')
                    valid_dates = date_series.notna().sum()
                    invalid_dates = date_series.isna().sum()
                    
                    print(f"   - Fechas válidas: {valid_dates}")
                    print(f"   - Fechas inválidas: {invalid_dates}")
                    
                    if valid_dates > 0:
                        print(f"   - Rango de fechas: {date_series.min()} a {date_series.max()}")
                    
                except Exception as e:
                    print(f"  Error al convertir fechas: {e}")
        
        print(f"\nVALIDACIÓN COMPLETADA")
        return True
        
    except Exception as e:
        print(f" ERROR durante la validación: {e}")
        return False

def create_smart_date_loader(csv_file):
    """
    Crea un script personalizado para cargar el CSV con manejo inteligente de fechas
    """
    loader_script = f"""
# SCRIPT DE CARGA INTELIGENTE PARA: {os.path.basename(csv_file)}
# Generado automáticamente por el reparador de fechas

import pandas as pd
import numpy as np

def load_wifi_data(csv_file):
    '''
    Función inteligente para cargar datos WiFi con manejo robusto de fechas
    '''
    
    # Estrategia 1: Intentar carga directa con parser personalizado
    try:
        df = pd.read_csv(
            csv_file,
            skiprows=1,
            parse_dates=['FirstSeen'],  # Ajustar según tus columnas de fecha
            dayfirst=True,
            infer_datetime_format=True
        )
        print("Datos cargados con parser de fechas integrado")
        return df
    except Exception as e:
        print(f"Error con parser integrado: {{e}}")
    
    # Estrategia 2: Cargar como texto y luego convertir
    try:
        df = pd.read_csv(csv_file, skiprows=1, dtype=str)
        
        # Conversión manual de fechas
        date_columns = ['FirstSeen', 'LastSeen']  # Ajustar según tus columnas
        
        for col in date_columns:
            if col in df.columns:
                # Intentar múltiples formatos
                df[col] = pd.to_datetime(
                    df[col], 
                    errors='coerce',
                    format='mixed',
                    dayfirst=True
                )
        
        print("Datos cargados con conversión manual de fechas")
        return df
    except Exception as e:
        print(f"Error con conversión manual: {{e}}")
    
    # Estrategia 3: Carga básica sin conversión de fechas
    try:
        df = pd.read_csv(csv_file, skiprows=1)
        print("Datos cargados sin conversión de fechas")
        return df
    except Exception as e:
        print(f"Error con carga básica: {{e}}")
        return None

# Uso:
# df = load_wifi_data('{csv_file}')
"""
    
    loader_file = csv_file.replace('.csv', '_smart_loader.py')
    with open(loader_file, 'w') as f:
        f.write(loader_script)
    
    print(f"Script de carga inteligente guardado como: {loader_file}")
    return loader_file

def main():
    """
    Script principal para depuración específica de problemas de fecha
    """
    parser = argparse.ArgumentParser(description='Depuración específica de problemas de fecha en archivos CSV')
    parser.add_argument('archivo', help='Archivo CSV a reparar')
    parser.add_argument('-o', '--output', help='Archivo de salida (opcional)')
    parser.add_argument('--solo-analisis', action='store_true', help='Solo analizar sin reparar')
    parser.add_argument('--validar', action='store_true', help='Validar archivo reparado')
    parser.add_argument('--crear-loader', action='store_true', help='Crear script de carga inteligente')
    
    args = parser.parse_args()
    
    # Verificar que el archivo existe
    if not os.path.exists(args.archivo):
        print(f"Error: El archivo '{args.archivo}' no existe")
        sys.exit(1)
    
    print("REPARADOR ESPECIALIZADO EN PROBLEMAS DE FECHA")
    print("=" * 50)
    
    if args.solo_analisis:
        # Solo análisis
        headers, problems, date_columns = analyze_date_problems(args.archivo)
        if problems:
            print(f"\nRESUMEN: Se encontraron {len(problems)} problemas de fecha")
        else:
            print(f"\nRESUMEN: No se encontraron problemas de fecha evidentes")
    else:
        # Reparación completa
        repaired_file = repair_date_issues(args.archivo, args.output)
        
        if repaired_file and args.validar:
            validate_date_repair(repaired_file)
        
        if args.crear_loader:
            create_smart_date_loader(args.archivo)
    
    print("\n" + "=" * 50)
    print("PROCESO COMPLETADO")

# Import necesario para las funciones de fecha
import re

if __name__ == "__main__":
    main()