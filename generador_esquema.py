import pandas as pd
from pathlib import Path

def extraer_esquema(ruta_excel_perfecto: str):
    print("⏳ Leyendo el Excel base... (esto puede tardar unos segundos)\n")
    
    # 1. Extraer columnas de D2 (Fila de títulos = 3)
    df_d2 = pd.read_excel(ruta_excel_perfecto, sheet_name="D2", header=3)
    columnas_d2 = df_d2.columns.tolist()
    
    # 2. Extraer columnas de PF (Fila de títulos = 0)
    df_pf = pd.read_excel(ruta_excel_perfecto, sheet_name="PF", header=0)
    columnas_pf = df_pf.columns.tolist()

    # 3. Formatear la salida como código Python listo para copiar y pegar
    print("# === COPIA Y PEGA ESTO EN TU ARCHIVO ajustes.py ===\n")
    
    print("COLUMNAS_BASE_D2 = [")
    for col in columnas_d2:
        # Filtramos las columnas fantasma por si el Excel base las tiene
        if not str(col).startswith("Unnamed"):
            print(f'    "{col}",')
    print("]\n")

    print("COLUMNAS_BASE_PF = [")
    for col in columnas_pf:
        if not str(col).startswith("Unnamed"):
            print(f'    "{col}",')
    print("]")

if __name__ == "__main__":
    # Reemplaza esto con la ruta exacta de un Excel que sepas que está perfecto
    RUTA = r"C:\Users\Rodrigo\Downloads\base.xlsx"
    
    extraer_esquema(RUTA)