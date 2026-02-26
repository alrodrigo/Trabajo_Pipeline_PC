"""punto de entrada para el pipeline P&C"""
import argparse
import sys
from src.ajustes import FILESERVER_LOCAL, FILESERVER_PRODUCCION
from src.procesador_principal import ProcesadorPrincipa
def main() -> None:
    #1. Configuracion de argumentos de la terminal
    parser = argparse.ArgumentParser(
        description = "Sistema de Extraccion P&C: Excel a parquet"

    )
    parser.add_argument(
        "--produccion",
        action="store_true",
        help="Usar ruta del fileserver corporativo en vez del emulado local",
    )
    parser.add_argument(
        "--forzar",
        action="store_true",
        help="Reprocesar absolutamente todos los archivos",
    )
    args = parser.parse_args()
    #2. Definir origen de datos
    ruta_base = FILESERVER_PRODUCCION if args.produccion else FILESERVER_LOCAL

    #3. Validacion de acceso
    if not ruta_base.exists():
        print(f"\n[ERROR] No se puede acceder a la ruta : {ruta_base}")
        print("Asegurate de estar conectado a la red VPN de la empresa o que la carpeta exista")
        sys.exit(1)
    #4. Presentacion
    print("=" * 60)
    print("PIPELINE P&C - EXTRACCION DE DATOS")
    print(f"  Modo: {'PRODUCCIÓN' if args.produccion else 'DESARROLLO LOCAL'}")
    print(f"  Ruta: {ruta_base}")
    print(f"  Forzar: {'SÍ' if args.forzar else 'NO'}")
    print("=" * 60)
    print()

    # 5. Ejecución del motor
    try:
        # Instanciamos el procesador que creamos antes
        procesador = ProcesadorPrincipal(ruta_base=ruta_base, forzar=args.forzar)
        
        # Arrancamos el proceso
        resultado = procesador.ejecutar()

        # 6. Resumen final en pantalla
        print("\n" + "=" * 60)
        print(f"PROCESO FINALIZADO - ESTADO: {resultado['estado'].upper()}")
        stats = resultado['stats']
        print(f"  Archivos OK:       {stats['exitosos']}")
        print(f"  Archivos Error:    {stats['errores']}")
        print(f"  Archivos Omitidos: {stats['omitidos']}")
        print(f"  Tiempo total:      {resultado['duracion']} seg.")
        print("=" * 60)

    except Exception as e:
        print(f"\n[CRITICAL ERROR] Falló el motor principal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProceso cancelado por el usuario (Ctrl+C).")
        sys.exit(0)