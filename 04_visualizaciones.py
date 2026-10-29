import csv
import glob
import os
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE_EDA = "/home/gio/tarea3/resultados/eda"
SALIDA = "/home/gio/tarea3/resultados/graficas"

os.makedirs(SALIDA, exist_ok=True)


def leer_csv(ruta_patron):
    archivo = glob.glob(ruta_patron)[0]

    with open(archivo, "r", encoding="utf-8") as archivo_csv:
        return list(csv.DictReader(archivo_csv))


# 1. Ventas por mes
ventas_mes = leer_csv(f"{BASE_EDA}/ventas_por_mes/part-*.csv")
meses = [fila["Mes"] for fila in ventas_mes]
ventas = [float(fila["VentasTotales"]) for fila in ventas_mes]

plt.figure(figsize=(11, 6))
plt.plot(meses, ventas, marker="o", color="#277da1", linewidth=2)
plt.title("Ventas mensuales - Online Retail")
plt.xlabel("Mes")
plt.ylabel("Ventas totales (£)")
plt.xticks(rotation=45)
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig(f"{SALIDA}/ventas_por_mes.png", dpi=180)
plt.close()


# 2. Principales países, sin Reino Unido para apreciar los demás mercados
ventas_pais = leer_csv(f"{BASE_EDA}/ventas_por_pais/part-*.csv")
paises = [fila for fila in ventas_pais if fila["Country"] != "United Kingdom"][:10]

nombres_paises = [fila["Country"] for fila in reversed(paises)]
ventas_paises = [float(fila["VentasTotales"]) for fila in reversed(paises)]

plt.figure(figsize=(10, 6))
plt.barh(nombres_paises, ventas_paises, color="#d97925")
plt.title("Diez principales mercados internacionales")
plt.xlabel("Ventas totales (£)")
plt.tight_layout()
plt.savefig(f"{SALIDA}/ventas_por_pais_sin_reino_unido.png", dpi=180)
plt.close()


# 3. Productos físicos con mayor valor de ventas
productos = leer_csv(f"{BASE_EDA}/productos_top_10/part-*.csv")

excluidos = {
    "POSTAGE",
    "DOTCOM POSTAGE",
    "MANUAL",
    "BANK CHARGES",
    "CARRIAGE"
}

productos = [
    fila for fila in productos
    if fila["Description"].strip().upper() not in excluidos
]

nombres_productos = [fila["Description"] for fila in reversed(productos)]
ventas_productos = [float(fila["VentasTotales"]) for fila in reversed(productos)]

plt.figure(figsize=(12, 7))
plt.barh(nombres_productos, ventas_productos, color="#368f5a")
plt.title("Productos físicos con mayor valor de ventas")
plt.xlabel("Ventas totales (£)")
plt.tight_layout()
plt.savefig(f"{SALIDA}/productos_top.png", dpi=180)
plt.close()


# 4. Clientes con mayor valor de compra
clientes = leer_csv(f"{BASE_EDA}/clientes_top_10/part-*.csv")

ids_clientes = [fila["CustomerID"] for fila in reversed(clientes)]
ventas_clientes = [float(fila["VentasTotales"]) for fila in reversed(clientes)]

plt.figure(figsize=(10, 6))
plt.barh(ids_clientes, ventas_clientes, color="#775da6")
plt.title("Diez clientes con mayor valor de compra")
plt.xlabel("Ventas totales (£)")
plt.ylabel("Identificador del cliente")
plt.tight_layout()
plt.savefig(f"{SALIDA}/clientes_top.png", dpi=180)
plt.close()

print("\nGráficas creadas correctamente en:")
print(SALIDA)
