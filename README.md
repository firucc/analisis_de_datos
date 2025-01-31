# Proyecto ETL con Airflow, MySQL y Docker 🚀

Este repositorio contiene un pipeline ETL (Extracción, Transformación y Carga) que utiliza Python para procesar datos desde archivos CSV y una base de datos MySQL. 
Los datos transformados se insertan en un **Data Warehouse (DW)** OLAP denominado `dw_netflix`, diseñado para análisis avanzado. Toda la configuración del entorno se gestiona mediante contenedores Docker.

---

## 🛠️ Requisitos

1. **Docker** y **Docker Compose** instalados.
2. **Python 3.8+** (opcional si deseas ejecutar los scripts localmente).

---

## 🚀 Configuración e instalación

**1. Clona este repositorio:**

git clone <URL_DEL_REPOSITORIO>
cd PROYECTO_ETL

**2. Configura los contenedores Docker: Levanta los servicios definidos en docker-compose.yml:**

docker-compose up -d

**3. Verifica que los contenedores estén corriendo:**

docker ps

**4. Carga los datos iniciales: Coloca los archivos CSV en la carpeta data/ según el formato esperado.**

📦 Uso del ETL

**5. Ejecuta el script principal: El script ETL.py realizará las siguientes tareas:**

- Leer los archivos CSV desde la carpeta data/.
- Extraer datos de la base de datos MySQL del contenedor.
- Transformar y limpiar los datos.
- Insertar los datos en el Data Warehouse OLAP dw_netflix.

```
python scripts/ETL.py
```

Verifica los logs: El archivo logs/etl_process.log contiene información detallada del proceso ETL.

🗃️ Data Warehouse

El Data Warehouse OLAP está configurado en el contenedor MySQL bajo el esquema dw_netflix. Contiene las tablas necesarias para análisis avanzado y generación de reportes.
Verifica que las rutas de los archivos CSV sean correctas y que los datos estén completos.
