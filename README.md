# Proyecto Big Data — Taxis NY (batch) + Subasta de componentes (streaming)

Cubre el **Parcial 3 (AVY1101)** y la **Evaluación Final Transversal (BIY7131)**: son el mismo
proyecto, la EFT amplía el alcance (arquitectura, gobierno de datos, streaming propio vía API).

## 1. Arquitectura elegida: Medallion (Bronze → Silver → Gold)

Arquitectura de la industria (usada por Databricks, Microsoft, Netflix, etc.) que organiza
los datos en tres capas según su nivel de curación:

- **Bronze**: datos crudos, tal como llegan de la fuente. Nunca se modifican (trazabilidad).
- **Silver**: datos limpios, validados, deduplicados y normalizados.
- **Gold**: datos agregados/listos para consumo de negocio (lo que alimenta el dashboard).

**Por qué esta arquitectura y no otra (justificación para el informe):**
- *Objetivos comerciales*: separa "lo que llegó" de "lo que es confiable", permitiendo
  responder preguntas de negocio sin arriesgar la integridad del dato original.
- *Escalabilidad*: cada capa se puede escalar o reprocesar independientemente.
- *Calidad de datos*: la capa Silver es el punto único donde se aplican reglas de limpieza,
  evitando lógica de validación dispersa.
- *Trazabilidad / gobierno*: como Bronze nunca se borra, siempre se puede reconstruir el
  dato final desde el origen (ciclo de vida del dato completo).
- *Herramientas de código abierto*: FastAPI, Pandas, PostgreSQL, Streamlit — sin licencias.

## 2. Stack tecnológico y por qué

| Componente | Herramienta | Por qué |
|---|---|---|
| API de ingesta streaming | **FastAPI** (Python) | Async, liviana, documentación automática, ideal para un endpoint POST simple |
| Hosting de la API | **Render** (free tier) | URL pública fija y gratuita — se registra una sola vez en la plataforma Duoc |
| Base de datos (Gold) | **PostgreSQL en Neon** | Solución cloud gratuita, serverless, cumple el requisito de "tecnología cloud" de la rúbrica |
| Procesamiento batch/transform | **Pandas** | Estándar de la industria para transformación de datos tabulares |
| Dashboard | **Streamlit + Plotly** | Panel interactivo en Python puro, rápido de conectar a Postgres |
| Orquestación | Scripts Python ejecutados manualmente o por cron | Suficiente para el alcance del proyecto; se documenta como punto de mejora (Airflow) |

## 3. Estructura del proyecto

```
bigdata_project/
├── api/
│   └── main.py                  # API FastAPI: endpoint POST que registrarán en Duoc
├── batch/
│   └── ingest_taxi_batch.py     # Pipeline batch completo: Bronze -> Silver -> Gold
├── transform/
│   └── transform_streaming.py   # Pipeline streaming: Bronze -> Silver -> Gold
├── db/
│   ├── schema.sql                # Modelo de datos final (Gold) + tabla de control
│   └── db_utils.py               # Conexión a BD (Postgres o SQLite local)
├── dashboard/
│   └── app.py                    # Panel de control interactivo (Streamlit)
├── data_lake/                    # Se genera solo al ejecutar (Bronze/Silver locales)
├── requirements.txt
├── render.yaml                   # Config de despliegue en Render
└── .env.example
```

## 4. Cómo probarlo YA, en local (antes de desplegar)

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Cargar los datos batch de taxis (usa SQLite local automáticamente si no hay DATABASE_URL)
python batch/ingest_taxi_batch.py --file ruta/a/yellow_tripdata_2022-01.parquet --periodo 2022-01

# 3. Levantar la API en una terminal
python -m uvicorn api.main:app --reload --port 8000

# 4. En otra terminal, simular un evento de streaming (como lo haría Duoc)
curl -X POST http://localhost:8000/api/subasta/ingesta \
  -H "Content-Type: application/json" \
  -d '{"component_id":"CPU-1","component_name":"CPU","price":350.5,"timestamp":"2026-07-11T10:00:00Z"}'

# 5. Procesar lo que llegó a Bronze streaming -> Silver -> Gold
python transform/transform_streaming.py

# 6. Ver el dashboard
streamlit run dashboard/app.py
```

Ya probamos exactamente este flujo con tu archivo real `yellow_tripdata_2022-01.parquet`:
**2.463.931 registros leídos → 2.448.350 cargados** (se descartaron ~15.000 por fechas
corruptas fuera de enero 2022, detectadas en la validación) y **349 duplicados evitados**.

## 5. Cómo desplegarlo para la demo real (URL pública)

### Paso A: Crear la base de datos en Neon
1. Ir a https://neon.tech → crear cuenta gratis → "New Project"
2. Copiar el **connection string** (empieza con `postgresql://...`)

### Paso B: Subir el proyecto a GitHub
```bash
cd bigdata_project
git init
git add .
git commit -m "Proyecto Big Data - versión inicial"
# crear un repo en github.com y luego:
git remote add origin https://github.com/<tu-usuario>/<tu-repo>.git
git push -u origin main
```

### Paso C: Desplegar la API en Render
1. https://render.com → "New Web Service" → conectar el repo de GitHub
2. Render detecta `render.yaml` automáticamente (o configurar manualmente):
   - Build command: `pip install -r requirements.txt`
   - Start command: `python -m uvicorn api.main:app --host 0.0.0.0 --port $PORT`
3. En "Environment", agregar la variable `DATABASE_URL` con el connection string de Neon
4. Deploy → Render entrega una URL pública tipo `https://bigdata-api-ingesta.onrender.com`

### Paso D: Registrar el endpoint en la plataforma de Duoc
En `https://bdrealtimeescuelait.duoc.cl`, en el campo **URL**, registrar:
```
https://bigdata-api-ingesta.onrender.com/api/subasta/ingesta
```
(recordar que el endpoint debe ser POST — el nuestro lo es)

### Paso E: Ejecutar el pipeline batch apuntando a Neon
```bash
export DATABASE_URL="postgresql://usuario:password@ep-xxxx.neon.tech/proyecto_bigdata?sslmode=require"
python batch/ingest_taxi_batch.py --file yellow_tripdata_2022-01.parquet --periodo 2022-01
```

### Paso F: Dashboard el día de la defensa
```bash
export DATABASE_URL="<mismo connection string de Neon>"
streamlit run dashboard/app.py
```

> **Nota sobre el disco de Render (free tier):** es efímero — se reinicia si el servicio
> se redespliega o duerme por inactividad. Para la demo esto no es problema (el servicio
> se mantiene activo mientras reciben eventos), pero es un punto válido para mencionar en
> el análisis de arquitectura ("trade-off": para producción real se usaría almacenamiento
> persistente tipo S3/GCS para la capa Bronze streaming).

## 6. Controles implementados (Anexo del informe)

| Control | Cómo se implementó |
|---|---|
| **Control de errores** | Try/except en cada etapa; eventos con JSON inválido o esquema incompleto se guardan en `streaming_rechazados/` sin perder el dato; la API responde 200 aunque haya advertencias, para no bloquear al emisor |
| **Control de duplicidad** | Batch: hash MD5 de llave natural (`vendor_id+pickup+PU+DO+fare`) con `drop_duplicates` + chequeo contra lo ya cargado en Gold. Streaming: hash de `(component_id+timestamp+price)` |
| **Registro de actividad** | Tabla `control_ejecucion`: cada corrida queda registrada con estado, leídos/cargados/rechazados/duplicados. `proceso_ya_ejecutado()` evita reprocesar un período ya cargado con éxito |
| **Validación de datos y procesos** | Batch: filtra fechas fuera de rango, distancias/tarifas negativas, dropoff antes que pickup. Streaming: exige `component_id` y `price`, descarta precios negativos |

## 7. Mapeo directo a los Indicadores de Logro (para no dejar nada fuera)

| IL | Dónde se cubre |
|---|---|
| IL 1.1 / 1.2 | Sección 1 y 2 de este README (qué es Big Data aplicado acá, herramientas usadas) |
| IL 1.3 | Sección 6 (gobierno de datos y ciclo de vida: Bronze nunca se borra, control_ejecucion trazabilidad) |
| IL 1.4 | Sección 1 (justificación de la arquitectura Medallion) |
| IL 2.1 / IL 3.1 | `batch/ingest_taxi_batch.py` (carga batch) y `api/main.py` (ingesta streaming vía API) |
| IL 2.3 / IL 3.2 | Funciones `_limpiar_y_transformar()` en ambos pipelines (normalización, agregación, enriquecimiento, validación, limpieza, deduplicación) |
| IL 3.3 | `dashboard/app.py` (panel interactivo con 4 reportes) |

## 8. Preguntas de negocio que el dashboard responde

1. ¿Qué horarios tienen mayor disponibilidad de taxis por zona? → Tab 1 (mapa de calor)
2. ¿Qué tarifas se cobran entre un punto y otro? → Tab 2 (tarifa promedio por ruta)
3. ¿Cómo fluctúan los precios de los componentes en tiempo real? → Tab 3 (serie de tiempo)
4. ¿Qué tan confiables son los datos cargados? → Tab 4 (métricas de calidad)
