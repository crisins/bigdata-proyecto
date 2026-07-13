"""
   streamlit run dashboard/app.py
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent / "db"))
from db_utils import engine  

st.set_page_config(page_title="Panel de Control - Big Data", layout="wide")
st.title(" Panel de Control - Proyecto Big Data")
st.caption("Taxis de New York (batch) + Subasta de componentes electrónicos (streaming)")

tab1, tab2, tab3, tab4 = st.tabs([
    " Disponibilidad por zona/horario",
    " Tarifas entre puntos",
    " Precios en tiempo real",
    " Calidad y trazabilidad de datos",
])


with tab1:
    st.subheader("¿En qué horarios hay mayor disponibilidad de taxis por zona?")

    query = """
        SELECT pu_location_id AS zona, pickup_hour AS hora, COUNT(*) AS n_viajes
        FROM taxi_trips
        GROUP BY pu_location_id, pickup_hour
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        st.info("Aún no hay datos batch cargados. Corre batch/ingest_taxi_batch.py primero.")
    else:
        zonas_top = df.groupby("zona")["n_viajes"].sum().sort_values(ascending=False).head(15).index
        df_top = df[df["zona"].isin(zonas_top)]

        pivot = df_top.pivot_table(index="zona", columns="hora", values="n_viajes", fill_value=0)
        fig = px.imshow(
            pivot, aspect="auto", color_continuous_scale="YlOrRd",
            labels=dict(x="Hora del día", y="Zona (PULocationID)", color="N° viajes"),
            title="Mapa de calor: viajes iniciados por zona y hora (top 15 zonas)",
        )
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            por_hora = df.groupby("hora")["n_viajes"].sum().reset_index()
            st.plotly_chart(
                px.bar(por_hora, x="hora", y="n_viajes", title="Total de viajes por hora del día"),
                use_container_width=True,
            )
        with col2:
            por_zona = df.groupby("zona")["n_viajes"].sum().reset_index().sort_values("n_viajes", ascending=False).head(10)
            st.plotly_chart(
                px.bar(por_zona, x="zona", y="n_viajes", title="Top 10 zonas con más viajes (mayor disponibilidad histórica)"),
                use_container_width=True,
            )

with tab2:
    st.subheader("¿Cuánto se cobra entre un punto y otro de la ciudad?")

    query = """
        SELECT pu_location_id AS origen, do_location_id AS destino,
               AVG(fare_amount) AS tarifa_promedio, AVG(trip_distance) AS distancia_promedio,
               COUNT(*) AS n_viajes
        FROM taxi_trips
        GROUP BY pu_location_id, do_location_id
        HAVING COUNT(*) >= 20
        ORDER BY n_viajes DESC
        LIMIT 25
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        st.info("Aún no hay datos batch cargados.")
    else:
        df["ruta"] = df["origen"].astype(str) + " → " + df["destino"].astype(str)
        st.plotly_chart(
            px.bar(df.sort_values("tarifa_promedio", ascending=True), x="tarifa_promedio", y="ruta",
                   orientation="h", title="Tarifa promedio por ruta (top 25 rutas más frecuentes)",
                   labels={"tarifa_promedio": "Tarifa promedio (USD)", "ruta": "Ruta (origen → destino)"}),
            use_container_width=True,
        )
        st.dataframe(df, use_container_width=True)

# ---------------------------------------------------------------------
# TAB 3: Streaming - precios de componentes en tiempo real
# ---------------------------------------------------------------------
with tab3:
    st.subheader("Fluctuación de precios de componentes electrónicos (streaming)")

    query = """
        SELECT component_id, component_name, price, cantidad, monto, forma_pago, cliente, event_timestamp
        FROM component_prices ORDER BY event_timestamp
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        st.info("Aún no han llegado eventos streaming. Corre transform/transform_streaming.py tras recibir datos en la API.")
    else:
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        df["etiqueta"] = df["component_name"].fillna(df["component_id"])

        st.plotly_chart(
            px.line(df, x="event_timestamp", y="price", color="etiqueta", markers=True,
                    title="Evolución de precio por producto",
                    labels={"etiqueta": "Producto", "event_timestamp": "Fecha/hora", "price": "Precio"}),
            use_container_width=True,
        )
        ultimo_precio = df.sort_values("event_timestamp").groupby("etiqueta").tail(1)
        st.metric("Eventos streaming procesados", len(df))
        st.dataframe(ultimo_precio[["etiqueta", "price", "event_timestamp"]], use_container_width=True)

        st.divider()
        st.subheader("Reportes de negocio adicionales (streaming)")

        col1, col2 = st.columns(2)

        with col1:
            if df["monto"].notna().any():
                ingresos = df.groupby("etiqueta")["monto"].sum().reset_index().sort_values("monto", ascending=False)
                st.plotly_chart(
                    px.bar(ingresos, x="etiqueta", y="monto", title="Ingresos totales por producto",
                           labels={"etiqueta": "Producto", "monto": "Monto total"}),
                    use_container_width=True,
                )
            else:
                st.info("Sin datos de 'monto' todavía.")

        with col2:
            if df["cantidad"].notna().any():
                unidades = df.groupby("etiqueta")["cantidad"].sum().reset_index().sort_values("cantidad", ascending=False)
                st.plotly_chart(
                    px.bar(unidades, x="etiqueta", y="cantidad", title="Unidades transadas por producto",
                           labels={"etiqueta": "Producto", "cantidad": "Unidades"}),
                    use_container_width=True,
                )
            else:
                st.info("Sin datos de 'cantidad' todavía.")

        if df["forma_pago"].notna().any():
            pagos = df["forma_pago"].value_counts().reset_index()
            pagos.columns = ["forma_pago", "cantidad_eventos"]
            st.plotly_chart(
                px.pie(pagos, names="forma_pago", values="cantidad_eventos",
                       title="Distribución de eventos por forma de pago"),
                use_container_width=True,
            )
        else:
            st.info("Sin datos de 'forma_pago' todavía.")


with tab4:
    st.subheader("Métricas de calidad y trazabilidad de cada ejecución")
    query = """
        SELECT proceso, tipo, fecha_ejecucion, estado, registros_leidos,
               registros_cargados, registros_rechazados, registros_duplicados
        FROM control_ejecucion ORDER BY fecha_ejecucion DESC
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        st.info("Aún no hay ejecuciones registradas.")
    else:
        st.dataframe(df, use_container_width=True)
        col1, col2, col3 = st.columns(3)
        col1.metric("Total registros cargados", int(df["registros_cargados"].sum()))
        col2.metric("Total rechazados", int(df["registros_rechazados"].sum()))
        col3.metric("Total duplicados evitados", int(df["registros_duplicados"].sum()))
