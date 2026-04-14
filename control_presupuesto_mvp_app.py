import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

DB_PATH = "presupuesto.db"

st.set_page_config(page_title="Control de Presupuesto", layout="wide")

# =========================
# Base de datos
# =========================

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS articulos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT UNIQUE,
            nombre TEXT NOT NULL,
            categoria TEXT,
            subcategoria TEXT,
            activo INTEGER DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS presupuesto (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anio INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            articulo_codigo TEXT NOT NULL,
            monto_presupuestado REAL DEFAULT 0,
            unidades_presupuestadas REAL DEFAULT 0,
            version TEXT DEFAULT 'Base',
            fecha_carga TEXT,
            UNIQUE(anio, mes, articulo_codigo, version)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ventas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            anio INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            articulo_codigo TEXT NOT NULL,
            monto_venta REAL DEFAULT 0,
            unidades_venta REAL DEFAULT 0
        )
    """)

    conn.commit()
    conn.close()


def seed_demo_data():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM articulos")
    if cur.fetchone()[0] > 0:
        conn.close()
        return

    articulos = [
        ("A001", "Arroz 1kg", "Abarrotes", "Granos"),
        ("A002", "Frijoles 1kg", "Abarrotes", "Granos"),
        ("A003", "Aceite 900ml", "Abarrotes", "Cocina"),
        ("A004", "Azúcar 1kg", "Abarrotes", "Endulzantes"),
    ]

    cur.executemany(
        "INSERT OR IGNORE INTO articulos (codigo, nombre, categoria, subcategoria) VALUES (?, ?, ?, ?)",
        articulos,
    )

    for anio in [2025, 2026]:
        for mes in range(1, 13):
            for codigo, _, _, _ in articulos:
                base = 10000 + (mes * 300)
                mult = {"A001": 1.0, "A002": 0.9, "A003": 1.2, "A004": 0.8}[codigo]

                if anio == 2026:
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO presupuesto
                        (anio, mes, articulo_codigo, monto_presupuestado, unidades_presupuestadas, version, fecha_carga)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            anio,
                            mes,
                            codigo,
                            round(base * mult * 1.08, 2),
                            round((base * mult * 1.08) / 10, 2),
                            "Base",
                            datetime.now().isoformat(),
                        ),
                    )

                venta = base * mult * (0.92 if anio == 2025 else 1.03)
                cur.execute(
                    """
                    INSERT INTO ventas
                    (fecha, anio, mes, articulo_codigo, monto_venta, unidades_venta)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"{anio}-{mes:02d}-01",
                        anio,
                        mes,
                        codigo,
                        round(venta, 2),
                        round(venta / 10, 2),
                    ),
                )

    conn.commit()
    conn.close()


# =========================
# Helpers
# =========================

def query_df(sql, params=None):
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params or ())
    conn.close()
    return df


def upsert_articulos(df):
    conn = get_conn()
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO articulos (codigo, nombre, categoria, subcategoria)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(codigo) DO UPDATE SET
                nombre=excluded.nombre,
                categoria=excluded.categoria,
                subcategoria=excluded.subcategoria
            """,
            (
                str(row["codigo"]),
                str(row["nombre"]),
                str(row.get("categoria", "")),
                str(row.get("subcategoria", "")),
            ),
        )
    conn.commit()
    conn.close()


def upsert_presupuesto(df):
    conn = get_conn()
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO presupuesto
            (anio, mes, articulo_codigo, monto_presupuestado, unidades_presupuestadas, version, fecha_carga)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(anio, mes, articulo_codigo, version) DO UPDATE SET
                monto_presupuestado=excluded.monto_presupuestado,
                unidades_presupuestadas=excluded.unidades_presupuestadas,
                fecha_carga=excluded.fecha_carga
            """,
            (
                int(row["anio"]),
                int(row["mes"]),
                str(row["articulo_codigo"]),
                float(row.get("monto_presupuestado", 0) or 0),
                float(row.get("unidades_presupuestadas", 0) or 0),
                str(row.get("version", "Base")),
                datetime.now().isoformat(),
            ),
        )
    conn.commit()
    conn.close()


def insert_ventas(df):
    conn = get_conn()
    cur = conn.cursor()
    for _, row in df.iterrows():
        fecha = pd.to_datetime(row["fecha"])
        cur.execute(
            """
            INSERT INTO ventas
            (fecha, anio, mes, articulo_codigo, monto_venta, unidades_venta)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                fecha.strftime("%Y-%m-%d"),
                int(fecha.year),
                int(fecha.month),
                str(row["articulo_codigo"]),
                float(row.get("monto_venta", 0) or 0),
                float(row.get("unidades_venta", 0) or 0),
            ),
        )
    conn.commit()
    conn.close()


def get_dashboard_df(anio, version="Base"):
    sql = """
    WITH ventas_actual AS (
        SELECT anio, mes, articulo_codigo, SUM(monto_venta) AS venta_actual
        FROM ventas
        WHERE anio = ?
        GROUP BY anio, mes, articulo_codigo
    ),
    ventas_prev AS (
        SELECT mes, articulo_codigo, SUM(monto_venta) AS venta_anio_pasado
        FROM ventas
        WHERE anio = ?
        GROUP BY mes, articulo_codigo
    ),
    presup AS (
        SELECT anio, mes, articulo_codigo, SUM(monto_presupuestado) AS presupuesto
        FROM presupuesto
        WHERE anio = ? AND version = ?
        GROUP BY anio, mes, articulo_codigo
    )
    SELECT
        p.anio,
        p.mes,
        p.articulo_codigo,
        a.nombre AS articulo,
        a.categoria,
        COALESCE(p.presupuesto, 0) AS presupuesto,
        COALESCE(v.venta_actual, 0) AS venta_actual,
        COALESCE(vp.venta_anio_pasado, 0) AS venta_anio_pasado,
        COALESCE(v.venta_actual, 0) - COALESCE(p.presupuesto, 0) AS variacion_presupuesto,
        CASE
            WHEN COALESCE(p.presupuesto, 0) = 0 THEN NULL
            ELSE COALESCE(v.venta_actual, 0) / p.presupuesto
        END AS cumplimiento,
        CASE
            WHEN COALESCE(vp.venta_anio_pasado, 0) = 0 THEN NULL
            ELSE (COALESCE(v.venta_actual, 0) - vp.venta_anio_pasado) / vp.venta_anio_pasado
        END AS crecimiento_vs_anio_pasado
    FROM presup p
    LEFT JOIN ventas_actual v
        ON p.anio = v.anio AND p.mes = v.mes AND p.articulo_codigo = v.articulo_codigo
    LEFT JOIN ventas_prev vp
        ON p.mes = vp.mes AND p.articulo_codigo = vp.articulo_codigo
    LEFT JOIN articulos a
        ON p.articulo_codigo = a.codigo
    ORDER BY p.mes, p.articulo_codigo
    """
    return query_df(sql, (anio, anio - 1, anio, version))


def format_pct(x):
    if pd.isna(x):
        return "-"
    return f"{x:.1%}"


# =========================
# UI
# =========================

init_db()
seed_demo_data()

st.title("📊 Control de Presupuesto")
st.caption("MVP para controlar presupuesto mensual por artículo y compararlo con ventas del año pasado")

menu = st.sidebar.radio(
    "Menú",
    ["Dashboard", "Artículos", "Cargar presupuesto", "Cargar ventas", "Plantillas"]
)

if menu == "Dashboard":
    años_disponibles = query_df("SELECT DISTINCT anio FROM presupuesto ORDER BY anio DESC")
    if años_disponibles.empty:
        st.warning("No hay presupuesto cargado.")
        st.stop()

    col1, col2, col3 = st.columns(3)
    with col1:
        anio = st.selectbox("Año", años_disponibles["anio"].tolist(), index=0)
    with col2:
        versiones = query_df("SELECT DISTINCT version FROM presupuesto WHERE anio = ? ORDER BY version", (anio,))
        version = st.selectbox("Versión", versiones["version"].tolist(), index=0)
    with col3:
        categorias = query_df("SELECT DISTINCT categoria FROM articulos WHERE categoria IS NOT NULL AND categoria <> '' ORDER BY categoria")
        categoria = st.selectbox("Categoría", ["Todas"] + categorias["categoria"].tolist())

    df = get_dashboard_df(anio, version)
    if categoria != "Todas":
        df = df[df["categoria"] == categoria]

    if df.empty:
        st.info("No hay datos para mostrar.")
        st.stop()

    total_presupuesto = df["presupuesto"].sum()
    total_venta = df["venta_actual"].sum()
    total_anio_pasado = df["venta_anio_pasado"].sum()
    cumplimiento = (total_venta / total_presupuesto) if total_presupuesto else None
    crecimiento = ((total_venta - total_anio_pasado) / total_anio_pasado) if total_anio_pasado else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Presupuesto", f"{total_presupuesto:,.2f}")
    c2.metric("Venta real", f"{total_venta:,.2f}")
    c3.metric("Cumplimiento", format_pct(cumplimiento))
    c4.metric("Vs año pasado", format_pct(crecimiento))

    resumen_mes = df.groupby("mes", as_index=False)[["presupuesto", "venta_anio_pasado"]].sum()
    resumen_mes["variacion"] = resumen_mes["presupuesto"] - resumen_mes["venta_anio_pasado"]

    nombres_meses_chart = {
        1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
    }
    resumen_mes["Mes"] = resumen_mes["mes"].map(nombres_meses_chart)
    resumen_mes = resumen_mes.sort_values("mes")

    st.subheader("Evolución mensual")
    import altair as alt

    chart_data = resumen_mes.copy()

    base = alt.Chart(chart_data).encode(
        x=alt.X('Mes:N', sort=list(nombres_meses_chart.values()))
    )

    line_presupuesto = base.mark_line(color='blue').encode(
        y='presupuesto:Q'
    )
    puntos_presupuesto = base.mark_point(color='blue', size=60).encode(
        y='presupuesto:Q'
    )

    line_anio_pasado = base.mark_line(color='green').encode(
        y='venta_anio_pasado:Q'
    )
    puntos_anio_pasado = base.mark_point(color='green', size=60).encode(
        y='venta_anio_pasado:Q'
    )

    bar_variacion = base.mark_bar(opacity=0.45).encode(
        y='variacion:Q',
        color=alt.condition(
            alt.datum.variacion >= 0,
            alt.value('green'),
            alt.value('red')
        )
    )

    # (eliminado duplicado de subheader arriba si existe)
    st.subheader("Evolución mensual")
    st.altair_chart(bar_variacion + line_presupuesto + puntos_presupuesto + line_anio_pasado + puntos_anio_pasado, use_container_width=True)
    st.subheader("Detalle por artículo")
    col_m1, col_m2 = st.columns(2)
    with col_m1:
        tipo_matriz = st.selectbox(
            "Métrica a mostrar en la matriz",
            ["Venta real", "Presupuesto", "Venta año pasado", "Variación vs presupuesto"],
            index=0,
        )
    with col_m2:
        tipo_orden = st.selectbox(
            "Ordenar artículos por",
            ["Código", "Nombre", "Total descendente", "Total ascendente"],
            index=0,
        )

    mapa_metricas = {
        "Venta real": "venta_actual",
        "Presupuesto": "presupuesto",
        "Venta año pasado": "venta_anio_pasado",
        "Variación vs presupuesto": "variacion_presupuesto",
    }

    columna_valor = mapa_metricas[tipo_matriz]
    matriz = df.pivot_table(
        index=["articulo_codigo", "articulo"],
        columns="mes",
        values=columna_valor,
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    meses_esperados = list(range(1, 13))
    for m in meses_esperados:
        if m not in matriz.columns:
            matriz[m] = 0

    matriz["Total"] = matriz[meses_esperados].sum(axis=1)

    nombres_meses = {
        1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
    }

    matriz = matriz.rename(columns=nombres_meses)
    columnas_mes = [nombres_meses[m] for m in meses_esperados]
    columnas_finales = ["articulo_codigo", "articulo"] + columnas_mes + ["Total"]
    matriz = matriz[columnas_finales]

    if tipo_orden == "Código":
        matriz = matriz.sort_values(by=["articulo_codigo", "articulo"], ascending=[True, True])
    elif tipo_orden == "Nombre":
        matriz = matriz.sort_values(by=["articulo"], ascending=[True])
    elif tipo_orden == "Total descendente":
        matriz = matriz.sort_values(by=["Total", "articulo"], ascending=[False, True])
    elif tipo_orden == "Total ascendente":
        matriz = matriz.sort_values(by=["Total", "articulo"], ascending=[True, True])

    editable = matriz.copy()

    # Solo permitir edición si es Presupuesto
    if tipo_matriz == "Presupuesto":
        columnas_editables = columnas_mes  # meses
    else:
        columnas_editables = []

    edited_df = st.data_editor(
        editable,
        use_container_width=True,
        disabled=[col for col in editable.columns if col not in columnas_editables],
        key="editor_matriz"
    )

    # Guardar cambios si es presupuesto
    if tipo_matriz == "Presupuesto" and st.button("Guardar cambios de presupuesto"):
        df_updates = []
        for _, row in edited_df.iterrows():
            codigo = row["articulo_codigo"]
            for i, mes in enumerate(meses_esperados):
                valor = row[nombres_meses[mes]]
                df_updates.append({
                    "anio": anio,
                    "mes": mes,
                    "articulo_codigo": codigo,
                    "monto_presupuestado": valor,
                    "version": version
                })

        df_updates = pd.DataFrame(df_updates)
        upsert_presupuesto(df_updates)
        st.success("Presupuesto actualizado correctamente")

    from io import BytesIO

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        matriz.to_excel(writer, index=False, sheet_name="Matriz")
    output.seek(0)

    st.download_button(
        label="Descargar matriz en Excel",
        data=output.getvalue(),
        file_name=f"matriz_detalle_articulos_{anio}_{tipo_matriz.lower().replace(' ', '_')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

elif menu == "Artículos":
    st.subheader("Catálogo de artículos")
    articulos_df = query_df("SELECT codigo, nombre, categoria, subcategoria, activo FROM articulos ORDER BY codigo")
    st.dataframe(articulos_df, use_container_width=True)

    st.markdown("### Cargar artículos por Excel o CSV")
    archivo = st.file_uploader("Sube archivo de artículos", type=["xlsx", "csv"])
    st.caption("Columnas requeridas: codigo, nombre. Opcionales: categoria, subcategoria")

    if archivo:
        if archivo.name.endswith(".csv"):
            df = pd.read_csv(archivo)
        else:
            df = pd.read_excel(archivo)
        st.dataframe(df.head(), use_container_width=True)
        if st.button("Guardar artículos"):
            upsert_articulos(df)
            st.success("Artículos cargados correctamente.")
            st.rerun()

elif menu == "Cargar presupuesto":
    st.subheader("Importar presupuesto")
    st.caption("Columnas: anio, mes, articulo_codigo, monto_presupuestado, unidades_presupuestadas, version")
    archivo = st.file_uploader("Sube archivo de presupuesto", type=["xlsx", "csv"])

    if archivo:
        if archivo.name.endswith(".csv"):
            df = pd.read_csv(archivo)
        else:
            df = pd.read_excel(archivo)
        if "version" not in df.columns:
            df["version"] = "Base"
        st.dataframe(df.head(), use_container_width=True)
        if st.button("Guardar presupuesto"):
            upsert_presupuesto(df)
            st.success("Presupuesto cargado correctamente.")
            st.rerun()

elif menu == "Cargar ventas":
    st.subheader("Importar ventas")
    st.caption("Columnas: fecha, articulo_codigo, monto_venta, unidades_venta")
    archivo = st.file_uploader("Sube archivo de ventas", type=["xlsx", "csv"])

    if archivo:
        if archivo.name.endswith(".csv"):
            df = pd.read_csv(archivo)
        else:
            df = pd.read_excel(archivo)
        st.dataframe(df.head(), use_container_width=True)
        if st.button("Guardar ventas"):
            insert_ventas(df)
            st.success("Ventas cargadas correctamente.")
            st.rerun()

elif menu == "Plantillas":
    st.subheader("Plantillas sugeridas")

    plantilla_articulos = pd.DataFrame([
        {"codigo": "A001", "nombre": "Producto ejemplo", "categoria": "Categoría", "subcategoria": "Subcategoría"}
    ])

    plantilla_presupuesto = pd.DataFrame([
        {
            "anio": 2026,
            "mes": 1,
            "articulo_codigo": "A001",
            "monto_presupuestado": 15000,
            "unidades_presupuestadas": 1500,
            "version": "Base",
        }
    ])

    plantilla_ventas = pd.DataFrame([
        {
            "fecha": "2026-01-15",
            "articulo_codigo": "A001",
            "monto_venta": 14800,
            "unidades_venta": 1490,
        }
    ])

    st.markdown("### Artículos")
    st.dataframe(plantilla_articulos, use_container_width=True)

    st.markdown("### Presupuesto")
    st.dataframe(plantilla_presupuesto, use_container_width=True)

    st.markdown("### Ventas")
    st.dataframe(plantilla_ventas, use_container_width=True)

    st.info("Puedes copiar estas estructuras y cargarlas en CSV o Excel.")
