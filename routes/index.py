import logging
import plotly.graph_objs as go
from quart import Blueprint, render_template, jsonify, request
from datetime import datetime, timedelta
from utils.weather_data import fetch_QuestWeatherStation_data, generate_timestamps
from database import fetch_all_rows
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from bisect import bisect_left

bp = Blueprint("index", __name__)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

html_template = "index.html"
executor = ThreadPoolExecutor(max_workers=35)

# -------------------- Global parameters -------------------- #
start_date = None   # e.g. "2024-06-01"
date_range = 21     # default days


# ----------------------------- helpers ----------------------------- #
def _to_iso(xvals):
    """Convert datetime objects to ISO8601 strings with 'T' for Plotly."""
    return [x.isoformat(timespec="seconds") if isinstance(x, datetime) else x for x in xvals]

def _strip_empty_traces(traces):
    return [t for t in traces if len(t["x"]) > 0 and len(t["y"]) > 0]

def _build_nearest_lookup(xs_sorted, max_gap: timedelta):
    """
    Return a lookup function that finds the nearest index in xs_sorted to t,
    but returns None if the surrounding gap exceeds max_gap.
    """
    def nearest_idx(t):
        i = bisect_left(xs_sorted, t)

        if i == 0:
            if len(xs_sorted) > 1 and (xs_sorted[1] - xs_sorted[0]) > max_gap:
                return None
            return 0

        if i == len(xs_sorted):
            if len(xs_sorted) > 1 and (xs_sorted[-1] - xs_sorted[-2]) > max_gap:
                return None
            return len(xs_sorted) - 1

        before, after = xs_sorted[i - 1], xs_sorted[i]

        # Reject if this neighborhood has a large gap
        if (after - before) > max_gap:
            return None

        return i - 1 if (t - before) <= (after - t) else i

    return nearest_idx
# ------------------------------------------------------------------- #

##//####### Health check ######################################################
@bp.route('/health')
def health():
    """Simple health check endpoint."""
    return "OK", 200
#============================== Routes: range getters/setters ==============================#
@bp.route("/get_range", methods=["GET"])
async def get_range():
    global start_date, date_range
    return jsonify(start_date=start_date, date_range=date_range)

@bp.route("/update_range", methods=["POST"])
async def update_range():
    global start_date, date_range
    data = await request.get_json()
    if "start_date" in data:
        start_date = data["start_date"]
    if "date_range" in data:
        try:
            date_range = int(data["date_range"])
        except Exception:
            logger.warning("Invalid date_range received, keeping previous value")
    logger.info(f"Updated range: start_date={start_date}, date_range={date_range}")
    return jsonify(success=True, start_date=start_date, date_range=date_range)


#============================== DB fetch helper ==============================#
async def _fetch_batched_sensor_data(sensorid_list, start_sql_str, end_sql_str, table_name="stationary_sensorpush_data"):
    if not sensorid_list:
        return []

    sensor_list_str = ", ".join(f"'{sid}'" for sid in sensorid_list)

    if table_name == "stationary_sensorpush_data":
        query = f"""
        SELECT t1.sensorid, t1.timestamp, t1.temperature, t2.owners_first_name
        FROM stationary_sensorpush_data t1
        INNER JOIN stationary_whitelist_sensor_meta_data t2 ON t1.sensorid = t2.sensor_name
        WHERE t1.sensorid IN ({sensor_list_str})
          AND t1.timestamp >= '{start_sql_str}'
          AND t1.timestamp < '{end_sql_str}'
        ORDER BY t1.timestamp ASC;
        """
    elif table_name == "stationary_LLNL_data":
        query = f"""
        SELECT * FROM stationary_LLNL_data
        WHERE sensorid IN ({sensor_list_str})
          AND timestamp >= '{start_sql_str}'
          AND timestamp < '{end_sql_str}'
        ORDER BY timestamp ASC;
        """

    elif table_name == "stationary_LVFERC_data":
        query = f"""
        SELECT * FROM stationary_LVFERC_data
        WHERE sensorid IN ({sensor_list_str})
          AND timestamp >= '{start_sql_str}'
          AND timestamp < '{end_sql_str}'
        ORDER BY timestamp ASC;
        """

    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(executor, fetch_all_rows, query)
    return rows


async def _fetch_urban_sensor_ids():
    query = r"""
        SELECT DISTINCT sensor_name FROM stationary_whitelist_sensor_meta_data
        WHERE sensor_name REGEXP '^Sensor[0-9]+$';
    """
    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(executor, fetch_all_rows, query)
    return [row["sensor_name"] for row in rows]



#============================== Data pipeline ==============================#
async def _fetch_and_process_data():
    global start_date, date_range

    # --- Compute range ---
    if start_date:
        start_dt = datetime.fromisoformat(start_date)
    else:
        start_dt = datetime.now() - timedelta(days=date_range)

    # Normalize start time to midnight
    start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    # End = start + N days, normalized to midnight
    end_dt = (start_dt + timedelta(days=date_range)).replace(hour=0, minute=0, second=0, microsecond=0)

    start_sql_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_sql_str   = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    start_iso_str = start_dt.isoformat(timespec="seconds")
    end_iso_str   = end_dt.isoformat(timespec="seconds")

    traces_celsius, traces_fahrenheit, traces_windspeed = [], [], []

    # --- Urban + LLNL + LVFERC DB data ---
    llnl_sensor_ids = ["Sensor52a", "Sensor52b", "Sensor52c", "Sensor52d"]
    lvferc_sensor_ids = ["Sensor60"]

    urban_sensor_ids = [
        sid for sid in await _fetch_urban_sensor_ids()
        if sid not in lvferc_sensor_ids
    ]

    urban_rows, llnl_rows, lvferc_rows = await asyncio.gather(
        _fetch_batched_sensor_data(urban_sensor_ids, start_sql_str, end_sql_str, "stationary_sensorpush_data"),
        _fetch_batched_sensor_data(llnl_sensor_ids, start_sql_str, end_sql_str, "stationary_LLNL_data"),
        _fetch_batched_sensor_data(lvferc_sensor_ids, start_sql_str, end_sql_str, "stationary_LVFERC_data")
    )
    logger.debug(f"Fetched {len(urban_rows)} urban rows, {len(llnl_rows)} LLNL rows, {len(lvferc_rows)} LVFERC rows")

    # --- Urban traces ---
    urban_data_by_sensor = defaultdict(list)
    for row in urban_rows:
        urban_data_by_sensor[row["sensorid"]].append(row)

    for sensorid, rows in urban_data_by_sensor.items():
        if not rows:
            continue
        timestamps = [datetime.fromisoformat(r["timestamp"]) for r in rows]
        temps_c = [r["temperature"] for r in rows]
        temps_f = [c * 9 / 5 + 32 for c in temps_c]
        extended_name = f"{sensorid} ({rows[0]['owners_first_name']})"
        traces_celsius.append(go.Scatter(x=_to_iso(timestamps), y=temps_c, mode="lines", name=extended_name))
        traces_fahrenheit.append(go.Scatter(x=_to_iso(timestamps), y=temps_f, mode="lines", name=extended_name))

    # --- LLNL traces ---
    llnl_data_by_sensor = defaultdict(list)
    for row in llnl_rows:
        llnl_data_by_sensor[row["sensorid"]].append(row)

    llnl_labels = ["LLNL (2m)", "LLNL (10m)", "LLNL (23m)", "LLNL (52m)"]
    for i, sid in enumerate(llnl_sensor_ids):
        rows = llnl_data_by_sensor.get(sid, [])
        if not rows:
            continue
        timestamps = [datetime.fromisoformat(r["timestamp"]) for r in rows]
        temps_c = [r["temperature"] for r in rows]
        temps_f = [c * 9 / 5 + 32 for c in temps_c]
        extended_name = f"{sid} ({llnl_labels[i]})"
        visible = True if i == 0 else "legendonly"
        traces_celsius.append(go.Scatter(x=_to_iso(timestamps), y=temps_c, mode="lines", name=extended_name, visible=visible))
        traces_fahrenheit.append(go.Scatter(x=_to_iso(timestamps), y=temps_f, mode="lines", name=extended_name, visible=visible))

    # --- LVFERC traces ---
    lvferc_data_by_sensor = defaultdict(list)
    for row in lvferc_rows:
        lvferc_data_by_sensor[row["sensorid"]].append(row)

    for sid in lvferc_sensor_ids:
        rows = lvferc_data_by_sensor.get(sid, [])
        if not rows:
            continue
        timestamps = [datetime.fromisoformat(r["timestamp"]) for r in rows]
        temps_c = [r["temperature"] for r in rows]
        temps_f = [c * 9 / 5 + 32 for c in temps_c]
        extended_name = f"{sid} (LVFERC)"
        traces_celsius.append(go.Scatter(x=_to_iso(timestamps), y=temps_c, mode="lines", name=extended_name))
        traces_fahrenheit.append(go.Scatter(x=_to_iso(timestamps), y=temps_f, mode="lines", name=extended_name))

    # --- Quest Weather Station API ---
    MAX_CONCURRENT_QUEST_CALLS = 3
    sem = asyncio.Semaphore(MAX_CONCURRENT_QUEST_CALLS)

    async def fetch_day(start_ts, end_ts):
        async with sem:
            async with aiohttp.ClientSession() as session:
                return await fetch_QuestWeatherStation_data(session, start_ts, end_ts)

    tasks = []
    end_ts = int(end_dt.timestamp())  # cutoff in POSIX seconds

    for start, end in generate_timestamps(start_dt, end_dt):
        if isinstance(start, datetime):
            start = int(start.timestamp())
        if isinstance(end, datetime):
            end = int(end.timestamp())

        if end > end_ts:
            end = end_ts
        if start < end_ts:
            tasks.append(fetch_day(start, end))

    responses = await asyncio.gather(*tasks)

    temps_api, ts_api, wind_api = [], [], []
    for data in responses:
        if not data or "sensors" not in data:
            continue
        for sensor in data["sensors"]:
            for record in sensor["data"]:
                if "temp_out" in record and "ts" in record:
                    temps_api.append((record["temp_out"] - 32) / 1.8)  # F→C
                    wind_api.append(record.get("wind_speed_avg"))
                    ts_api.append(datetime.fromtimestamp(record["ts"]))

    if temps_api and ts_api:
        iso_ts_api = _to_iso(ts_api)
        ws_name = "Sensor51 (Quest Weather Station)"
        traces_celsius.append(go.Scatter(x=iso_ts_api, y=temps_api, mode="lines", name=ws_name))
        traces_fahrenheit.append(go.Scatter(x=iso_ts_api, y=[t * 9/5 + 32 for t in temps_api], mode="lines", name=ws_name))
        traces_windspeed.append(go.Scatter(
            x=iso_ts_api, y=wind_api, mode="lines",
            name="Sensor51 (Quest Wind Speed)", yaxis="y2", visible="legendonly"
        ))

    return traces_celsius, traces_fahrenheit, traces_windspeed, start_iso_str, end_iso_str


#============================== Routes ==============================#
@bp.route("/")
async def index():
    try:
        traces_c, traces_f, traces_w, start_iso_str, end_iso_str = await _fetch_and_process_data()

        # Build figures
        fig_c = go.Figure(data=traces_c + traces_w)
        fig_f = go.Figure(data=traces_f + traces_w)

        # ---------------- Shaded AM background (00:00–12:00 each day) ---------------- #
        shapes = []
        start_dt = datetime.fromisoformat(start_iso_str)
        end_dt = datetime.fromisoformat(end_iso_str)
        cur = start_dt
        while cur < end_dt:
            shapes.append(dict(
                type="rect",
                xref="x", yref="paper",
                x0=cur.isoformat(),
                x1=(cur + timedelta(hours=12)).isoformat(),
                y0=0, y1=1,
                fillcolor="lightgray",
                opacity=0.4,
                layer="below",
                line_width=0
            ))
            cur += timedelta(days=1)

        # Celsius figure
        fig_c.update_layout(
            title=f"{start_iso_str.split('T')[0]}<br>Temperatures (°C) {start_iso_str} → {end_iso_str}",
            xaxis_title="Date/Time",
            yaxis=dict(title="Temperature (°C)"),
            yaxis2=dict(title="Wind Speed (m/s)", overlaying="y", side="right"),
            legend=dict(x=1.05, y=0.5),
            legend_title="Sensors - click toggles visibility",
            xaxis_range=[start_iso_str, end_iso_str],
            shapes=shapes
        ).update_xaxes(type="date")

        # Fahrenheit figure
        fig_f.update_layout(
            title=f"{start_iso_str.split('T')[0]}<br>Temperatures (°F) {start_iso_str} → {end_iso_str}",
            xaxis_title="Date/Time",
            yaxis=dict(title="Temperature (°F)"),
            yaxis2=dict(title="Wind Speed (m/s)", overlaying="y", side="right"),
            legend=dict(x=1.05, y=0.5),
            legend_title="Sensors - click toggles visibility",
            xaxis_range=[start_iso_str, end_iso_str],
            shapes=shapes
        ).update_xaxes(type="date")

        return await render_template(
            html_template,
            plot_data_celsius=fig_c.to_plotly_json(),
            plot_data_fahrenheit=fig_f.to_plotly_json()
        )

    except Exception:
        logger.exception("Exception in index route")
        return jsonify({"error": "Internal Server Error"}), 500


@bp.route("/get_plot_data", methods=["GET"])
async def get_plot_data():
    try:
        subtract_reference = request.args.get("subtract_reference", "false").lower() == "true"

        global start_date, date_range

        start_str_q = request.args.get("start_date", "").strip()
        if start_str_q:
            start_date = start_str_q
        if start_date:
            start_dt = datetime.fromisoformat(start_date)
        else:
            start_dt = datetime.now() - timedelta(days=date_range)
        start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)

        range_str_q = request.args.get("date_range", "").strip()
        if range_str_q:
            try:
                date_range = int(range_str_q)
            except Exception:
                logger.warning("Invalid date_range in query; keeping previous value")

        start_date = start_dt.date().isoformat()

        traces_c, traces_f, traces_w, start_iso_str, end_iso_str = await _fetch_and_process_data()

        if subtract_reference:
            ref_c = next((t for t in traces_c if "Sensor52a" in t["name"]), None)
            ref_f = next((t for t in traces_f if "Sensor52a" in t["name"]), None)

            if not ref_c or not ref_f:
                ref_c = ref_c or next((t for t in traces_c if "Quest Weather Station" in t["name"]), None)
                ref_f = ref_f or next((t for t in traces_f if "Quest Weather Station" in t["name"]), None)

            if ref_c and ref_f:
                ref_x_c = [datetime.fromisoformat(x) if isinstance(x, str) else x for x in ref_c["x"]]
                ref_y_c = list(ref_c["y"])
                ref_x_f = [datetime.fromisoformat(x) if isinstance(x, str) else x for x in ref_f["x"]]
                ref_y_f = list(ref_f["y"])

                nearest_idx_c = _build_nearest_lookup(ref_x_c, max_gap=timedelta(minutes=20))
                nearest_idx_f = _build_nearest_lookup(ref_x_f, max_gap=timedelta(minutes=20))

                sub_c, sub_f = [], []

                def should_subtract(name: str) -> bool:
                    return ("Sensor" in name or "Quest Weather Station" in name) and (name != ref_c["name"])

                for trace in traces_c:
                    if not should_subtract(trace["name"]):
                        continue
                    xs = [datetime.fromisoformat(x) if isinstance(x, str) else x for x in trace["x"]]
                    ys = trace["y"]
                    dx, dy = [], []
                    for x, y in zip(xs, ys):
                        j = nearest_idx_c(x)
                        if j is None:
                            dx.append(x)
                            dy.append(None)
                        else:
                            dx.append(x)
                            dy.append(y - ref_y_c[j])
                    sub_c.append(go.Scatter(
                        x=_to_iso(dx), y=dy, mode="lines+markers",
                        name=f"{trace['name']} (Δ vs {ref_c['name']})",
                        connectgaps=False
                    ))

                for trace in traces_f:
                    if not should_subtract(trace["name"]):
                        continue
                    xs = [datetime.fromisoformat(x) if isinstance(x, str) else x for x in trace["x"]]
                    ys = trace["y"]
                    dx, dy = [], []
                    for x, y in zip(xs, ys):
                        j = nearest_idx_f(x)
                        if j is None:
                            dx.append(x)
                            dy.append(None)
                        else:
                            dx.append(x)
                            dy.append(y - ref_y_f[j])
                    sub_f.append(go.Scatter(
                        x=_to_iso(dx), y=dy, mode="lines+markers",
                        name=f"{trace['name']} (Δ vs {ref_f['name']})",
                        connectgaps=False
                    ))

                if sub_c:
                    traces_c = _strip_empty_traces(sub_c)
                if sub_f:
                    traces_f = _strip_empty_traces(sub_f)

                # Add horizontal zero line
                x_range = [start_iso_str, end_iso_str]
                zero_line_c = go.Scatter(
                    x=x_range, y=[0, 0], mode="lines", name="ΔT = 0",
                    line=dict(color="blue", dash="dash")
                )
                zero_line_f = go.Scatter(
                    x=x_range, y=[0, 0], mode="lines", name="ΔT = 0",
                    line=dict(color="blue", dash="dash")
                )
                traces_c.append(zero_line_c)
                traces_f.append(zero_line_f)

        if traces_w:
            traces_w[0].update(yaxis="y2", visible=True)

        shapes = []
        cur = datetime.fromisoformat(start_iso_str)
        end_dt = datetime.fromisoformat(end_iso_str)
        while cur < end_dt:
            shapes.append(dict(
                type="rect",
                xref="x", yref="paper",
                x0=cur.isoformat(),
                x1=(cur + timedelta(hours=12)).isoformat(),
                y0=0, y1=1,
                fillcolor="lightgray",
                opacity=0.55,
                layer="below",
                line_width=0
            ))
            cur += timedelta(days=1)

        fig_c = go.Figure(data=traces_c + traces_w)
        fig_c.update_layout(
            title=f"{start_date}<br>Temperatures (°C) {start_iso_str} → {end_iso_str}",
            xaxis_title="Date/Time",
            yaxis=dict(title="Temperature (°C)"),
            yaxis2=dict(title="Wind Speed (m/s)", overlaying="y", side="right"),
            legend=dict(x=1.02, y=1.0),
            legend_title="Sensors",
            margin=dict(l=60, r=60, t=60, b=40),
            xaxis_range=[start_iso_str, end_iso_str],
            shapes=shapes
        ).update_xaxes(type="date")

        fig_f = go.Figure(data=traces_f + traces_w)
        fig_f.update_layout(
            title=f"{start_date}<br>Temperatures (°F) {start_iso_str} → {end_iso_str}",
            xaxis_title="Date/Time",
            yaxis=dict(title="Temperature (°F)"),
            yaxis2=dict(title="Wind Speed (m/s)", overlaying="y", side="right"),
            legend=dict(x=1.02, y=1.0),
            legend_title="Sensors",
            margin=dict(l=60, r=60, t=60, b=40),
            xaxis_range=[start_iso_str, end_iso_str],
            shapes=shapes
        ).update_xaxes(type="date")

        return jsonify(
            plot_data_celsius=fig_c.to_plotly_json(),
            plot_data_fahrenheit=fig_f.to_plotly_json(),
            start_date=start_date,
            date_range=date_range
        )

    except Exception:
        logger.exception("Exception in get_plot_data route")
        return jsonify({"error": "Internal Server Error"}), 500
