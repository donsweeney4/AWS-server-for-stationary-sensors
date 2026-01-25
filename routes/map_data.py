from quart import Blueprint
import folium
from database import fetch_all_rows

bp = Blueprint('map_data', __name__)

@bp.route('/get_map_data')
async def get_map_data():
    try:
        # Query your table
        query = """
            SELECT sensor_id, sensor_name, owners_first_name,
                   date_installed, timestamp,
                   current_latitude, current_longitude
            FROM latest_sensor_meta_data;
        """
        rows = fetch_all_rows(query)   # assuming sync version

        # Base map (center Livermore)
        m = folium.Map(location=[37.6818745, -121.7680088],
                       zoom_start=13,
                       control_scale=True,
                       tiles="OpenStreetMap")
        
        # Add satellite view (Esri World Imagery)
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            attr="Tiles © Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community",
            name="Satellite",
            overlay=False,
            control=True
        ).add_to(m)

# Add layer control (lets user switch between base maps)
        folium.LayerControl().add_to(m)

        # Add markers + circles
        for row in rows:
            try:
                lat = float(row["current_latitude"])
                lon = float(row["current_longitude"])
                sid = str(row["sensor_id"])
                sname = row.get("sensor_name", "N/A")
                owner = row.get("owners_first_name", "N/A")
                date_installed = row.get("date_installed", "N/A")
                timestamp = row.get("timestamp", "N/A")

                # Street View link
                streetview_url = f"https://www.google.com/maps?q=&layer=c&cbll={lat},{lon}"

                popup_html = f"""
                <div style="font-size:14px; line-height:1.4;">
                    <b>Sensor {sid}</b><br>
                    <b>Name:</b> {sname}<br>
                    <b>Owner:</b> {owner}<br>
                    <b>Date Installed:</b> {date_installed}<br>
                    <b>Last Update:</b> {timestamp}<br>
                    <a href="{streetview_url}" target="_blank">Open Street View</a>
                </div>
                """

                # Circle with popup
                folium.Circle(
                    location=[lat, lon],
                    radius=250,
                    color="#FF0000",
                    weight=2,
                    fill=True,
                    fill_color="#FF0000",
                    fill_opacity=0.35,
                    popup=folium.Popup(popup_html, max_width=300)
                ).add_to(m)

                # Text label at the center
                folium.Marker(
                    location=[lat, lon],
                    icon=folium.DivIcon(html=f"""
                        <div style="font-size:16px; font-weight:bold; color:black;">{sid}</div>
                    """)
                ).add_to(m)

            except Exception as inner_e:
                print(f"Skipping row: {inner_e}")

        # Return Folium map HTML
        return m._repr_html_()

    except Exception as e:
        # Fallback: error map
        m = folium.Map(location=[37.6818745, -121.7680088], zoom_start=13)
        folium.Marker(
            location=[37.6818745, -121.7680088],
            popup=f"Error: {e}",
            icon=folium.Icon(color="red")
        ).add_to(m)
        return m._repr_html_()
