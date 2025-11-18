import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime
import psycopg2 # Although st.connection handles it, it's good practice to have the driver imported

# --- Page Configuration ---
st.set_page_config(
    page_title="Weather Dashboard",
    page_icon="🌦️",
    layout="wide",
)

# --- API Key and City Data ---
# Load API key from secrets
try:
    api_key = st.secrets["openweathermap"]["api_key"]
except KeyError:
    st.error("OpenWeatherMap API key not found. Please add it to your .streamlit/secrets.toml file.")
    st.stop()

# Predefined cities with their coordinates
CITIES = {
    "Hanoi": {"lat": 21.0278, "lon": 105.8342},
    "Haiphong": {"lat": 20.8449, "lon": 106.6881},
    "Halong": {"lat": 20.9517, "lon": 107.0757},
}

# --- Helper Functions ---

# --- NEW: Function to get data from PostgreSQL ---
@st.cache_data(ttl=600) # Cache the result for 10 minutes
def get_latest_weather_from_db(city):
    """Fetches the most recent weather data for a city from the database."""
    try:
        # Initialize connection
        conn = st.connection("postgres", type="sql")
        
        # SQL query to get the latest entry for the selected city
        query = f"SELECT * FROM weather_data WHERE city_name = '{city}' ORDER BY timestamp DESC LIMIT 1;"
        
        df = conn.query(query)
        
        if df.empty:
            st.warning(f"No data found in the database for {city}. Falling back to API.")
            return None
        
        # Convert the first row of the DataFrame to a dictionary
        latest_data = df.iloc[0].to_dict()
        return latest_data
        
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# --- NEW: Function to format DB data to match API structure ---
def format_db_data_to_api_structure(db_data):
    """Converts the flat dictionary from the DB to the nested one the UI expects."""
    if not db_data:
        return None
    return {
        'main': {
            'temp': db_data.get('temperature'),
            'feels_like': db_data.get('feels_like'),
            'humidity': db_data.get('humidity'),
            'pressure': db_data.get('pressure'),
            'sea_level': db_data.get('sea_level_pressure'),
            'grnd_level': db_data.get('ground_level_pressure')
        },
        'wind': {
            'speed': db_data.get('wind_speed')
        },
        'visibility': db_data.get('visibility', 0) * 1000, # Assuming visibility is stored in km
        'clouds': {
            'all': db_data.get('cloudiness')
        },
        'weather': [{
            'main': db_data.get('weather_main'),
            'description': db_data.get('weather_description', '').capitalize(),
            'icon': db_data.get('weather_icon')
        }]
    }

def get_forecast_data(lat, lon, api_key):
    """Fetches ONLY the 5-day forecast data from OpenWeatherMap API."""
    base_url = "https://api.openweathermap.org/data/2.5/"
    
    # Fetch 5-day/3-hour forecast
    forecast_url = f"{base_url}forecast?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    forecast_response = requests.get(forecast_url)
    if forecast_response.status_code != 200:
        st.error(f"Error fetching forecast data: {forecast_response.json().get('message', 'Unknown error')}")
        return None
    return forecast_response.json()

def process_forecast_data(forecast_data):
    """Processes the forecast data to create DataFrames for hourly and daily forecasts."""
    if not forecast_data or 'list' not in forecast_data:
        return pd.DataFrame(), pd.DataFrame()

    records = []
    for item in forecast_data['list']:
        records.append({
            'timestamp': datetime.fromtimestamp(item['dt']),
            'temp': item['main']['temp'],
            'feels_like': item['main']['feels_like'],
            'pop': item['pop'] * 100, # Convert to percentage
            'description': item['weather'][0]['description'],
            'icon': item['weather'][0]['icon'],
        })
    
    df = pd.DataFrame(records)
    df['date'] = df['timestamp'].dt.date
    
    # --- 5-Hour Forecast ---
    hourly_df = df.head(5).copy()

    # --- 3-Day Forecast ---
    daily_df = df.groupby('date').agg(
        min_temp=('temp', 'min'),
        max_temp=('temp', 'max'),
        pop=('pop', 'max')
    ).reset_index().head(3)

    return hourly_df, daily_df

# --- UI Layout ---

st.sidebar.title("📍 Location")
selected_city = st.sidebar.selectbox("Select a city:", list(CITIES.keys()))

st.title(f"🌦️ Weather in {selected_city}")

# --- MODIFIED: Fetch current data from DB and forecast from API ---
lat = CITIES[selected_city]["lat"]
lon = CITIES[selected_city]["lon"]

# Get current weather from your PostgreSQL database
db_data = get_latest_weather_from_db(selected_city)
current_data = format_db_data_to_api_structure(db_data)

# Get forecast data from the API
forecast_data = get_forecast_data(lat, lon, api_key)

if current_data and forecast_data:
    # --- Current Weather Details (This section remains unchanged as it now reads the formatted data) ---
    st.header("Now")
    
    weather_icon = f"http://openweathermap.org/img/wn/{current_data['weather'][0]['icon']}@2x.png"
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="Temperature", value=f"{current_data['main']['temp']:.1f} °C")
        st.image(weather_icon, width=70)
        
    with col2:
        st.metric(label="Feels Like", value=f"{current_data['main']['feels_like']:.1f} °C")
    with col3:
        st.metric(label="Humidity", value=f"{current_data['main']['humidity']}%")
    with col4:
        st.metric(label="Wind Speed", value=f"{current_data['wind']['speed']:.1f} m/s")
    
    st.write("")

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        visibility_km = current_data.get('visibility', 0) / 1000
        st.metric(label="Visibility", value=f"{visibility_km:.1f} km")
    with col6:
        st.metric(label="Cloudiness", value=f"{current_data.get('clouds', {}).get('all', 0)}%")
    with col7:
        sea_level = current_data.get('main', {}).get('sea_level')
        if sea_level:
            st.metric(label="Sea Level Pressure", value=f"{sea_level} hPa")
        else:
            st.metric(label="Pressure", value=f"{current_data['main'].get('pressure', 'N/A')} hPa")
    with col8:
        grnd_level = current_data.get('main', {}).get('grnd_level')
        if grnd_level:
            st.metric(label="Ground Level Pressure", value=f"{grnd_level} hPa")
        else:
            st.metric(label="Ground Level Pressure", value="N/A")

    st.write(f"**Condition:** {current_data['weather'][0]['main']} - {current_data['weather'][0]['description'].capitalize()}")
    
    # Process and display forecast data (this part is also unchanged)
    hourly_df, daily_df = process_forecast_data(forecast_data)
    
    st.markdown("---")
    
    # --- 5-Hour Forecast Section ---
    if not hourly_df.empty:
        st.header("Next 5 Hours Forecast")
        col1_hourly, col2_hourly = st.columns(2)
        with col1_hourly:
            fig_temp_hourly = px.line(
                hourly_df, x='timestamp', y='temp', title='Temperature (°C)',
                labels={'timestamp': 'Time', 'temp': 'Temperature'}, markers=True
            )
            fig_temp_hourly.update_traces(line_color='#FF8C00', marker=dict(color='#FF8C00', size=8))
            fig_temp_hourly.update_layout(xaxis=dict(tickformat='%I %p'))
            st.plotly_chart(fig_temp_hourly, use_container_width=True)
        with col2_hourly:
            fig_pop_hourly = px.bar(
                hourly_df, x='timestamp', y='pop', title='Probability of Precipitation (%)',
                labels={'timestamp': 'Time', 'pop': 'Probability (%)'}, color_discrete_sequence=['#1E90FF']
            )
            fig_pop_hourly.update_layout(yaxis_range=[0, 100], xaxis=dict(tickformat='%I %p'))
            st.plotly_chart(fig_pop_hourly, use_container_width=True)
            
    st.markdown("---")

    # --- 3-Day Forecast Section ---
    if not daily_df.empty:
        st.header("Next 3 Days Forecast")
        col1_daily, col2_daily = st.columns(2)
        with col1_daily:
            fig_temp_daily = px.line(
                daily_df, x='date', y=['min_temp', 'max_temp'], title='Min/Max Temperature (°C)',
                labels={'date': 'Date', 'value': 'Temperature'}, markers=True
            )
            fig_temp_daily.update_layout(legend_title_text='Temperature')
            new_names = {'min_temp': 'Min Temp', 'max_temp': 'Max Temp'}
            fig_temp_daily.for_each_trace(lambda t: t.update(name = new_names[t.name]))
            st.plotly_chart(fig_temp_daily, use_container_width=True)
        with col2_daily:
            fig_pop_daily = px.bar(
                daily_df, x='date', y='pop', title='Max Daily Probability of Precipitation (%)',
                labels={'date': 'Date', 'pop': 'Probability (%)'}, color_discrete_sequence=['#4682B4']
            )
            fig_pop_daily.update_layout(yaxis_range=[0, 100])
            st.plotly_chart(fig_pop_daily, use_container_width=True)

else:
    st.warning("Could not retrieve weather data. Please check your database connection, API key, and network connection.")