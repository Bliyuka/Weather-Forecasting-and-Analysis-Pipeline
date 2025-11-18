import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime

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

def get_weather_data(lat, lon, api_key):
    """Fetches current weather and 5-day forecast data from OpenWeatherMap API."""
    base_url = "https://api.openweathermap.org/data/2.5/"
    
    # Fetch current weather
    current_weather_url = f"{base_url}weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    current_response = requests.get(current_weather_url)
    if current_response.status_code != 200:
        st.error(f"Error fetching current weather data: {current_response.json().get('message', 'Unknown error')}")
        return None, None
    current_data = current_response.json()

    # Fetch 5-day/3-hour forecast
    forecast_url = f"{base_url}forecast?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    forecast_response = requests.get(forecast_url)
    if forecast_response.status_code != 200:
        st.error(f"Error fetching forecast data: {forecast_response.json().get('message', 'Unknown error')}")
        return current_data, None
    forecast_data = forecast_response.json()

    return current_data, forecast_data

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
    # Get the next 5 forecast points (which are in 3-hour intervals)
    hourly_df = df.head(5).copy()

    # --- 3-Day Forecast ---
    # Group by date and aggregate
    daily_df = df.groupby('date').agg(
        min_temp=('temp', 'min'),
        max_temp=('temp', 'max'),
        pop=('pop', 'max') # Use the max probability of precipitation for the day
    ).reset_index().head(3)

    return hourly_df, daily_df

# --- UI Layout ---

# Sidebar for city selection
st.sidebar.title("📍 Location")
selected_city = st.sidebar.selectbox("Select a city:", list(CITIES.keys()))

# Main title
st.title(f"🌦️ Weather in {selected_city}")

# Fetch and display data
lat = CITIES[selected_city]["lat"]
lon = CITIES[selected_city]["lon"]
current_data, forecast_data = get_weather_data(lat, lon, api_key)

if current_data and forecast_data:
    # --- Current Weather Details ---
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
    
    # Add a little space before the next row
    st.write("")

    # --- Second row of metrics for additional data ---
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        # Visibility is in meters, convert to km
        visibility_km = current_data.get('visibility', 0) / 1000
        st.metric(label="Visibility", value=f"{visibility_km:.1f} km")
    with col6:
        st.metric(label="Cloudiness", value=f"{current_data.get('clouds', {}).get('all', 0)}%")
    with col7:
        # Use .get() for pressure values as they might not always be present
        sea_level = current_data.get('main', {}).get('sea_level')
        if sea_level:
            st.metric(label="Sea Level Pressure", value=f"{sea_level} hPa")
        else:
            # Fallback to standard pressure if sea_level is not available
            st.metric(label="Pressure", value=f"{current_data['main'].get('pressure', 'N/A')} hPa")
    with col8:
        grnd_level = current_data.get('main', {}).get('grnd_level')
        if grnd_level:
            st.metric(label="Ground Level Pressure", value=f"{grnd_level} hPa")
        else:
            st.metric(label="Ground Level Pressure", value="N/A")

    st.write(f"**Condition:** {current_data['weather'][0]['main']} - {current_data['weather'][0]['description'].capitalize()}")
    
    # Process forecast data
    hourly_df, daily_df = process_forecast_data(forecast_data)
    
    st.markdown("---")
    
    # --- 5-Hour Forecast Section ---
    if not hourly_df.empty:
        st.header("Next 5 Hours Forecast")

        # Create two columns for the graphs
        col1_hourly, col2_hourly = st.columns(2)

        with col1_hourly:
            # Temperature Line Chart
            fig_temp_hourly = px.line(
                hourly_df, 
                x='timestamp', 
                y='temp', 
                title='Temperature (°C)',
                labels={'timestamp': 'Time', 'temp': 'Temperature'},
                markers=True
            )
            fig_temp_hourly.update_traces(line_color='#FF8C00', marker=dict(color='#FF8C00', size=8))
            fig_temp_hourly.update_layout(xaxis=dict(tickformat='%I %p')) # Format time as e.g., '03 PM'
            st.plotly_chart(fig_temp_hourly, use_container_width=True)

        with col2_hourly:
            # POP Bar Chart
            fig_pop_hourly = px.bar(
                hourly_df, 
                x='timestamp', 
                y='pop',
                title='Probability of Precipitation (%)',
                labels={'timestamp': 'Time', 'pop': 'Probability (%)'},
                color_discrete_sequence=['#1E90FF']
            )
            fig_pop_hourly.update_layout(
                yaxis_range=[0, 100],
                xaxis=dict(tickformat='%I %p') # Format time as e.g., '03 PM'
            )
            st.plotly_chart(fig_pop_hourly, use_container_width=True)
            
    st.markdown("---")

    # --- 3-Day Forecast Section ---
    if not daily_df.empty:
        st.header("Next 3 Days Forecast")

        # Create two columns for the graphs
        col1_daily, col2_daily = st.columns(2)

        with col1_daily:
            # Min/Max Temperature Line Chart
            fig_temp_daily = px.line(
                daily_df,
                x='date',
                y=['min_temp', 'max_temp'],
                title='Min/Max Temperature (°C)',
                labels={'date': 'Date', 'value': 'Temperature'},
                markers=True
            )
            fig_temp_daily.update_layout(legend_title_text='Temperature')
            # Customizing line names in legend
            new_names = {'min_temp': 'Min Temp', 'max_temp': 'Max Temp'}
            fig_temp_daily.for_each_trace(lambda t: t.update(name = new_names[t.name]))
            st.plotly_chart(fig_temp_daily, use_container_width=True)

        with col2_daily:
            # Daily POP Bar Chart
            fig_pop_daily = px.bar(
                daily_df,
                x='date',
                y='pop',
                title='Max Daily Probability of Precipitation (%)',
                labels={'date': 'Date', 'pop': 'Probability (%)'},
                color_discrete_sequence=['#4682B4']
            )
            fig_pop_daily.update_layout(yaxis_range=[0, 100])
            st.plotly_chart(fig_pop_daily, use_container_width=True)

else:
    st.warning("Could not retrieve weather data. Please check your API key and network connection.")
