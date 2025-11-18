import streamlit as st
import pandas as pd
import time
from sqlalchemy import create_engine

# Set the title and a header for the page
st.set_page_config(page_title="Live Postgres Dashboard", layout="wide")
st.title("📈 Realtime Dashboard with Model Predictions")

# --- Database Connection ---

# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    """Initializes a connection to the PostgreSQL database using credentials from st.secrets."""
    try:
        # Construct the connection string from Streamlit secrets
        db_url = (
            f"postgresql+psycopg2://{st.secrets['postgres']['user']}:"
            f"{st.secrets['postgres']['password']}@"
            f"{st.secrets['postgres']['host']}:"
            f"{st.secrets['postgres']['port']}/"
            f"{st.secrets['postgres']['dbname']}"
        )
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        st.error(f"Failed to connect to the database. Please check your secrets.toml file. Error: {e}")
        return None

# Uses st.cache_data to only rerun when the query changes or after 10s.
@st.cache_data(ttl=10)
def run_query(_engine, query):
    """Runs a SQL query and returns the result as a pandas DataFrame."""
    with _engine.connect() as connection:
        return pd.read_sql(query, connection)

# --- Mock Model Prediction Function ---
def get_prediction(data_point):
    """
    Simulates a machine learning model prediction.
    Takes a single row of data (as a pandas Series) and returns a prediction.
    In a real-world scenario, you would load a pre-trained model file here 
    (e.g., using pickle or joblib) and call its .predict() method.
    """
    try:
        # Example logic: 'predict' a score based on the values.
        # This is just a placeholder for your actual model's logic.
        score = (data_point['a'] * 2) + data_point['b'] - data_point['c']
        
        if score > 1.0:
            return "High Risk", score, "Critical"
        elif score > 0.5:
            return "Medium Risk", score, "Warning"
        else:
            return "Low Risk", score, "Normal"
    except KeyError:
        # Handle cases where the expected columns aren't in the data
        return "Data Error", 0, "N/A"

# --- App Layout ---

engine = init_connection()

if engine:
    # Create placeholders for different parts of the UI for smoother updates
    chart_placeholder = st.empty()
    metrics_placeholder = st.empty()

    # Main loop to query data and update the dashboard
    while True:
        query = "SELECT * FROM live_metrics ORDER BY created_at DESC LIMIT 20;"
        
        try:
            df = run_query(engine, query)
            
            if not df.empty:
                # --- Prepare data for charting ---
                df_for_chart = df.iloc[::-1] # Reverse order for time-series plot
                if 'created_at' in df_for_chart.columns:
                    df_for_chart = df_for_chart.set_index('created_at')

                # --- Get and Display Prediction on the latest data point ---
                latest_data_point = df.iloc[0]
                prediction_label, prediction_score, _ = get_prediction(latest_data_point)

                # --- Update UI Elements ---
                with chart_placeholder.container():
                    st.header("Live Metrics from PostgreSQL")
                    # Chart only the numeric columns
                    st.line_chart(df_for_chart[['a', 'b', 'c']])

                with metrics_placeholder.container():
                    # Use columns for a cleaner layout
                    col1, col2 = st.columns(2)
                    with col1:
                        st.header("Latest Model Prediction")
                        st.metric(
                            label="Risk Assessment",
                            value=prediction_label,
                            delta=f"Score: {prediction_score:.2f}",
                            delta_color="off"
                        )
                    
                    with col2:
                        st.header("Latest Data Queried")
                        st.dataframe(df)

            else:
                 with chart_placeholder.container():
                    st.warning("No data found in 'live_metrics' table. Waiting for new data...")

        except Exception as e:
            with chart_placeholder.container():
                st.error(f"An error occurred: {e}")
                st.info("Please ensure the 'live_metrics' table exists and contains the required columns (a, b, c).")
        
        # Wait for 10 seconds before the next update
        time.sleep(10)

