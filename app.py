import streamlit as st
import json
import os
import pandas as pd
import time
from datetime import datetime
from config import SIGNALS_FILE_PATH, TRADE_CONFIG

MAIN_LOOP_SLEEP_SECONDS = TRADE_CONFIG.get("MAIN_LOOP_SLEEP_SECONDS", 60)

# Set page configuration
st.set_page_config(
    page_title="SMC Crypto Bot Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Utility Functions ---
@st.cache_data(ttl=5) # Cache data for 5 seconds to reduce file reads and allow quick refresh
def load_signals():
    """Loads active signals from the JSON file."""
    if not os.path.exists(SIGNALS_FILE_PATH):
        return [], None

    try:
        with open(SIGNALS_FILE_PATH, 'r') as f:
            signals_data = json.load(f)

        last_modified = datetime.fromtimestamp(os.path.getmtime(SIGNALS_FILE_PATH)).strftime('%Y-%m-%d %H:%M:%S')
        return signals_data, last_modified
    except json.JSONDecodeError:
        st.error(f"Error decoding JSON from {SIGNALS_FILE_PATH}. File might be corrupted or empty.")
        return [], None
    except Exception as e:
        st.error(f"An error occurred while loading signals: {e}")
        return [], None


def normalize_signal_rows(signals):
    normalized = []
    for signal in signals:
        row = dict(signal)
        direction = row.get('direction', row.get('signal', 'NO_SIGNAL'))
        entry_price = row.get('entry_price', row.get('entry', row.get('current_price', 0)))
        confidence_score = row.get('confidence_score', row.get('strength', 0))

        row['direction'] = direction
        row['entry_price'] = float(entry_price or 0)
        row['confidence_score'] = float(confidence_score or 0)
        row['stop_loss'] = float(row.get('stop_loss') or 0)
        row['take_profit_1'] = float(row.get('take_profit_1') or 0)
        row['take_profit_2'] = float(row.get('take_profit_2') or 0)
        normalized.append(row)
    return normalized

# --- Streamlit App Layout ---

st.title("📈 SMC Crypto Bot Dashboard")

# --- Sidebar for Navigation/Info ---
with st.sidebar:
    st.header("Control Panel")
    st.write(f"Signals file: `{os.path.basename(SIGNALS_FILE_PATH)}`")
    st.write(f"Data refresh interval (Bot Loop): {MAIN_LOOP_SLEEP_SECONDS} seconds")

    st.subheader("Instructions")
    st.markdown("""
    1.  Run the main bot logic in a terminal:
        `python main.py`
    2.  Run this Streamlit dashboard in another terminal:
        `streamlit run app.py`

    The bot (`main.py`) will periodically scan for and generate signals, saving them to `active_signals.json`. This dashboard will automatically refresh to display the latest signals.
    """)

# --- Main Content Area ---
signals, last_modified_time = load_signals()

# Display last updated time
if last_modified_time:
    st.info(f"Last updated: {last_modified_time}")
else:
    st.warning("Signals file not found or could not be loaded. Please ensure `main.py` is running.")

st.subheader("All Active Trade Signals")

if signals:
    # Convert list of dictionaries to DataFrame for better display
    signals_df = pd.DataFrame(normalize_signal_rows(signals))

    # Clean up and reorder columns for display
    display_columns = [
        'symbol', 'direction', 'entry_price', 'stop_loss',
        'take_profit_1', 'take_profit_2', 'confidence_score', 'timestamp'
    ]

    # Format numerical columns
    signals_df['entry_price'] = signals_df['entry_price'].apply(lambda x: f"{x:.4f}")
    signals_df['stop_loss'] = signals_df['stop_loss'].apply(lambda x: f"{x:.4f}")
    signals_df['take_profit_1'] = signals_df['take_profit_1'].apply(lambda x: f"{x:.4f}")
    signals_df['take_profit_2'] = signals_df['take_profit_2'].apply(lambda x: f"{x:.4f}")
    signals_df['confidence_score'] = signals_df['confidence_score'].apply(lambda x: f"{x:.0f}") # Display as integer

    st.dataframe(signals_df[display_columns], use_container_width=True, hide_index=True)

    # Detailed Signal View
    st.subheader("Detailed Signal Information")

    # Create a list of symbols for the selectbox, adding an "Select a symbol..." option
    symbol_options = ["Select a symbol..."] + sorted(signals_df['symbol'].tolist())
    selected_symbol = st.selectbox("Choose a symbol to view details:", symbol_options)

    if selected_symbol != "Select a symbol...":
        detail_signal = signals_df[signals_df['symbol'] == selected_symbol].iloc[0] # Get the first (and only) matching row

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Symbol", detail_signal['symbol'])
            st.metric("Direction", detail_signal['direction'])
            st.metric("Entry Price", detail_signal['entry_price'])
            st.metric("Stop Loss", detail_signal['stop_loss'])
        with col2:
            st.metric("Confidence Score", detail_signal['confidence_score'])
            st.metric("Take Profit 1", detail_signal['take_profit_1'])
            st.metric("Take Profit 2", detail_signal['take_profit_2'])
            st.metric("Generated At", detail_signal['timestamp'])

        st.markdown("### Rationale")
        for item in detail_signal['rationale']:
            st.write(f"- {item}")
else:
    st.info("No active trade signals to display yet. The bot may be starting up or no signals were generated.")

# Auto-refresh mechanism
time.sleep(MAIN_LOOP_SLEEP_SECONDS) # Wait for the bot's next cycle
st.rerun() # Rerun the script to load updated data
