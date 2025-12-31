import streamlit as st
import pandas as pd
from databricks import sql
import os
from dotenv import load_dotenv

# Page configuration
st.set_page_config(
    page_title="Coffee Shop Dashboard",
    page_icon="☕",
    layout="wide"
)

# Load environment variables (Local)
load_dotenv()

def get_secret(key, default=None):
    """Retrieve secret from streamlit secrets or environment variables."""
    # 1. Try Streamlit Secrets (Cloud Deployment) - Priority
    try:
        if key in st.secrets:
            return st.secrets[key]
        if key.lower() in st.secrets:
            return st.secrets[key.lower()]
    except:
        pass

    # 2. Try environment variables (Local .env)
    val = os.getenv(key)
    if val and "your-personal-access-token" not in val and "your-warehouse-id" not in val:
        return val
        
    return default

DATABRICKS_SERVER_HOSTNAME = get_secret("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = get_secret("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = get_secret("DATABRICKS_TOKEN")

# Debugging in production (Expandable)
with st.sidebar.expander("🛠️ Debug Deployment (Secrets)"):
    st.write(f"Hostname detected: {'✅' if DATABRICKS_SERVER_HOSTNAME else '❌'}")
    st.write(f"HTTP Path detected: {'✅' if DATABRICKS_HTTP_PATH else '❌'}")
    st.write(f"Token detected: {'✅' if DATABRICKS_TOKEN else '❌'}")
    if not DATABRICKS_TOKEN:
        st.info("Ensure you added the secrets in Streamlit Cloud Dashboard -> Settings -> Secrets.")



def get_databricks_connection():
    try:
        if not DATABRICKS_TOKEN or "your-personal-access-token" in DATABRICKS_TOKEN:
            return None, "Error: Databricks Token is missing or invalid in .env file."
        
        connection = sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        return connection, None
    except Exception as e:
        return None, str(e)

def run_query(query):
    conn, error = get_databricks_connection()
    if error:
        st.error(error)
        return None
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            # Convert to DataFrame
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=columns)
            return df
    except Exception as e:
        st.error(f"Query Error: {e}")
        return None
    finally:
        if conn:
            conn.close()

# Main Dashboard UI
st.title("☕ Coffee Shop Sales Analysis")
st.markdown("---")

if not DATABRICKS_TOKEN or "your-personal-access-token" in DATABRICKS_TOKEN:
    st.warning("⚠️ **Missing Configuration**: Please add your `DATABRICKS_TOKEN` to your Secrets (Cloud) or `.env` file (Local).")
    st.stop()

# Sidebar for filters or info
st.sidebar.header("Dashboard Settings")
st.sidebar.info("Connected to Databricks SQL Warehouse.")

# Data Loading
@st.cache_data(ttl=3600)
def load_dashboard_data():
    # Load all data for analysis (or aggregate in SQL for better performance)
    query = "SELECT date, datetime, cash_type, money, coffee_name FROM index_1 ORDER BY datetime DESC"
    df = run_query(query)
    if df is not None:
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['date'] = pd.to_datetime(df['date'])
    return df

with st.spinner("Fetching data from Databricks..."):
    df = load_dashboard_data()

if df is not None and not df.empty:
    # --- METRICS SECTION ---
    total_sales = df['money'].sum()
    total_trans = len(df)
    avg_trans = total_sales / total_trans if total_trans > 0 else 0
    unique_products = df['coffee_name'].nunique()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Sales", f"${total_sales:,.2f}")
    m2.metric("Total Transactions", f"{total_trans:,}")
    m3.metric("Average Transaction", f"${avg_trans:,.2f}")
    m4.metric("Unique Products", f"{unique_products}")

    st.markdown("---")

    # --- CHARTS SECTION ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Daily Sales Trend")
        daily_sales = df.groupby('date')['money'].sum().reset_index()
        st.line_chart(daily_sales.set_index('date'))

    with col2:
        st.subheader("Top Selling Products")
        top_products = df.groupby('coffee_name')['money'].sum().sort_values(ascending=False).head(10)
        st.bar_chart(top_products)

    # --- SECOND ROW ---
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Payment Method Distribution")
        payment_dist = df['cash_type'].value_counts()
        st.write(payment_dist) # Simple table for now, can use pie chart if library available

    with col4:
        st.subheader("Sales by Coffee Type")
        sales_by_type = df.groupby('coffee_name')['money'].sum().reset_index()
        st.dataframe(sales_by_type.sort_values(by='money', ascending=False), hide_index=True)

    # --- RAW DATA ---
    st.markdown("---")
    st.subheader("Recent Transactions")
    st.dataframe(df.head(100), use_container_width=True)
else:
    st.info("No data found in `index_1` table.")
