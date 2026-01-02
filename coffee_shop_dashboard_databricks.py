import streamlit as st
import pandas as pd
from databricks import sql
import os

# Page configuration
st.set_page_config(
    page_title="Coffee Shop Dashboard",
    page_icon="☕",
    layout="wide"
)

# Direct configuration - REMOVE BEFORE DEPLOYING TO PRODUCTION
DATABRICKS_SERVER_HOSTNAME = "adb-7405619795306590.10.azuredatabricks.net"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/bea12c9fd152c7cd"
DATABRICKS_TOKEN = "dapi5192ab4cb289db6a8457a722e8fda792"

# Add debug information
st.sidebar.subheader("Connection Details")
st.sidebar.code(f"""
Hostname: {DATABRICKS_SERVER_HOSTNAME}
HTTP Path: {DATABRICKS_HTTP_PATH}
Token: {DATABRICKS_TOKEN[:10]}... (truncated)
""")

def get_databricks_connection():
    """Establish connection to Databricks SQL Warehouse"""
    try:
        # Validate token
        if not DATABRICKS_TOKEN or len(DATABRICKS_TOKEN) < 10:
            st.error("❌ Token appears to be invalid (too short)")
            return None, "Invalid token"
        
        # Test connection
        connection = sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        st.sidebar.success("✅ Connection successful!")
        return connection, None
        
    except Exception as e:
        error_msg = f"Connection Error: {str(e)}"
        st.sidebar.error(f"❌ {error_msg}")
        return None, error_msg

def run_query(query):
    """Execute SQL query and return DataFrame"""
    conn, error = get_databricks_connection()
    if error:
        st.error(f"Connection failed: {error}")
        return None
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=columns)
                return df
            else:
                st.info("Query executed successfully but returned no data")
                return pd.DataFrame()
    except Exception as e:
        st.error(f"Query Error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def test_tables():
    """Check what tables are available in the database"""
    query = "SHOW TABLES"
    df = run_query(query)
    if df is not None and not df.empty:
        st.sidebar.subheader("Available Tables")
        st.sidebar.dataframe(df)
        return df['tableName'].tolist()
    return []

# Main Dashboard UI
st.title("☕ Coffee Shop Sales Analysis")
st.markdown("---")

# Test connection first
st.header("Connection Test")
if st.button("Test Databricks Connection"):
    conn, error = get_databricks_connection()
    if conn:
        conn.close()

# Discover available tables
st.header("Database Discovery")
if st.button("Discover Tables"):
    tables = test_tables()
    if tables:
        st.success(f"Found {len(tables)} tables: {', '.join(tables)}")
    else:
        st.warning("No tables found or unable to query")

# Try different table names
st.header("Try Different Table Names")
table_options = ["index_1", "index_table", "sales", "transactions", "coffee_sales"]
selected_table = st.selectbox("Select table to query:", table_options)

if st.button(f"Query {selected_table}"):
    query = f"SELECT * FROM {selected_table} LIMIT 10"
    df = run_query(query)
    
    if df is not None:
        if not df.empty:
            st.success(f"✅ Found {len(df)} rows in {selected_table}")
            st.dataframe(df)
        else:
            st.info(f"Table '{selected_table}' exists but has no data")

# Original dashboard functionality
st.markdown("---")
st.header("Main Dashboard")

# Check if we have valid data
@st.cache_data(ttl=3600)
def load_dashboard_data():
    # Try multiple possible table names
    possible_tables = ["index_1", "index_table", "sales"]
    
    for table_name in possible_tables:
        query = f"""
        SELECT 
            date, 
            datetime, 
            cash_type, 
            money, 
            coffee_name 
        FROM {table_name} 
        ORDER BY datetime DESC 
        LIMIT 1000
        """
        df = run_query(query)
        if df is not None and not df.empty:
            # Convert date columns
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            return df, table_name
    
    return None, None

with st.spinner("Loading data..."):
    df, table_used = load_dashboard_data()

if df is not None and not df.empty:
    st.success(f"✅ Using data from table: {table_used}")
    
    # --- METRICS SECTION ---
    total_sales = df['money'].sum()
    total_trans = len(df)
    avg_trans = total_sales / total_trans if total_trans > 0 else 0
    unique_products = df['coffee_name'].nunique() if 'coffee_name' in df.columns else 0

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
        if 'date' in df.columns and 'money' in df.columns:
            daily_sales = df.groupby('date')['money'].sum().reset_index()
            st.line_chart(daily_sales.set_index('date'))
        else:
            st.warning("Missing 'date' or 'money' columns for chart")

    with col2:
        st.subheader("Top Selling Products")
        if 'coffee_name' in df.columns and 'money' in df.columns:
            top_products = df.groupby('coffee_name')['money'].sum().sort_values(ascending=False).head(10)
            st.bar_chart(top_products)
        else:
            st.warning("Missing 'coffee_name' or 'money' columns for chart")

    # --- RAW DATA ---
    st.markdown("---")
    st.subheader("Sample Data")
    st.dataframe(df.head(100), use_container_width=True)

else:
    st.error("""
    ❌ No data found. Possible issues:
    1. Invalid access token - regenerate it in Databricks
    2. Wrong table name - check your table names
    3. No data in the table
    4. Incorrect server hostname or HTTP path
    
    **Steps to fix:**
    1. Go to Databricks → User Settings → Developer → Access Tokens
    2. Generate a new token and copy it
    3. Update the DATABRICKS_TOKEN in the code
    4. Check the table name exists in your database
    """)
