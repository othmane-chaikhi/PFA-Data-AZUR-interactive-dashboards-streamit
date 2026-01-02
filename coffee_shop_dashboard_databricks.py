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

# Direct configuration
DATABRICKS_SERVER_HOSTNAME = "adb-7405619795306590.10.azuredatabricks.net"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/bea12c9fd152c7cd"
DATABRICKS_TOKEN = "dapi06163300c6c4a02e600241dba5f4ab9a"

# Add debug information
st.sidebar.title("🔧 Debug Panel")
st.sidebar.code(f"""
Hostname: {DATABRICKS_SERVER_HOSTNAME}
HTTP Path: {DATABRICKS_HTTP_PATH}
Token: {DATABRICKS_TOKEN[:10]}...
""")

def test_connection():
    """Test connection with detailed error reporting"""
    try:
        # First, test basic connection
        st.sidebar.info("🔄 Testing connection...")
        
        connection = sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        
        # Try to execute a simple query to verify permissions
        with connection.cursor() as cursor:
            # Check if we can query system tables
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            
        connection.close()
        
        if databases:
            st.sidebar.success("✅ Connection successful!")
            st.sidebar.info(f"Found {len(databases)} databases")
            return True, "Connection successful"
        else:
            return False, "Connected but no databases found"
            
    except Exception as e:
        error_msg = str(e)
        
        # Common error patterns
        if "invalid access token" in error_msg.lower():
            return False, "❌ Invalid access token. Please regenerate your token."
        elif "http path" in error_msg.lower():
            return False, "❌ Invalid HTTP path. Check your warehouse configuration."
        elif "server hostname" in error_msg.lower():
            return False, "❌ Invalid server hostname."
        elif "permission denied" in error_msg.lower():
            return False, "❌ Permission denied. Check token permissions."
        else:
            return False, f"❌ Connection error: {error_msg}"

def explore_database():
    """Explore the database structure"""
    try:
        connection = sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        
        with connection.cursor() as cursor:
            # Get all databases
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            
            db_info = []
            for db in databases:
                db_name = db[0]
                # Try to get tables in each database
                try:
                    cursor.execute(f"SHOW TABLES IN {db_name}")
                    tables = cursor.fetchall()
                    for table in tables:
                        db_info.append({
                            'database': db_name,
                            'table': table[1],
                            'is_temporary': table[3]
                        })
                except:
                    continue
        
        connection.close()
        return db_info
        
    except Exception as e:
        st.error(f"Exploration error: {e}")
        return []

def run_simple_query():
    """Run a simple test query to verify data access"""
    try:
        connection = sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        
        with connection.cursor() as cursor:
            # Try multiple common table names
            possible_queries = [
                "SELECT * FROM default.index_1 LIMIT 5",
                "SELECT * FROM index_1 LIMIT 5",
                "SELECT * FROM default.index_table LIMIT 5",
                "SELECT * FROM index_table LIMIT 5",
                "SELECT current_timestamp() as test_time"
            ]
            
            for query in possible_queries:
                try:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        df = pd.DataFrame(result, columns=columns)
                        if not df.empty:
                            return True, query, df
                except:
                    continue
        
        connection.close()
        return False, "No successful queries", pd.DataFrame()
        
    except Exception as e:
        return False, f"Query error: {e}", pd.DataFrame()

# Main Dashboard UI
st.title("☕ Coffee Shop Sales Analysis Dashboard")
st.markdown("---")

# Step 1: Test Connection
st.header("Step 1: Connection Test")
col1, col2 = st.columns([3, 1])
with col2:
    if st.button("🔗 Test Connection", use_container_width=True):
        success, message = test_connection()
        if success:
            st.success(message)
        else:
            st.error(message)

# Step 2: Explore Database
st.header("Step 2: Database Exploration")
if st.button("🔍 Explore Database Structure"):
    with st.spinner("Exploring database..."):
        tables = explore_database()
        
    if tables:
        st.success(f"✅ Found {len(tables)} tables")
        
        # Create DataFrame
        df_tables = pd.DataFrame(tables)
        
        # Group by database
        for database in df_tables['database'].unique():
            db_tables = df_tables[df_tables['database'] == database]
            st.subheader(f"Database: `{database}`")
            st.dataframe(db_tables[['table', 'is_temporary']], 
                        use_container_width=True)
    else:
        st.warning("No tables found. The database might be empty or you don't have permissions.")

# Step 3: Test Data Access
st.header("Step 3: Test Data Access")
if st.button("🧪 Test Queries"):
    with st.spinner("Testing queries..."):
        success, query_used, df_result = run_simple_query()
    
    if success:
        st.success(f"✅ Query successful: `{query_used}`")
        st.dataframe(df_result, use_container_width=True)
        
        # Show column information
        st.subheader("Column Information")
        for col in df_result.columns:
            st.code(f"{col}: {df_result[col].dtype}")
    else:
        st.error(f"❌ Query failed: {query_used}")

# Step 4: Data Loading for Dashboard
st.markdown("---")
st.header("Step 4: Load Data for Dashboard")

# Let user specify table name
table_name = st.text_input("Enter table name:", "index_1")
database_name = st.selectbox("Select database:", ["default", ""], 
                           help="Leave empty for no database prefix")

@st.cache_data(ttl=600)
def load_table_data(table_name, database_name=""):
    """Load data from specified table"""
    try:
        connection = sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        
        # Construct query
        if database_name:
            full_table_name = f"{database_name}.{table_name}"
        else:
            full_table_name = table_name
            
        query = f"""
        SELECT * 
        FROM {full_table_name} 
        LIMIT 1000
        """
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=columns)
                
                # Try to convert date columns
                for col in df.columns:
                    if 'date' in col.lower() or 'time' in col.lower():
                        try:
                            df[col] = pd.to_datetime(df[col])
                        except:
                            pass
                
                return df, query
            else:
                return pd.DataFrame(), query
                
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None

if st.button("📥 Load Table Data"):
    with st.spinner(f"Loading data from {table_name}..."):
        df, query_used = load_table_data(table_name, database_name)
    
    if df is not None:
        if not df.empty:
            st.success(f"✅ Loaded {len(df)} rows from `{table_name}`")
            st.code(query_used)
            
            # Show basic info
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Rows", len(df))
            with col2:
                st.metric("Columns", len(df.columns))
            with col3:
                st.metric("Memory", f"{df.memory_usage().sum() / 1024:.1f} KB")
            
            # Show data preview
            st.subheader("Data Preview")
            st.dataframe(df, use_container_width=True)
            
            # Show column types
            st.subheader("Data Types")
            dtype_df = pd.DataFrame({
                'Column': df.columns,
                'Type': [str(dtype) for dtype in df.dtypes],
                'Non-Null': df.notnull().sum().values,
                'Null': df.isnull().sum().values
            })
            st.dataframe(dtype_df, use_container_width=True)
        else:
            st.warning(f"Table `{table_name}` exists but contains no data")

# Dashboard Section (only if we have data)
st.markdown("---")
st.header("📊 Dashboard Preview")

# Simple preview dashboard
if 'df' in locals() and df is not None and not df.empty:
    # Check for required columns
    money_col = None
    date_col = None
    product_col = None
    
    # Find appropriate columns
    for col in df.columns:
        if 'money' in col.lower() or 'price' in col.lower() or 'amount' in col.lower():
            money_col = col
        if 'date' in col.lower() or 'time' in col.lower():
            date_col = col
        if 'product' in col.lower() or 'coffee' in col.lower() or 'name' in col.lower():
            product_col = col
    
    if money_col:
        # Metrics
        total_sales = df[money_col].sum()
        total_trans = len(df)
        
        m1, m2 = st.columns(2)
        m1.metric("Total Sales", f"${total_sales:,.2f}")
        m2.metric("Total Transactions", f"{total_trans:,}")
        
        # Charts if we have date column
        if date_col:
            st.subheader("Sales Over Time")
            try:
                daily_sales = df.groupby(date_col)[money_col].sum().reset_index()
                st.line_chart(daily_sales.set_index(date_col))
            except:
                st.warning("Could not create time series chart")
        
        # Products if available
        if product_col:
            st.subheader("Product Distribution")
            product_sales = df.groupby(product_col)[money_col].sum().sort_values(ascending=False)
            st.bar_chart(product_sales.head(10))
else:
    st.info("👆 Load data in Step 4 to see dashboard preview")

# Troubleshooting Guide
st.markdown("---")
st.header("🛠️ Troubleshooting Guide")

with st.expander("Common Issues and Solutions", expanded=False):
    st.markdown("""
    ### 1. "Invalid access token" error
    **Solution:** 
    - Go to Databricks → User Settings → Developer → Access Tokens
    - Generate a **new token** (your old one might be expired)
    - Copy the token **immediately** (you won't see it again)
    - Update `DATABRICKS_TOKEN` in the code
    
    ### 2. "No tables found" error
    **Possible causes:**
    - Your token doesn't have permission to access tables
    - You're connected to the wrong database/catalog
    - No tables exist in your workspace
    
    **Solutions:**
    - Check token permissions in Databricks
    - Create a table in your Databricks workspace
    - Use the full table name: `catalog.database.table`
    
    ### 3. Table exists but query returns no data
    **Solution:**
    - The table might be empty
    - Check if you're querying the right database/catalog
    - Try: `SELECT * FROM default.your_table LIMIT 5`
    
    ### 4. Connection works but queries fail
    **Solution:**
    - Check if your warehouse is running (Status should be "Running")
    - Try stopping and starting your SQL warehouse
    - Check the warehouse size (might need larger cluster)
    """)

# Quick Fix Button
st.markdown("---")
if st.button("🔄 Quick Fix: Regenerate Token & Restart"):
    st.markdown("""
    **Follow these steps:**
    
    1. **Go to Databricks:** https://adb-7405619795306590.10.azuredatabricks.net
    2. **Navigate to:** User Settings → Developer → Access Tokens
    3. **Generate new token** with these settings:
       - Comment: "Streamlit Dashboard"
       - Lifetime: 90 days (recommended)
    4. **Copy the token** immediately
    5. **Update line 18** in this code with the new token
    6. **Save the file** and restart the Streamlit app
    
    **Also check:**
    - SQL Warehouse is running (Status: Running)
    - You have at least one table in your workspace
    - Your user has permission to query tables
    """)
