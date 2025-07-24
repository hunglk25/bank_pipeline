import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
from datetime import datetime, timedelta
import numpy as np

st.set_page_config(
    page_title="Bank Data Analytics Dashboard",
    layout="wide"
)

def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'postgres_data'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'mydata'),
            user=os.getenv('DB_USER', 'user'),
            password=os.getenv('DB_PASSWORD', 'userpass')
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load all data from database"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        # Load all tables
        customers = pd.read_sql("SELECT * FROM Customer", conn)
        devices = pd.read_sql("SELECT * FROM Device", conn)
        accounts = pd.read_sql("SELECT * FROM Account", conn)
        transactions = pd.read_sql("SELECT * FROM Transaction", conn)
        auth_logs = pd.read_sql("SELECT * FROM AuthenticationLog", conn)
        
        conn.close()
        
        return {
            'customers': customers,
            'devices': devices,
            'accounts': accounts,
            'transactions': transactions,
            'auth_logs': auth_logs
        }
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        conn.close()
        return None

def create_overview_metrics(data):
    """Create overview metrics"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Customers", len(data['customers']))
    
    with col2:
        st.metric("Total Devices", len(data['devices']))
    
    with col3:
        st.metric("Total Accounts", len(data['accounts']))
    
    with col4:
        st.metric("Total Transactions", len(data['transactions']))


def create_customer_analysis(data):
    """Create customer analysis charts"""
    st.subheader("👥 Customer Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Devices per customer
        if not data['devices'].empty:
            device_counts = data['devices'].groupby('customerid').size().value_counts().sort_index()
            fig = px.bar(
                x=device_counts.index,
                y=device_counts.values,
                labels={'x': 'Number of Devices', 'y': 'Number of Customers'},
                title="Distribution of Devices per Customer"
            )
        else:
            fig = px.bar(title="Distribution of Devices per Customer (No Data)")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Device verification status
        verification_counts = data['devices']['isverified'].value_counts()
        fig = px.pie(
            values=verification_counts.values,
            names=['Verified' if x else 'Not Verified' for x in verification_counts.index],
            title="Device Verification Status"
        )
        st.plotly_chart(fig, use_container_width=True)

def create_account_analysis(data):
    """Create account analysis charts"""
    st.subheader("Account Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Account types distribution
        account_types = data['accounts']['accounttype'].value_counts()
        fig = px.pie(
            values=account_types.values,
            names=account_types.index,
            title="Account Types Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Balance distribution by account type
        fig = px.box(
            data['accounts'],
            x='accounttype',
            y='balance',
            title="Balance Distribution by Account Type"
        )
        fig.update_layout(yaxis_title="Balance ($)")
        st.plotly_chart(fig, use_container_width=True)

def create_transaction_analysis(data):
    """Create transaction analysis charts"""
    st.subheader("Transaction Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Transaction amounts distribution
        fig = px.histogram(
            data['transactions'],
            x='amount',
            nbins=30,
            title="Transaction Amount Distribution"
        )
        fig.update_layout(xaxis_title="Amount ($)")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Risk flag analysis
        risk_counts = data['transactions']['riskflag'].value_counts()
        fig = px.pie(
            values=risk_counts.values,
            names=['High Risk' if x else 'Normal' for x in risk_counts.index],
            title="Transaction Risk Analysis"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Transaction timeline
    if 'timestamp' in data['transactions'].columns:
        st.subheader("Transaction Timeline")
        transactions_with_date = data['transactions'].copy()
        transactions_with_date['timestamp'] = pd.to_datetime(transactions_with_date['timestamp'])
        transactions_with_date['date'] = transactions_with_date['timestamp'].dt.date
        
        daily_transactions = transactions_with_date.groupby('date').agg({
            'amount': ['count', 'sum']
        }).round(2)
        daily_transactions.columns = ['Transaction Count', 'Total Amount']
        daily_transactions = daily_transactions.reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_transactions['date'],
            y=daily_transactions['Transaction Count'],
            mode='lines+markers',
            name='Transaction Count',
            yaxis='y'
        ))
        
        fig.add_trace(go.Scatter(
            x=daily_transactions['date'],
            y=daily_transactions['Total Amount'],
            mode='lines+markers',
            name='Total Amount ($)',
            yaxis='y2'
        ))
        
        fig.update_layout(
            title='Daily Transaction Trends',
            xaxis_title='Date',
            yaxis=dict(title='Transaction Count'),
            yaxis2=dict(title='Total Amount ($)', overlaying='y', side='right'),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)

def create_auth_analysis(data):
    """Create authentication analysis charts"""
    st.subheader("Authentication Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if not data['auth_logs'].empty:
            auth_methods = data['auth_logs']['authmethod'].value_counts().reset_index()
            auth_methods.columns = ['authmethod', 'count']

            if not auth_methods.empty:
                fig = px.bar(
                    auth_methods,
                    x='count',
                    y='authmethod',
                    orientation='h',
                    title="Authentication Methods Usage"
                )
                fig.update_layout(xaxis_title="Count", yaxis_title="Authentication Method")
            else:
                fig = px.bar(title="Authentication Methods Usage (No Data)")
        else:
            fig = px.bar(title="Authentication Methods Usage (No Data)")

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if not data['auth_logs'].empty and 'authstatus' in data['auth_logs'].columns:
            auth_status = data['auth_logs']['authstatus'].value_counts()
            total = len(data['auth_logs'])
            success_count = auth_status.get('SUCCESS', 0)
            success_rate = (success_count / total * 100) if total > 0 else 0.0
        else:
            success_rate = 0.0

        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=success_rate,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Authentication Success Rate (%)"},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': "darkgreen"},
                'steps': [
                    {'range': [0, 50], 'color': "lightgray"},
                    {'range': [50, 80], 'color': "yellow"},
                    {'range': [80, 100], 'color': "lightgreen"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))

        st.plotly_chart(fig, use_container_width=True)


def create_data_tables(data):
    """Create data tables for detailed view"""
    st.subheader("📊 Data Tables")
    
    table_option = st.selectbox(
        "Select table to view:",
        ["Customers", "Devices", "Accounts", "Transactions", "Authentication Logs"]
    )
    
    if table_option == "Customers":
        st.dataframe(data['customers'], use_container_width=True)
    elif table_option == "Devices":
        st.dataframe(data['devices'], use_container_width=True)
    elif table_option == "Accounts":
        st.dataframe(data['accounts'], use_container_width=True)
    elif table_option == "Transactions":
        st.dataframe(data['transactions'], use_container_width=True)
    elif table_option == "Authentication Logs":
        st.dataframe(data['auth_logs'], use_container_width=True)

def main():
    st.title("🏦 Bank Data Analytics Dashboard")
    st.markdown("Real-time analytics dashboard for banking data")
    
    # Load data
    with st.spinner("Loading data from database..."):
        data = load_data()
    
    if data is None:
        st.error("Failed to load data from database")
        return
    
    # Sidebar for refresh
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("### Data Overview")
    for table_name, df in data.items():
        st.sidebar.write(f"**{table_name.title()}**: {len(df)} records")
    
    # Main dashboard
    create_overview_metrics(data)
    
    st.divider()
    
    # Analysis sections
    create_customer_analysis(data)
    create_account_analysis(data)
    create_transaction_analysis(data)
    create_auth_analysis(data)
    
    st.divider()
    
    # Data tables
    create_data_tables(data)
    
    # Footer
    st.markdown("---")
    st.markdown("*Dashboard last updated: {}*".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

if __name__ == "__main__":
    main()