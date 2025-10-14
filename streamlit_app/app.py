"""
FlightLake Streamlit Dashboard

Interactive dashboard for demonstrating MariaDB InnoDB vs ColumnStore
performance for analytical queries. Allows users to execute queries,
compare performance, and visualize business insights.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import sys
from pathlib import Path

# Add parent directory to path to import scripts
sys.path.append(str(Path(__file__).parent.parent))

from scripts.config import INNODB_TABLE, COLUMNSTORE_TABLE, DASHBOARD_CONFIG
from scripts.db_connector import DatabaseConnection, compare_storage
from scripts.queries import QUERIES, list_queries_by_category
from scripts.utils import format_time, calculate_speedup


# Page configuration
st.set_page_config(
    page_title=DASHBOARD_CONFIG['page_title'],
    page_icon=DASHBOARD_CONFIG['page_icon'],
    layout=DASHBOARD_CONFIG['layout']
)


# Initialize session state
if 'results' not in st.session_state:
    st.session_state['results'] = None
if 'storage_metrics' not in st.session_state:
    st.session_state['storage_metrics'] = None


def execute_query(query_key: str, engine_mode: str) -> dict:
    """
    Execute query on selected engine(s) and measure performance.

    Args:
        query_key: Query identifier
        engine_mode: "Both (Compare)", "InnoDB Only", or "ColumnStore Only"

    Returns:
        Dictionary with results and timing information
    """
    query_info = QUERIES[query_key]
    results = {'query_key': query_key, 'query_info': query_info}

    if engine_mode in ["Both (Compare)", "InnoDB Only"]:
        # Execute on InnoDB
        with DatabaseConnection(INNODB_TABLE) as innodb_conn:
            innodb_sql = query_info['sql'].format(table_name=INNODB_TABLE)

            start = time.time()
            innodb_data = innodb_conn.execute_query(innodb_sql)
            innodb_time = time.time() - start

            # Get column names
            column_names = innodb_conn.get_column_names()

            results['innodb'] = {
                'time': innodb_time,
                'data': innodb_data,
                'columns': column_names
            }

    if engine_mode in ["Both (Compare)", "ColumnStore Only"]:
        # Execute on ColumnStore
        with DatabaseConnection(COLUMNSTORE_TABLE) as cs_conn:
            cs_sql = query_info['sql'].format(table_name=COLUMNSTORE_TABLE)

            start = time.time()
            cs_data = cs_conn.execute_query(cs_sql)
            cs_time = time.time() - start

            # Get column names
            column_names = cs_conn.get_column_names()

            results['columnstore'] = {
                'time': cs_time,
                'data': cs_data,
                'columns': column_names
            }

    return results


def display_results(results: dict, engine_mode: str) -> None:
    """
    Display query results in tables.

    Args:
        results: Query results dictionary
        engine_mode: Selected engine mode
    """
    if engine_mode == "Both (Compare)":
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("**InnoDB Results**")
            st.caption(f"{format_time(results['innodb']['time'])}")

            df_innodb = pd.DataFrame(
                results['innodb']['data'],
                columns=results['innodb']['columns']
            )
            st.dataframe(df_innodb, use_container_width=True, height=400)

        with col_b:
            st.markdown("**ColumnStore Results**")
            st.caption(f"{format_time(results['columnstore']['time'])}")

            df_cs = pd.DataFrame(
                results['columnstore']['data'],
                columns=results['columnstore']['columns']
            )
            st.dataframe(df_cs, use_container_width=True, height=400)

    elif engine_mode == "InnoDB Only":
        st.caption(f"{format_time(results['innodb']['time'])}")
        df = pd.DataFrame(
            results['innodb']['data'],
            columns=results['innodb']['columns']
        )
        st.dataframe(df, use_container_width=True, height=400)

    else:  # ColumnStore Only
        st.caption(f"{format_time(results['columnstore']['time'])}")
        df = pd.DataFrame(
            results['columnstore']['data'],
            columns=results['columnstore']['columns']
        )
        st.dataframe(df, use_container_width=True, height=400)


def display_performance_metrics(results: dict, engine_mode: str) -> None:
    """
    Display performance comparison metrics.

    Args:
        results: Query results dictionary
        engine_mode: Selected engine mode
    """
    if engine_mode == "Both (Compare)":
        innodb_time = results['innodb']['time']
        cs_time = results['columnstore']['time']
        speedup = calculate_speedup(innodb_time, cs_time)

        # Winner badge
        if cs_time < innodb_time:
            st.success(f"ColumnStore wins by **{speedup:.1f}x**")
        else:
            st.info(f"InnoDB wins by **{1/speedup:.1f}x**")

        # Timing comparison chart
        fig = go.Figure(data=[
            go.Bar(
                x=['InnoDB', 'ColumnStore'],
                y=[innodb_time, cs_time],
                marker_color=['#3b82f6', '#10b981'],
                text=[format_time(innodb_time), format_time(cs_time)],
                textposition='auto'
            )
        ])
        fig.update_layout(
            title="Execution Time Comparison",
            yaxis_title="Time (seconds)",
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

        # Metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("InnoDB", format_time(innodb_time))
        col2.metric("ColumnStore", format_time(cs_time))
        col3.metric("Speedup", f"{speedup:.1f}x")

    else:
        # Single engine mode
        engine = 'innodb' if engine_mode == "InnoDB Only" else 'columnstore'
        exec_time = results[engine]['time']
        st.metric("Execution Time", format_time(exec_time))


def display_storage_metrics() -> None:
    """
    Display storage size comparison.
    """
    if st.session_state['storage_metrics'] is None:
        try:
            st.session_state['storage_metrics'] = compare_storage(
                INNODB_TABLE, COLUMNSTORE_TABLE
            )
        except Exception as e:
            st.error(f"Error retrieving storage metrics: {e}")
            return

    metrics = st.session_state['storage_metrics']
    innodb_size = metrics['innodb'].get('total_mb', 0)
    cs_size = metrics['columnstore'].get('total_mb', 0)
    compression = metrics['compression_ratio']

    col1, col2, col3 = st.columns(3)

    col1.metric("InnoDB Size", f"{innodb_size:,.0f} MB")
    col2.metric("ColumnStore Size", f"{cs_size:,.0f} MB")
    col3.metric("Compression", f"{compression:.1f}x")

    # Compression chart
    fig = go.Figure(data=[
        go.Bar(
            x=['InnoDB', 'ColumnStore'],
            y=[innodb_size, cs_size],
            marker_color=['#3b82f6', '#10b981'],
            text=[f'{innodb_size:,.0f} MB', f'{cs_size:,.0f} MB'],
            textposition='auto'
        )
    ])
    fig.update_layout(
        title="Storage Size Comparison",
        yaxis_title="Size (MB)",
        height=300,
        showlegend=False
    )
    st.plotly_chart(fig, use_container_width=True)


def display_business_insights(results: dict) -> None:
    """
    Display business-relevant visualizations based on query results.

    Args:
        results: Query results dictionary
    """
    query_key = results['query_key']

    # Get data from whichever engine was used
    if 'innodb' in results:
        data = results['innodb']['data']
        columns = results['innodb']['columns']
    else:
        data = results['columnstore']['data']
        columns = results['columnstore']['columns']

    df = pd.DataFrame(data, columns=columns)

    if df.empty:
        st.info("No data to visualize")
        return

    # Query-specific visualizations
    if query_key == "top_10_hubs":
        fig = px.bar(
            df,
            x='origin_airport',
            y='total_seats',
            title="Top 10 Busiest Hubs by Seat Capacity",
            labels={'total_seats': 'Total Seats', 'origin_airport': 'Airport'},
            color='total_seats',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)

    elif query_key == "regional_capacity":
        # Create a heatmap of regional flows
        pivot = df.pivot_table(
            index='origin_region',
            columns='destination_region',
            values='total_capacity',
            aggfunc='sum'
        ).fillna(0)

        fig = px.imshow(
            pivot,
            title="Regional Capacity Flow Matrix",
            labels=dict(x="Destination Region", y="Origin Region", color="Total Capacity"),
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)

    elif query_key == "capacity_trends":
        # Time series line chart
        df['date'] = pd.to_datetime(df[['flight_year', 'flight_month']].assign(day=1))

        fig = px.line(
            df,
            x='date',
            y='total_seats',
            color='origin_region',
            title="Capacity Trends by Region Over Time",
            labels={'total_seats': 'Total Seats', 'date': 'Date'}
        )
        st.plotly_chart(fig, use_container_width=True)

    elif query_key == "distance_analysis":
        # Bar chart for distance categories
        fig = px.bar(
            df,
            x='distance_category',
            y='total_seats',
            title="Capacity Distribution by Flight Distance",
            labels={'total_seats': 'Total Seats', 'distance_category': 'Distance Category'},
            color='avg_seats',
            color_continuous_scale='Greens'
        )
        st.plotly_chart(fig, use_container_width=True)

    elif query_key == "hub_concentration":
        # Top N hubs with cumulative percentage
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=df['origin_airport'][:10],
            y=df['hub_seats'][:10],
            name='Hub Seats',
            yaxis='y',
            marker_color='#3b82f6'
        ))

        fig.add_trace(go.Scatter(
            x=df['origin_airport'][:10],
            y=df['cumulative_pct'][:10],
            name='Cumulative %',
            yaxis='y2',
            mode='lines+markers',
            marker_color='#ef4444',
            line=dict(width=3)
        ))

        fig.update_layout(
            title="Hub Concentration Analysis",
            xaxis_title="Airport",
            yaxis_title="Total Seats",
            yaxis2=dict(
                title="Cumulative %",
                overlaying='y',
                side='right'
            ),
            hovermode='x unified',
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)


# Main App Layout
def main():
    """
    Main application layout and logic.
    """
    # Title
    st.title("FlightLake - Hybrid Analytics Platform")
    st.markdown("**Demonstrating MariaDB InnoDB vs ColumnStore Performance**")
    st.divider()

    # Sidebar
    with st.sidebar:
        st.header("Configuration")

        # Query selector
        categories = list_queries_by_category()
        category_options = ["All Queries"] + list(categories.keys())

        selected_category = st.selectbox("Filter by Category", category_options)

        # Build query options based on selected category
        if selected_category == "All Queries":
            query_options = {info['name']: key for key, info in QUERIES.items()}
        else:
            query_keys = categories[selected_category]
            query_options = {QUERIES[key]['name']: key for key in query_keys}

        selected_query_name = st.selectbox("Select Query", list(query_options.keys()))
        selected_query_key = query_options[selected_query_name]

        # Display query description
        st.info(QUERIES[selected_query_key]['description'])

        # Display use case
        if 'use_case' in QUERIES[selected_query_key]:
            st.caption(f"**Use Case:** {QUERIES[selected_query_key]['use_case']}")

        # Engine selector
        engine_mode = st.radio(
            "Execute On",
            options=["Both (Compare)", "InnoDB Only", "ColumnStore Only"]
        )

        # Run button
        run_button = st.button("Execute Query", type="primary", use_container_width=True)

        st.divider()

        # Storage metrics refresh
        st.subheader("Storage Metrics")
        if st.button("Refresh Storage Stats"):
            st.session_state['storage_metrics'] = None
            st.rerun()

    # Main content area
    tab1, tab2, tab3 = st.tabs(["Query Results", "Performance", "Insights"])

    with tab1:
        st.subheader("Query Results")

        if run_button:
            with st.spinner("Executing query..."):
                try:
                    results = execute_query(selected_query_key, engine_mode)
                    st.session_state['results'] = results
                    st.success("Query executed successfully!")
                except Exception as e:
                    st.error(f"Error executing query: {e}")
                    st.session_state['results'] = None

        if st.session_state['results']:
            display_results(st.session_state['results'], engine_mode)
        else:
            st.info("Click 'Execute Query' to run a query and see results")

    with tab2:
        st.subheader("Performance Metrics")

        if st.session_state['results']:
            display_performance_metrics(st.session_state['results'], engine_mode)
        else:
            st.info("Execute a query to see performance metrics")

    with tab3:
        st.subheader("Business Insights")

        if st.session_state['results']:
            display_business_insights(st.session_state['results'])
        else:
            st.info("Execute a query to see business insights")

    # Storage comparison section (always visible)
    st.divider()
    st.subheader("Storage Comparison")
    display_storage_metrics()

    # Footer
    st.divider()
    st.caption("Built with Streamlit | Powered by MariaDB InnoDB + ColumnStore")


if __name__ == "__main__":
    main()
