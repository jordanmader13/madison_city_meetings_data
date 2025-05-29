import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path

def run_web_interface():
    """Run the Streamlit web interface"""
    st.set_page_config(page_title="Madison City Council Votes", layout="wide")
    st.title("Madison City Council Votes Explorer")

    # Initialize database connection
    @st.cache_resource
    def init_connection():
        return duckdb.connect('madison_votes.db')

    db = init_connection()

    # Sidebar with quick stats
    with st.sidebar:
        st.header("Quick Stats")
        stats = db.execute("""
            SELECT 
                COUNT(DISTINCT meeting_date) as total_meetings,
                COUNT(*) as total_votes,
                SUM(CASE WHEN NOT is_unanimous THEN 1 ELSE 0 END) as non_unanimous_votes
            FROM votes_summary
        """).fetch_df().iloc[0]
        
        st.metric("Total Meetings", stats['total_meetings'])
        st.metric("Total Votes", stats['total_votes'])
        st.metric("Non-unanimous Votes", stats['non_unanimous_votes'])

    # Main area tabs
    tab1, tab2, tab3 = st.tabs(["Quick Views", "Custom Query", "Schema"])

    with tab1:
        st.header("Pre-built Views")
        view_choice = st.selectbox(
            "Select a view",
            ["Votes with Member Voting Records", "Member Voting Patterns", "Most Active Voters"]
        )

        if view_choice == "Votes with Member Voting Records":
            st.dataframe(db.execute("""
                SELECT * FROM votes_with_voters
                WHERE is_unanimous = FALSE
            """).fetch_df())

        elif view_choice == "Member Voting Patterns":
            st.dataframe(db.execute("""
                SELECT * FROM member_voting_patterns
                ORDER BY member_name, vote_count DESC
            """).fetch_df())

        elif view_choice == "Most Active Voters":
            st.dataframe(db.execute("""
                SELECT 
                    member_name, 
                    COUNT(*) as vote_count,
                    COUNT(DISTINCT meeting_date) as meetings_attended
                FROM votes_detailed 
                GROUP BY member_name 
                ORDER BY vote_count DESC
            """).fetch_df())

    with tab2:
        st.header("Custom SQL Query")
        query = st.text_area("Enter your SQL query:", height=150)
        if st.button("Run Query"):
            try:
                results = db.execute(query).fetch_df()
                st.dataframe(results)
            except Exception as e:
                st.error(f"Error executing query: {e}")

    with tab3:
        st.header("Database Schema")
        tables = db.execute("""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetch_df()
        
        for _, row in tables.iterrows():
            st.subheader(f"{row['table_name']} ({row['table_type']})")
            schema = db.execute(f"DESCRIBE {row['table_name']}").fetch_df()
            st.dataframe(schema)

if __name__ == "__main__":
    run_web_interface() 