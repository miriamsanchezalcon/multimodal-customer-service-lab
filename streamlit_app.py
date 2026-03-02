import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

def esc(text):
    """Escape dollar signs to prevent LaTeX rendering."""
    if text is None:
        return ""
    return str(text).replace("$", "\\$")

def safe_query(sql, default=None):
    """Run a query, returning default if the table doesn't exist yet."""
    try:
        return session.sql(sql).collect()
    except Exception:
        return default

def safe_count(table, where=""):
    """Safely count rows in a table that may not exist yet."""
    clause = f" WHERE {where}" if where else ""
    result = safe_query(f"SELECT COUNT(*) FROM {table}{clause}")
    return result[0][0] if result else 0

def table_exists(name):
    """Check if a table exists in the current schema."""
    result = safe_query(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{name}' AND TABLE_SCHEMA = CURRENT_SCHEMA()")
    return bool(result)

# Clean Snowflake-inspired CSS
st.markdown("""
<style>
    .block-container {padding: 1.5rem 2rem;}
    
    /* Header styling */
    .app-header {
        background: linear-gradient(135deg, #11567C 0%, #29B5E8 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
    }
    .app-title {
        color: white;
        font-size: 1.75rem;
        font-weight: 600;
        margin: 0;
    }
    .app-subtitle {
        color: rgba(255,255,255,0.85);
        font-size: 0.95rem;
        margin: 0.25rem 0 0 0;
    }
    
    /* Metric cards */
    div[data-testid="stMetric"] {
        background: #F0F9FF;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #BAE6FD;
    }
    div[data-testid="stMetric"] label {color: #0369A1; font-weight: 500;}
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {color: #11567C;}
    
    /* Tabs */
    div[data-testid="stTabs"] [data-baseweb="tab-list"] {
        gap: 0.5rem;
        background: #F0F9FF;
        padding: 0.5rem;
        border-radius: 8px;
    }
    div[data-testid="stTabs"] [data-baseweb="tab"] {
        background: transparent;
        border-radius: 6px;
        padding: 0.5rem 1rem;
        color: #0369A1;
    }
    div[data-testid="stTabs"] [aria-selected="true"] {
        background: white;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    /* Expanders */
    .stExpander {border: 1px solid #E0F2FE; border-radius: 8px; background: #FAFEFF;}
    
    /* Subheaders */
    h3 {color: #11567C !important;}
    
    /* Dividers */
    hr {border-color: #E0F2FE !important;}
    
    /* Buttons and inputs */
    .stSelectbox > div > div {border-color: #BAE6FD;}
    .stTextInput > div > div > input {border-color: #BAE6FD;}
    
    /* Charts */
    .stBarChart {background: #FAFEFF; padding: 1rem; border-radius: 8px; border: 1px solid #E0F2FE;}
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="app-header">
    <p class="app-title">❄️ Customer Service Analytics</p>
    <p class="app-subtitle">Multimodal insights powered by Snowflake Cortex AI</p>
</div>
""", unsafe_allow_html=True)

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Call Analytics", "Chat Validation", "Alignment Issues"])

# ============ TAB 1: OVERVIEW ============
with tab1:
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    calls = safe_count("transcription_results")
    docs = safe_count("parsed_documents_raw")
    chats = safe_count("chat_validation_results")
    flagged = safe_count("chat_validation_results", "is_flagged = TRUE")
    misaligned = safe_count("ticket_chat_alignment", "alignment_status = 'misaligned'")
    
    col1.metric("Calls Processed", calls)
    col2.metric("Documents Parsed", docs)
    col3.metric("Chats Analyzed", chats)
    col4.metric("Issues Detected", flagged + misaligned)
    
    st.divider()
    
    # Row 1: Sentiment across sources
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Call Sentiment")
        sentiment_df = session.sql("""
            SELECT sentiment_label AS label, COUNT(*) AS count 
            FROM transcription_results 
            WHERE sentiment_label IS NOT NULL
            GROUP BY sentiment_label
        """).to_pandas()
        if not sentiment_df.empty:
            color_map = {'positive': '#2ecc71', 'negative': '#e74c3c', 'neutral': '#95a5a6', 'mixed': '#f39c12'}
            chart = alt.Chart(sentiment_df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
                x=alt.X('LABEL:N', title=None, sort='-y'),
                y=alt.Y('COUNT:Q', title='Count'),
                color=alt.Color('LABEL:N', scale=alt.Scale(domain=list(color_map.keys()), range=list(color_map.values())), legend=None)
            ).properties(height=250)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No call sentiment data yet")
    
    with col2:
        st.subheader("Chat Sentiment (AI vs Reported)")
        if table_exists('CHAT_VALIDATION_RESULTS'):
            chat_sent_df = session.sql("""
                SELECT 'AI Detected' AS source, ai_sentiment_normalized AS sentiment, COUNT(*) AS count
                FROM chat_validation_results
                WHERE ai_sentiment_normalized IS NOT NULL
                GROUP BY ai_sentiment_normalized
                UNION ALL
                SELECT 'Agent Reported', self_reported_sentiment, COUNT(*)
                FROM chat_validation_results
                WHERE self_reported_sentiment IS NOT NULL
                GROUP BY self_reported_sentiment
            """).to_pandas()
            if not chat_sent_df.empty:
                chart = alt.Chart(chat_sent_df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
                    x=alt.X('SOURCE:N', title=None),
                    y=alt.Y('COUNT:Q', title='Count'),
                    color=alt.Color('SOURCE:N', scale=alt.Scale(range=['#29B5E8', '#11567C']), title='Source'),
                    column=alt.Column('SENTIMENT:N', title=None, header=alt.Header(labelOrient='bottom'))
                ).properties(height=220, width=80)
                st.altair_chart(chart)
            else:
                st.info("No chat sentiment data yet")
        else:
            st.info("Run notebook Module 3 to see chat validation data")
    
    st.divider()
    
    # Row 2: Categories and alignment
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Chat Category Mismatches")
        if table_exists('CHAT_VALIDATION_RESULTS'):
            cat_df = session.sql("""
                SELECT 
                    CASE WHEN self_reported_category = ai_classified_category 
                         THEN 'Match' ELSE 'Mismatch' END AS status,
                    COUNT(*) AS count
                FROM chat_validation_results
                GROUP BY status
            """).to_pandas()
            if not cat_df.empty:
                chart = alt.Chart(cat_df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
                    x=alt.X('STATUS:N', title=None),
                    y=alt.Y('COUNT:Q', title='Count'),
                    color=alt.Color('STATUS:N', scale=alt.Scale(domain=['Match', 'Mismatch'], range=['#2ecc71', '#e74c3c']), legend=None)
                ).properties(height=250)
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No category data yet")
        else:
            st.info("Run notebook Module 3 to see category analysis")
    
    with col2:
        st.subheader("Ticket Alignment Severity")
        if table_exists('TICKET_CHAT_ALIGNMENT'):
            severity_df = session.sql("""
                SELECT 
                    COALESCE(misalignment_severity, 'aligned') AS severity,
                    COUNT(*) AS count
                FROM ticket_chat_alignment
                GROUP BY severity
                ORDER BY CASE severity 
                    WHEN 'critical' THEN 1 WHEN 'moderate' THEN 2 
                    WHEN 'minor' THEN 3 ELSE 4 END
            """).to_pandas()
            if not severity_df.empty:
                sev_colors = {'critical': '#e74c3c', 'moderate': '#f39c12', 'minor': '#2ecc71', 'aligned': '#29B5E8'}
                chart = alt.Chart(severity_df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
                    x=alt.X('SEVERITY:N', title=None, sort=['critical', 'moderate', 'minor', 'aligned']),
                    y=alt.Y('COUNT:Q', title='Count'),
                    color=alt.Color('SEVERITY:N', scale=alt.Scale(domain=list(sev_colors.keys()), range=list(sev_colors.values())), legend=None)
                ).properties(height=250)
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No alignment data yet")
        else:
            st.info("Run notebook Module 3 to see alignment analysis")
    
    st.divider()
    
    # Try it yourself
    st.subheader("Try Cortex AI")
    user_text = st.text_input("Enter a customer message to analyze:", placeholder="Example: I need help with my billing issue")
    
    if user_text:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            sentiment = session.sql(f"SELECT SNOWFLAKE.CORTEX.SENTIMENT($${user_text}$$)").collect()[0][0]
            label = "Positive" if sentiment > 0.2 else "Negative" if sentiment < -0.2 else "Neutral"
            color = "🟢" if sentiment > 0.2 else "🔴" if sentiment < -0.2 else "🟡"
            st.metric(f"{color} Sentiment", label, f"Score: {round(sentiment, 2)}")
        
        with col2:
            category = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT($${user_text}$$, 
                    ['Billing', 'Technical Support', 'Sales', 'Complaint', 'General'])['label']::STRING
            """).collect()[0][0]
            st.metric("Category", category)
        
        with col3:
            st.metric("AI Functions Used", "2", "SENTIMENT + CLASSIFY")

# ============ TAB 2: CALL ANALYTICS ============
with tab2:
    st.subheader("Processed Call Recordings")
    
    calls_df = session.sql("""
        SELECT file_name, ROUND(audio_duration, 1) as duration_sec,
               sentiment_label, call_category, call_summary
        FROM transcription_results
        ORDER BY transcription_completed_at DESC
    """).to_pandas()
    
    if not calls_df.empty:
        # Filter
        sentiment_filter = st.selectbox("Filter by sentiment", ["All", "positive", "negative", "neutral"])
        
        filtered = calls_df if sentiment_filter == "All" else calls_df[calls_df['SENTIMENT_LABEL'] == sentiment_filter]
        
        for _, row in filtered.iterrows():
            icon = {"positive": "🟢", "negative": "🔴", "neutral": "🟡"}.get(row['SENTIMENT_LABEL'], "⚪")
            with st.expander(f"{icon} {row['FILE_NAME']} — {row['DURATION_SEC']}s — {row['CALL_CATEGORY']}"):
                st.write(f"**Sentiment:** {row['SENTIMENT_LABEL']}")
                st.write(f"**Category:** {row['CALL_CATEGORY']}")
                st.write(f"**Summary:** {esc(row['CALL_SUMMARY'])}")
    else:
        st.info("No calls processed yet. Run the notebook first!")

# ============ TAB 3: CHAT VALIDATION ============
with tab3:
    st.subheader("Chat Validation Results")
    
    if table_exists('CHAT_VALIDATION_RESULTS'):
        col1, col2 = st.columns([1, 3])
        with col1:
            show_flagged = st.checkbox("Show flagged only", value=True)
        
        query = """
            SELECT chat_id, customer_name, 
                   self_reported_category, ai_classified_category,
                   self_reported_sentiment, ai_sentiment_normalized,
                   is_flagged, flag_reasons
            FROM chat_validation_results
        """
        if show_flagged:
            query += " WHERE is_flagged = TRUE"
        query += " ORDER BY chat_timestamp DESC LIMIT 50"
        
        chats_df = session.sql(query).to_pandas()
        
        if not chats_df.empty:
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            total = len(chats_df)
            cat_mismatch = len(chats_df[chats_df['SELF_REPORTED_CATEGORY'] != chats_df['AI_CLASSIFIED_CATEGORY']])
            sent_mismatch = len(chats_df[chats_df['SELF_REPORTED_SENTIMENT'] != chats_df['AI_SENTIMENT_NORMALIZED']])
            
            col1.metric("Showing", total)
            col2.metric("Category Mismatches", cat_mismatch)
            col3.metric("Sentiment Mismatches", sent_mismatch)
            
            st.divider()
            st.dataframe(chats_df, use_container_width=True)
        else:
            st.info("No flagged chats found." if show_flagged else "No chats processed yet.")
    else:
        st.info("No data yet. Complete notebook Module 3 (Chat & Ticket Validation) first, then refresh this page.")

# ============ TAB 4: ALIGNMENT ISSUES ============
with tab4:
    st.subheader("Ticket-Chat Alignment Analysis")
    
    if table_exists('TICKET_CHAT_ALIGNMENT'):
        # Summary
        col1, col2, col3 = st.columns(3)
        
        total = safe_count("ticket_chat_alignment")
        aligned = safe_count("ticket_chat_alignment", "alignment_status = 'aligned'")
        critical = safe_count("ticket_chat_alignment", "misalignment_severity = 'critical'")
        
        col1.metric("Total Pairs", total)
        col2.metric("Aligned", aligned, f"{round(aligned/total*100)}%" if total > 0 else "0%")
        col3.metric("Critical Issues", critical)
        
        st.divider()
        
        # Filter
        severity_filter = st.selectbox("Filter by severity", ["All", "critical", "moderate", "minor"])
        
        query = """
            SELECT ticket_number, ticket_subject, alignment_status, alignment_confidence,
                   alignment_reason, misalignment_severity, category_mismatch_flag, product_mismatch_flag
            FROM ticket_chat_alignment
            WHERE alignment_status = 'misaligned'
        """
        if severity_filter != "All":
            query += f" AND misalignment_severity = '{severity_filter}'"
        query += " ORDER BY CASE misalignment_severity WHEN 'critical' THEN 1 WHEN 'moderate' THEN 2 ELSE 3 END LIMIT 20"
        
        issues_df = session.sql(query).to_pandas()
        
        if not issues_df.empty:
            for _, row in issues_df.iterrows():
                icon = {"critical": "🔴", "moderate": "🟡", "minor": "🟢"}.get(row['MISALIGNMENT_SEVERITY'], "⚪")
                
                with st.expander(f"{icon} {row['TICKET_NUMBER']}: {esc(row['TICKET_SUBJECT'][:60])}..."):
                    col1, col2 = st.columns(2)
                    col1.metric("Alignment", row['ALIGNMENT_STATUS'])
                    col2.metric("Confidence", row['ALIGNMENT_CONFIDENCE'])
                    
                    st.write(f"**Reason:** {esc(row['ALIGNMENT_REASON'])}")
                    
                    if row['CATEGORY_MISMATCH_FLAG']:
                        st.warning("Category mismatch detected")
                    if row['PRODUCT_MISMATCH_FLAG']:
                        st.warning("Product mismatch detected")
        else:
            st.success("No misalignment issues found!")
    else:
        st.info("No data yet. Complete notebook Module 3 (Chat & Ticket Validation) first, then refresh this page.")

# Footer
st.divider()
st.caption("Built with Snowflake Cortex AI")
