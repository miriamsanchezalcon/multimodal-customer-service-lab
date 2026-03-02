import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Customer Service Analytics",
    page_icon="🎧",
    layout="wide"
)

session = get_active_session()

st.title("🎧 Customer Service Analytics Dashboard")
st.markdown("Insights from multimodal customer service data powered by **Cortex AI**")

tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📞 Call Analytics", "💬 Chat Validation", "🔍 Alignment Issues"])

with tab1:
    col1, col2, col3, col4 = st.columns(4)
    
    calls_count = session.sql("SELECT COUNT(*) FROM transcription_results").collect()[0][0]
    chats_count = session.sql("SELECT COUNT(*) FROM chat_validation_results").collect()[0][0]
    flagged_chats = session.sql("SELECT COUNT(*) FROM chat_validation_results WHERE is_flagged = TRUE").collect()[0][0]
    misaligned = session.sql("SELECT COUNT(*) FROM ticket_chat_alignment WHERE alignment_status = 'misaligned'").collect()[0][0]
    
    col1.metric("📞 Calls Processed", calls_count)
    col2.metric("💬 Chats Analyzed", chats_count)
    col3.metric("🚩 Flagged Chats", flagged_chats, delta=f"{round(flagged_chats/chats_count*100)}%" if chats_count > 0 else "0%")
    col4.metric("⚠️ Misaligned Tickets", misaligned)
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Sentiment Distribution")
        sentiment_df = session.sql("""
            SELECT sentiment_label, COUNT(*) as count 
            FROM transcription_results 
            WHERE sentiment_label IS NOT NULL
            GROUP BY sentiment_label
        """).to_pandas()
        if not sentiment_df.empty:
            st.bar_chart(sentiment_df.set_index('SENTIMENT_LABEL'))
    
    with col2:
        st.subheader("Call Categories")
        category_df = session.sql("""
            SELECT call_category, COUNT(*) as count 
            FROM transcription_results 
            WHERE call_category IS NOT NULL
            GROUP BY call_category
        """).to_pandas()
        if not category_df.empty:
            st.bar_chart(category_df.set_index('CALL_CATEGORY'))

with tab2:
    st.subheader("📞 Processed Call Recordings")
    
    calls_df = session.sql("""
        SELECT 
            file_name,
            ROUND(audio_duration, 1) as duration_sec,
            sentiment_label,
            call_category,
            call_summary
        FROM transcription_results
        ORDER BY transcription_completed_at DESC
    """).to_pandas()
    
    if not calls_df.empty:
        for _, row in calls_df.iterrows():
            with st.expander(f"🎙️ {row['FILE_NAME']} ({row['DURATION_SEC']}s) - {row['SENTIMENT_LABEL']}"):
                st.markdown(f"**Category:** {row['CALL_CATEGORY']}")
                st.markdown(f"**Summary:** {row['CALL_SUMMARY']}")
    else:
        st.info("No calls processed yet. Run the notebook first!")

with tab3:
    st.subheader("💬 Chat Validation Results")
    
    show_flagged_only = st.checkbox("Show flagged chats only", value=True)
    
    query = """
        SELECT 
            chat_id,
            customer_name,
            self_reported_category,
            ai_classified_category,
            self_reported_sentiment,
            ai_sentiment_normalized,
            is_flagged,
            flag_reasons
        FROM chat_validation_results
    """
    if show_flagged_only:
        query += " WHERE is_flagged = TRUE"
    query += " ORDER BY chat_timestamp DESC"
    
    chats_df = session.sql(query).to_pandas()
    
    if not chats_df.empty:
        st.dataframe(
            chats_df,
            column_config={
                "IS_FLAGGED": st.column_config.CheckboxColumn("Flagged"),
                "FLAG_REASONS": st.column_config.ListColumn("Issues")
            },
            use_container_width=True
        )
    else:
        st.info("No flagged chats found." if show_flagged_only else "No chats processed yet.")

with tab4:
    st.subheader("🔍 Ticket-Chat Alignment Analysis")
    
    alignment_df = session.sql("""
        SELECT 
            ticket_number,
            ticket_subject,
            alignment_status,
            alignment_confidence,
            alignment_reason,
            misalignment_severity,
            category_mismatch_flag,
            product_mismatch_flag
        FROM ticket_chat_alignment
        WHERE is_flagged = TRUE
        ORDER BY 
            CASE misalignment_severity 
                WHEN 'critical' THEN 1 
                WHEN 'moderate' THEN 2 
                ELSE 3 
            END
    """).to_pandas()
    
    if not alignment_df.empty:
        for _, row in alignment_df.iterrows():
            severity_color = {"critical": "🔴", "moderate": "🟡", "minor": "🟢"}.get(row['MISALIGNMENT_SEVERITY'], "⚪")
            with st.expander(f"{severity_color} {row['TICKET_NUMBER']}: {row['TICKET_SUBJECT'][:50]}..."):
                col1, col2 = st.columns(2)
                col1.metric("Alignment", row['ALIGNMENT_STATUS'])
                col2.metric("Confidence", row['ALIGNMENT_CONFIDENCE'])
                st.markdown(f"**Reason:** {row['ALIGNMENT_REASON']}")
                if row['CATEGORY_MISMATCH_FLAG']:
                    st.warning("Category mismatch detected")
                if row['PRODUCT_MISMATCH_FLAG']:
                    st.warning("Product mismatch detected")
    else:
        st.success("No alignment issues found!")

st.divider()
st.caption("Built with Snowflake Cortex AI • Data is synthetic for demonstration purposes")
