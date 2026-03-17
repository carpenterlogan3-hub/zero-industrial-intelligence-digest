"""BR_04_compile_summary.py

Aggregate runtime counters into a summary dict with exactly 15 keys:
    pipeline_run_date, pipeline_start_time, pipeline_end_time,
    feeds_attempted, feeds_errored, articles_fetched, articles_new,
    articles_classified, articles_classification_errors,
    digests_generated, emails_sent, emails_failed,
    slack_messages_sent, total_errors, error_highlights (first 5, 200 chars each).
"""
