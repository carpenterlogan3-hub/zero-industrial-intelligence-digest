"""BR_03_send_digest_slack.py

For stakeholders with non-null slack_channel (currently Logan only → #digest-ai),
post plain-text version of digest to Slack. Runs AFTER email delivery.

Exceptions:
    SE-01: Slack auth error → log, skip all Slack, rely on email.
    SE-02: Channel not found → log, alert admin, skip this post.
"""
