"""BR_03_role_distribution_config.py

Load config/distribution_config.yaml. Validate each stakeholder has non-empty
email and prompt_template_file that exists on disk.

Returns: List[Dict] — validated stakeholder config entries.

Exceptions:
    BE-01: Missing required fields → log warning, skip invalid stakeholders, continue with valid.
"""
