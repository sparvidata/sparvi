from sparvi.profiler.profile_engine import profile_table as core_profile_table


def cloud_profile_table(connection_string, table_name, previous_profile=None):
    # Call the core function
    result = core_profile_table(connection_string, table_name, previous_profile)

    # Add cloud-specific enhancements
    result["cloud_features"] = {
        "user_id": get_current_user_id(),
        "organization_id": get_current_organization_id(),
        # Other cloud-specific data
    }

    return result