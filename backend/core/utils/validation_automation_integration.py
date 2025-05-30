import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class ValidationAutomationIntegrator:
    """Integrates validation system with automation for automated validation runs"""

    def __init__(self, validation_manager, supabase_manager):
        """
        Initialize integrator

        Args:
            validation_manager: Instance of SupabaseValidationManager
            supabase_manager: Instance of SupabaseManager
        """
        self.validation_manager = validation_manager
        self.supabase_manager = supabase_manager

    def run_automated_validations(self, connection_id: str, organization_id: str) -> Dict[str, Any]:
        """
        Run automated validations for all tables configured for automation
        This is called by the automation scheduler

        Args:
            connection_id: Database connection ID
            organization_id: Organization ID

        Returns:
            Dictionary with validation results summary
        """
        try:
            logger.info(f"Running automated validations for connection {connection_id}")

            # Get connection details
            connection = self.supabase_manager.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection not found: {connection_id}")

            # Build connection string
            connection_string = self._build_connection_string(connection)

            # Get all tables that have automation enabled
            automated_tables = self._get_automated_validation_tables(connection_id, organization_id)

            results_summary = {
                "tables_processed": 0,
                "total_rules": 0,
                "passed_rules": 0,
                "failed_rules": 0,
                "tables_with_failures": [],
                "errors": []
            }

            for table_name in automated_tables:
                try:
                    logger.info(f"Running validations for table: {table_name}")

                    # Execute validation rules for this table
                    validation_results = self.validation_manager.execute_rules(
                        organization_id=organization_id,
                        connection_string=connection_string,
                        table_name=table_name,
                        connection_id=connection_id
                    )

                    # Process results
                    results_summary["tables_processed"] += 1
                    results_summary["total_rules"] += len(validation_results)

                    passed = sum(1 for r in validation_results if r.get("is_valid", False))
                    failed = len(validation_results) - passed

                    results_summary["passed_rules"] += passed
                    results_summary["failed_rules"] += failed

                    if failed > 0:
                        results_summary["tables_with_failures"].append({
                            "table_name": table_name,
                            "failed_rules": failed,
                            "total_rules": len(validation_results),
                            "failures": [r for r in validation_results if not r.get("is_valid", False)]
                        })

                    logger.info(f"Completed validations for {table_name}: {passed} passed, {failed} failed")

                except Exception as table_error:
                    logger.error(f"Error validating table {table_name}: {str(table_error)}")
                    results_summary["errors"].append({
                        "table_name": table_name,
                        "error": str(table_error)
                    })

            # Publish automation events if there were failures
            if results_summary["failed_rules"] > 0:
                self._publish_validation_failure_event(
                    connection_id=connection_id,
                    organization_id=organization_id,
                    results_summary=results_summary
                )

            logger.info(f"Automated validation completed. Processed {results_summary['tables_processed']} tables, "
                        f"{results_summary['failed_rules']} failures")

            return results_summary

        except Exception as e:
            logger.error(f"Error in automated validation run: {str(e)}")
            return {
                "tables_processed": 0,
                "total_rules": 0,
                "passed_rules": 0,
                "failed_rules": 0,
                "tables_with_failures": [],
                "errors": [{"error": str(e)}]
            }

    def _get_automated_validation_tables(self, connection_id: str, organization_id: str) -> List[str]:
        """Get list of tables that have validation automation enabled"""
        try:
            # Get table-level automation configs
            response = self.supabase_manager.supabase.table("automation_table_configs") \
                .select("table_name") \
                .eq("connection_id", connection_id) \
                .eq("auto_run_validations", True) \
                .execute()

            table_configs = response.data or []

            # If no table-specific configs, check connection-level config
            if not table_configs:
                conn_response = self.supabase_manager.supabase.table("automation_connection_configs") \
                    .select("validation_automation") \
                    .eq("connection_id", connection_id) \
                    .execute()

                if (conn_response.data and
                        conn_response.data[0].get("validation_automation", {}).get("enabled", False)):
                    # Get all tables that have validation rules
                    tables_with_validations = self.validation_manager.get_tables_with_validations(
                        organization_id, connection_id
                    )
                    return tables_with_validations

            return [config["table_name"] for config in table_configs]

        except Exception as e:
            logger.error(f"Error getting automated validation tables: {str(e)}")
            return []

    def _build_connection_string(self, connection: Dict[str, Any]) -> str:
        """Build connection string from connection details"""
        try:
            connection_type = connection.get("connection_type", "").lower()
            details = connection.get("connection_details", {})

            if connection_type == "snowflake":
                username = details.get("username")
                password = details.get("password")
                account = details.get("account")
                database = details.get("database")
                schema = details.get("schema", "PUBLIC")
                warehouse = details.get("warehouse")

                # URL encode password to handle special characters
                import urllib.parse
                encoded_password = urllib.parse.quote_plus(password)

                return f"snowflake://{username}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}"

            # Add other connection types as needed
            else:
                raise Exception(f"Unsupported connection type: {connection_type}")

        except Exception as e:
            logger.error(f"Error building connection string: {str(e)}")
            raise

    def _publish_validation_failure_event(self, connection_id: str, organization_id: str,
                                          results_summary: Dict[str, Any]):
        """Publish automation event for validation failures"""
        try:
            from core.automation.events import AutomationEventType, publish_automation_event

            publish_automation_event(
                event_type=AutomationEventType.VALIDATION_FAILURES_DETECTED,
                data={
                    "connection_id": connection_id,
                    "failed_rules": results_summary["failed_rules"],
                    "tables_with_failures": len(results_summary["tables_with_failures"]),
                    "tables": results_summary["tables_with_failures"]
                },
                connection_id=connection_id,
                organization_id=organization_id
            )

            logger.info("Published validation failure automation event")

        except ImportError:
            logger.warning("Automation events not available")
        except Exception as e:
            logger.error(f"Error publishing validation failure event: {str(e)}")

    def generate_default_validations_for_table(self, connection_id: str, organization_id: str, table_name: str) -> Dict[
        str, Any]:
        """
        Generate default validations for a new table (called by automation when new tables are detected)

        Args:
            connection_id: Database connection ID
            organization_id: Organization ID
            table_name: Name of the table

        Returns:
            Dictionary with generation results
        """
        try:
            logger.info(f"Generating default validations for new table: {table_name}")

            # Get connection details
            connection = self.supabase_manager.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection not found: {connection_id}")

            # Build connection string
            connection_string = self._build_connection_string(connection)

            # Import and use the default validation generator
            from core.validations.default_validations import add_default_validations

            # Generate default validations
            result = add_default_validations(
                validation_manager=self.validation_manager,
                connection_string=connection_string,
                table_name=table_name,
                connection_id=connection_id
            )

            logger.info(f"Generated {result.get('added', 0)} default validations for table {table_name}")

            return result

        except Exception as e:
            logger.error(f"Error generating default validations for table {table_name}: {str(e)}")
            return {"added": 0, "skipped": 0, "total": 0, "error": str(e)}

    def check_validation_automation_config(self, connection_id: str) -> Dict[str, Any]:
        """
        Check if validation automation is properly configured for a connection

        Args:
            connection_id: Database connection ID

        Returns:
            Dictionary with configuration status
        """
        try:
            # Check connection-level automation config
            conn_response = self.supabase_manager.supabase.table("automation_connection_configs") \
                .select("validation_automation") \
                .eq("connection_id", connection_id) \
                .execute()

            conn_config = conn_response.data[0] if conn_response.data else {}
            validation_automation = conn_config.get("validation_automation", {})

            # Check table-level automation configs
            table_response = self.supabase_manager.supabase.table("automation_table_configs") \
                .select("table_name, auto_run_validations") \
                .eq("connection_id", connection_id) \
                .eq("auto_run_validations", True) \
                .execute()

            automated_tables = table_response.data or []

            # Check if there are any validation rules
            rules_response = self.supabase_manager.supabase.table("validation_rules") \
                .select("table_name", count="exact") \
                .eq("connection_id", connection_id) \
                .execute()

            total_rules = rules_response.count or 0

            return {
                "connection_automation_enabled": validation_automation.get("enabled", False),
                "connection_interval_hours": validation_automation.get("interval_hours", 12),
                "auto_generate_for_new_tables": validation_automation.get("auto_generate_for_new_tables", True),
                "table_level_automation_count": len(automated_tables),
                "automated_tables": [t["table_name"] for t in automated_tables],
                "total_validation_rules": total_rules,
                "properly_configured": (
                                               validation_automation.get("enabled", False) or len(automated_tables) > 0
                                       ) and total_rules > 0
            }

        except Exception as e:
            logger.error(f"Error checking validation automation config: {str(e)}")
            return {
                "properly_configured": False,
                "error": str(e)
            }


# Factory function to create integrator instance
def create_validation_automation_integrator():
    """Create and return a validation automation integrator instance"""
    try:
        from core.validations.supabase_validation_manager import SupabaseValidationManager
        from core.storage.supabase_manager import SupabaseManager

        validation_manager = SupabaseValidationManager()
        supabase_manager = SupabaseManager()

        return ValidationAutomationIntegrator(validation_manager, supabase_manager)

    except Exception as e:
        logger.error(f"Error creating validation automation integrator: {str(e)}")
        raise


# Usage example for automation scheduler:
"""
# In automation scheduler when executing validation_run job:

def _execute_validation_run(self, job_id: str, connection_id: str, config: Dict[str, Any]):
    try:
        # Create integrator
        integrator = create_validation_automation_integrator()

        # Get organization ID from connection
        connection = self.supabase.get_connection(connection_id)
        organization_id = connection.get("organization_id")

        # Run automated validations
        results = integrator.run_automated_validations(connection_id, organization_id)

        # Update job status
        self._update_job_status(job_id, "completed", result_summary=results)

        return results

    except Exception as e:
        logger.error(f"Error in validation automation job: {str(e)}")
        self._update_job_status(job_id, "failed", error_message=str(e))
        return {"error": str(e)}
"""