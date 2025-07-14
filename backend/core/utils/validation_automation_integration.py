import logging
import json
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class ValidationAutomationIntegrator:
    """FIXED: Integrates validation system with automation for automated validation runs"""

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
        FIXED: Run automated validations for all tables with validation rules

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

            # FIXED: Get ALL tables that have validation rules (not just automation-enabled ones)
            tables_with_rules = self._get_tables_with_validation_rules(connection_id, organization_id)

            logger.info(f"Found {len(tables_with_rules)} tables with validation rules")

            results_summary = {
                "tables_processed": 0,
                "total_rules": 0,
                "passed_rules": 0,
                "failed_rules": 0,
                "tables_with_failures": [],
                "errors": [],
                "execution_details": []
            }

            if not tables_with_rules:
                logger.info("No tables found with validation rules - creating default validations")

                # Try to get some tables and create default validations
                self._create_default_validations_if_needed(connection_id, organization_id, connection_string)

                # Try again after creating defaults
                tables_with_rules = self._get_tables_with_validation_rules(connection_id, organization_id)
                logger.info(f"After creating defaults, found {len(tables_with_rules)} tables with rules")

            for table_name in tables_with_rules:
                try:
                    logger.info(f"Running validations for table: {table_name}")

                    # FIXED: Execute validation rules for this table
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

                    execution_detail = {
                        "table_name": table_name,
                        "rules_executed": len(validation_results),
                        "passed": passed,
                        "failed": failed,
                        "success": True
                    }

                    if failed > 0:
                        failure_details = {
                            "table_name": table_name,
                            "failed_rules": failed,
                            "total_rules": len(validation_results),
                            "failures": [r for r in validation_results if not r.get("is_valid", False)]
                        }
                        results_summary["tables_with_failures"].append(failure_details)
                        execution_detail["failure_details"] = failure_details

                    results_summary["execution_details"].append(execution_detail)

                    logger.info(f"Completed validations for {table_name}: {passed} passed, {failed} failed")

                except Exception as table_error:
                    logger.error(f"Error validating table {table_name}: {str(table_error)}")
                    error_detail = {
                        "table_name": table_name,
                        "error": str(table_error)
                    }
                    results_summary["errors"].append(error_detail)
                    results_summary["execution_details"].append({
                        "table_name": table_name,
                        "success": False,
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
                "errors": [{"error": str(e)}],
                "execution_details": []
            }

    def _get_tables_with_validation_rules(self, connection_id: str, organization_id: str) -> List[str]:
        """FIXED: Get list of tables that have ANY validation rules"""
        try:
            # Get all tables that have validation rules for this connection
            response = self.supabase_manager.supabase.table("validation_rules") \
                .select("table_name") \
                .eq("connection_id", connection_id) \
                .eq("organization_id", organization_id) \
                .eq("is_active", True) \
                .execute()

            if not response.data:
                logger.info(f"No validation rules found for connection {connection_id}")
                return []

            # Get unique table names
            tables = list(set(rule["table_name"] for rule in response.data))
            logger.info(f"Found validation rules for tables: {tables}")

            return tables

        except Exception as e:
            logger.error(f"Error getting tables with validation rules: {str(e)}")
            return []

    def _create_default_validations_if_needed(self, connection_id: str, organization_id: str, connection_string: str):
        """Create default validations for tables if none exist"""
        try:
            logger.info("Creating default validations for tables without rules")

            # Get some tables from the connection to create validations for
            from core.metadata.connector_factory import ConnectorFactory

            connector_factory = ConnectorFactory(self.supabase_manager)
            connector = connector_factory.create_connector(connection_id)

            if connector:
                # Get first few tables
                tables = connector.get_tables()
                limited_tables = tables[:3]  # Just do first 3 tables

                logger.info(f"Creating default validations for tables: {limited_tables}")

                for table_name in limited_tables:
                    try:
                        # Import and use the default validation generator
                        from core.validations.default_validations import add_default_validations

                        result = add_default_validations(
                            validation_manager=self.validation_manager,
                            connection_string=connection_string,
                            table_name=table_name,
                            connection_id=connection_id
                        )

                        logger.info(f"Created {result.get('added', 0)} default validations for {table_name}")

                    except Exception as table_error:
                        logger.warning(f"Could not create defaults for {table_name}: {str(table_error)}")

        except Exception as e:
            logger.warning(f"Could not create default validations: {str(e)}")

    def _get_automated_validation_tables(self, connection_id: str, organization_id: str) -> List[str]:
        """DEPRECATED: Get list of tables that have validation automation enabled"""
        # This method is kept for backwards compatibility but we now use _get_tables_with_validation_rules
        return self._get_tables_with_validation_rules(connection_id, organization_id)

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

            # Parse validation_automation if it's a JSON string
            if isinstance(validation_automation, str):
                try:
                    validation_automation = json.loads(validation_automation)
                except json.JSONDecodeError:
                    validation_automation = {}

            # Check if there are any validation rules
            rules_response = self.supabase_manager.supabase.table("validation_rules") \
                .select("table_name", count="exact") \
                .eq("connection_id", connection_id) \
                .execute()

            total_rules = rules_response.count or 0

            # Get list of tables with rules
            tables_response = self.supabase_manager.supabase.table("validation_rules") \
                .select("table_name") \
                .eq("connection_id", connection_id) \
                .execute()

            tables_with_rules = list(set(r["table_name"] for r in (tables_response.data or [])))

            return {
                "connection_automation_enabled": validation_automation.get("enabled", False),
                "connection_interval_hours": validation_automation.get("interval_hours", 12),
                "auto_generate_for_new_tables": validation_automation.get("auto_generate_for_new_tables", True),
                "total_validation_rules": total_rules,
                "tables_with_rules": tables_with_rules,
                "tables_with_rules_count": len(tables_with_rules),
                "properly_configured": total_rules > 0,
                "recommendation": "Create validation rules for tables" if total_rules == 0 else "Configuration looks good"
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