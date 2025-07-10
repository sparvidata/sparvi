import logging
import os
from typing import Dict, List, Any, Tuple

logger = logging.getLogger(__name__)


def validate_automation_environment() -> Tuple[bool, str, Dict[str, Any]]:
    """
    Validate automation environment configuration

    Returns:
        Tuple of (is_valid, message, details)
    """
    try:
        details = {
            "environment": os.getenv("ENVIRONMENT", "development"),
            "automation_enabled": False,
            "scheduler_enabled": False,
            "reasons": [],
            "recommendations": []
        }

        environment = details["environment"]

        # Check if explicitly disabled
        if os.getenv("DISABLE_AUTOMATION", "false").lower() == "true":
            details["reasons"].append("Automation explicitly disabled via DISABLE_AUTOMATION")
            return False, "Automation is explicitly disabled", details

        # Check environment-specific rules
        if environment == "production":
            details["automation_enabled"] = True
            details["scheduler_enabled"] = True
            details["reasons"].append("Production environment - automation enabled by default")
            return True, "Automation enabled for production", details

        elif environment == "development":
            scheduler_enabled = os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true"

            if scheduler_enabled:
                details["automation_enabled"] = True
                details["scheduler_enabled"] = True
                details["reasons"].append("Development environment with ENABLE_AUTOMATION_SCHEDULER=true")
                return True, "Automation enabled for development", details
            else:
                details["reasons"].append("Development environment requires ENABLE_AUTOMATION_SCHEDULER=true")
                details["recommendations"].append("Set ENABLE_AUTOMATION_SCHEDULER=true to enable automation")
                return False, "Automation disabled in development (set ENABLE_AUTOMATION_SCHEDULER=true to enable)", details

        else:
            # Unknown environment
            details["reasons"].append(f"Unknown environment: {environment}")
            details["recommendations"].append("Set ENVIRONMENT=production or ENVIRONMENT=development")
            return False, f"Unknown environment: {environment}", details

    except Exception as e:
        logger.error(f"Error validating automation environment: {str(e)}")
        return False, f"Error validating configuration: {str(e)}", {}


def validate_automation_database_config(supabase_manager) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Validate automation database configuration

    Args:
        supabase_manager: SupabaseManager instance

    Returns:
        Tuple of (is_valid, message, details)
    """
    try:
        details = {
            "supabase_available": False,
            "global_config_exists": False,
            "global_config_enabled": False,
            "required_tables": {},
            "issues": [],
            "recommendations": []
        }

        if not supabase_manager:
            details["issues"].append("Supabase manager not available")
            return False, "Database not available", details

        details["supabase_available"] = True

        # Check if we can access the database
        try:
            # Test basic connectivity
            response = supabase_manager.supabase.table("automation_global_config").select("id").limit(1).execute()
            details["global_config_exists"] = len(response.data or []) > 0

            if details["global_config_exists"]:
                # Check if automation is enabled globally
                config_response = supabase_manager.supabase.table("automation_global_config") \
                    .select("automation_enabled") \
                    .order("created_at", desc=True) \
                    .limit(1) \
                    .execute()

                if config_response.data:
                    details["global_config_enabled"] = config_response.data[0].get("automation_enabled", True)
        except Exception as db_error:
            details["issues"].append(f"Database access error: {str(db_error)}")
            details["recommendations"].append("Check database connectivity and permissions")

        # Check required tables exist
        required_tables = [
            "automation_global_config",
            "automation_connection_configs",
            "automation_jobs",
            "automation_runs",
            "automation_events"
        ]

        for table_name in required_tables:
            try:
                response = supabase_manager.supabase.table(table_name).select("id").limit(1).execute()
                details["required_tables"][table_name] = True
            except Exception:
                details["required_tables"][table_name] = False
                details["issues"].append(f"Required table missing or inaccessible: {table_name}")

        # Determine overall validity
        has_critical_issues = len(details["issues"]) > 0
        missing_tables = sum(1 for exists in details["required_tables"].values() if not exists)

        if has_critical_issues:
            return False, f"Database configuration issues: {len(details['issues'])} problems found", details
        elif missing_tables > 0:
            details["recommendations"].append("Run database migrations to create missing tables")
            return False, f"Missing {missing_tables} required database tables", details
        elif not details["global_config_enabled"]:
            details["recommendations"].append("Enable automation in global configuration")
            return False, "Automation disabled in global configuration", details
        else:
            return True, "Database configuration valid", details

    except Exception as e:
        logger.error(f"Error validating database config: {str(e)}")
        return False, f"Error validating database: {str(e)}", {}


def validate_automation_dependencies() -> Tuple[bool, str, Dict[str, Any]]:
    """
    Validate automation system dependencies

    Returns:
        Tuple of (is_valid, message, details)
    """
    try:
        details = {
            "required_modules": {},
            "optional_modules": {},
            "issues": [],
            "recommendations": []
        }

        # Check required modules
        required_modules = [
            ("core.storage.supabase_manager", "SupabaseManager"),
            ("core.automation.simplified_scheduler", "SimplifiedAutomationScheduler"),
            ("core.automation.schedule_manager", "ScheduleManager"),
            ("core.automation.events", "publish_automation_event")
        ]

        for module_name, class_name in required_modules:
            try:
                module = __import__(module_name, fromlist=[class_name])
                getattr(module, class_name)
                details["required_modules"][f"{module_name}.{class_name}"] = True
            except ImportError as e:
                details["required_modules"][f"{module_name}.{class_name}"] = False
                details["issues"].append(f"Required module missing: {module_name}")
            except AttributeError as e:
                details["required_modules"][f"{module_name}.{class_name}"] = False
                details["issues"].append(f"Required class missing: {class_name} in {module_name}")

        # Check optional modules (for integration)
        optional_modules = [
            ("core.metadata.manager", "MetadataTaskManager"),
            ("core.validations.supabase_validation_manager", "SupabaseValidationManager"),
            ("core.metadata.schema_change_detector", "SchemaChangeDetector")
        ]

        for module_name, class_name in optional_modules:
            try:
                module = __import__(module_name, fromlist=[class_name])
                getattr(module, class_name)
                details["optional_modules"][f"{module_name}.{class_name}"] = True
            except (ImportError, AttributeError):
                details["optional_modules"][f"{module_name}.{class_name}"] = False
                # Don't treat as critical issues

        # Determine validity
        missing_required = sum(1 for available in details["required_modules"].values() if not available)

        if missing_required > 0:
            details["recommendations"].append("Install missing dependencies or check module paths")
            return False, f"Missing {missing_required} required dependencies", details
        else:
            return True, "All required dependencies available", details

    except Exception as e:
        logger.error(f"Error validating dependencies: {str(e)}")
        return False, f"Error validating dependencies: {str(e)}", {}


def comprehensive_automation_validation(supabase_manager=None) -> Dict[str, Any]:
    """
    Perform comprehensive automation configuration validation

    Args:
        supabase_manager: Optional SupabaseManager instance

    Returns:
        Dictionary with comprehensive validation results
    """
    try:
        logger.info("Performing comprehensive automation validation...")

        validation_results = {
            "timestamp": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            "overall_valid": False,
            "can_start_automation": False,
            "validation_checks": {},
            "summary": {
                "passed": 0,
                "failed": 0,
                "total": 0
            },
            "recommendations": [],
            "next_steps": []
        }

        # 1. Validate environment configuration
        env_valid, env_message, env_details = validate_automation_environment()
        validation_results["validation_checks"]["environment"] = {
            "valid": env_valid,
            "message": env_message,
            "details": env_details
        }

        # 2. Validate dependencies
        deps_valid, deps_message, deps_details = validate_automation_dependencies()
        validation_results["validation_checks"]["dependencies"] = {
            "valid": deps_valid,
            "message": deps_message,
            "details": deps_details
        }

        # 3. Validate database configuration (if supabase manager provided)
        if supabase_manager:
            db_valid, db_message, db_details = validate_automation_database_config(supabase_manager)
            validation_results["validation_checks"]["database"] = {
                "valid": db_valid,
                "message": db_message,
                "details": db_details
            }
        else:
            validation_results["validation_checks"]["database"] = {
                "valid": False,
                "message": "Database validation skipped (no supabase manager)",
                "details": {}
            }

        # Calculate summary
        checks = validation_results["validation_checks"]
        validation_results["summary"]["total"] = len(checks)
        validation_results["summary"]["passed"] = sum(1 for check in checks.values() if check["valid"])
        validation_results["summary"]["failed"] = validation_results["summary"]["total"] - \
                                                  validation_results["summary"]["passed"]

        # Determine overall validity
        all_critical_passed = (
                checks["environment"]["valid"] and
                checks["dependencies"]["valid"]
        )

        # Database is required for full functionality but not for basic startup
        database_available = checks.get("database", {}).get("valid", False)

        validation_results["overall_valid"] = all_critical_passed
        validation_results["can_start_automation"] = all_critical_passed and database_available

        # Collect recommendations
        for check_name, check_result in checks.items():
            if not check_result["valid"]:
                details = check_result.get("details", {})
                recommendations = details.get("recommendations", [])
                validation_results["recommendations"].extend(recommendations)

        # Generate next steps
        if not validation_results["can_start_automation"]:
            if not checks["environment"]["valid"]:
                validation_results["next_steps"].append("Fix environment configuration")
            if not checks["dependencies"]["valid"]:
                validation_results["next_steps"].append("Install missing dependencies")
            if not checks.get("database", {}).get("valid", False):
                validation_results["next_steps"].append("Fix database configuration")
        else:
            validation_results["next_steps"].append("Automation is ready to start")

        logger.info(
            f"Validation complete: {validation_results['summary']['passed']}/{validation_results['summary']['total']} checks passed")

        return validation_results

    except Exception as e:
        logger.error(f"Error in comprehensive validation: {str(e)}")
        return {
            "overall_valid": False,
            "can_start_automation": False,
            "error": str(e),
            "message": "Validation failed due to error"
        }


def log_validation_summary(validation_results: Dict[str, Any]):
    """
    Log a summary of validation results

    Args:
        validation_results: Results from comprehensive_automation_validation
    """
    try:
        logger.info("=== Automation Configuration Validation Summary ===")

        summary = validation_results.get("summary", {})
        logger.info(f"Validation Results: {summary.get('passed', 0)}/{summary.get('total', 0)} checks passed")

        if validation_results.get("can_start_automation", False):
            logger.info("✓ Automation system is ready to start")
        else:
            logger.warning("✗ Automation system cannot start - configuration issues detected")

        # Log failed checks
        checks = validation_results.get("validation_checks", {})
        for check_name, check_result in checks.items():
            if check_result.get("valid", False):
                logger.info(f"✓ {check_name}: {check_result.get('message', 'OK')}")
            else:
                logger.warning(f"✗ {check_name}: {check_result.get('message', 'Failed')}")

        # Log recommendations
        recommendations = validation_results.get("recommendations", [])
        if recommendations:
            logger.info("Recommendations:")
            for i, rec in enumerate(recommendations[:5], 1):  # Show first 5
                logger.info(f"  {i}. {rec}")

        # Log next steps
        next_steps = validation_results.get("next_steps", [])
        if next_steps:
            logger.info("Next Steps:")
            for i, step in enumerate(next_steps, 1):
                logger.info(f"  {i}. {step}")

        logger.info("=== End Validation Summary ===")

    except Exception as e:
        logger.error(f"Error logging validation summary: {str(e)}")