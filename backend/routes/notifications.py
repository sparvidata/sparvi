from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from core.storage.supabase_manager import SupabaseManager
from core.notifications.publisher import NotificationPublisher
import logging

logger = logging.getLogger(__name__)

notifications_bp = Blueprint('notifications', __name__)
supabase = SupabaseManager()
notification_publisher = NotificationPublisher()


@notifications_bp.route('/notification-settings', methods=['GET'])
@jwt_required()
def get_notification_settings():
    """Get notification settings for the user's organization"""
    try:
        current_user = get_jwt_identity()

        # Get user's organization
        user_org = supabase.get_user_organization(current_user['user_id'])
        if not user_org:
            return jsonify({"error": "User not associated with an organization"}), 400

        organization_id = user_org['organization_id']

        # Get notification settings
        response = supabase.supabase.table("notification_settings") \
            .select("*") \
            .eq("organization_id", organization_id) \
            .execute()

        if response.data and len(response.data) > 0:
            settings = response.data[0]
            # Don't send sensitive data like passwords to frontend
            if settings.get('email_config') and 'smtp_password' in settings['email_config']:
                settings['email_config']['smtp_password'] = '***'
        else:
            # Return default settings if none exist
            settings = {
                "notify_high_severity": True,
                "notify_medium_severity": True,
                "notify_low_severity": False,
                "email_enabled": False,
                "email_config": {},
                "slack_enabled": False,
                "slack_config": {},
                "webhook_enabled": False,
                "webhook_config": {}
            }

        return jsonify({"data": settings}), 200

    except Exception as e:
        logger.error(f"Error getting notification settings: {str(e)}")
        return jsonify({"error": "Failed to get notification settings"}), 500


@notifications_bp.route('/notification-settings', methods=['POST'])
@jwt_required()
def save_notification_settings():
    """Save notification settings for the user's organization"""
    try:
        current_user = get_jwt_identity()
        data = request.get_json()

        # Get user's organization and check permissions
        user_org = supabase.get_user_organization(current_user['user_id'])
        if not user_org:
            return jsonify({"error": "User not associated with an organization"}), 400

        # Check if user is admin/owner
        if user_org['role'] not in ['admin', 'owner']:
            return jsonify({"error": "Insufficient permissions to modify notification settings"}), 403

        organization_id = user_org['organization_id']

        # Prepare settings data
        settings_data = {
            "organization_id": organization_id,
            "notify_high_severity": data.get("notify_high_severity", True),
            "notify_medium_severity": data.get("notify_medium_severity", True),
            "notify_low_severity": data.get("notify_low_severity", False),
            "email_enabled": data.get("email_enabled", False),
            "email_config": data.get("email_config", {}),
            "slack_enabled": data.get("slack_enabled", False),
            "slack_config": data.get("slack_config", {}),
            "webhook_enabled": data.get("webhook_enabled", False),
            "webhook_config": data.get("webhook_config", {}),
            "updated_by": current_user['user_id']
        }

        # Check if settings already exist
        existing = supabase.supabase.table("notification_settings") \
            .select("id") \
            .eq("organization_id", organization_id) \
            .execute()

        if existing.data and len(existing.data) > 0:
            # Update existing settings
            response = supabase.supabase.table("notification_settings") \
                .update(settings_data) \
                .eq("organization_id", organization_id) \
                .execute()
        else:
            # Create new settings
            settings_data["created_by"] = current_user['user_id']
            response = supabase.supabase.table("notification_settings") \
                .insert(settings_data) \
                .execute()

        if not response.data:
            return jsonify({"error": "Failed to save notification settings"}), 500

        return jsonify({"message": "Notification settings saved successfully"}), 200

    except Exception as e:
        logger.error(f"Error saving notification settings: {str(e)}")
        return jsonify({"error": "Failed to save notification settings"}), 500


@notifications_bp.route('/notification-settings/test', methods=['POST'])
@jwt_required()
def test_notification():
    """Send a test notification"""
    try:
        current_user = get_jwt_identity()
        data = request.get_json()
        notification_type = data.get('type')

        if notification_type not in ['email', 'slack', 'webhook']:
            return jsonify({"error": "Invalid notification type"}), 400

        # Get user's organization
        user_org = supabase.get_user_organization(current_user['user_id'])
        if not user_org:
            return jsonify({"error": "User not associated with an organization"}), 400

        organization_id = user_org['organization_id']

        # Create test payload
        test_payload = {
            "type": "anomaly_detected",
            "organization_id": organization_id,
            "timestamp": "2024-01-01T12:00:00Z",
            "data": {
                "table_name": "test_table",
                "column_name": "test_column",
                "metric_name": "row_count",
                "anomaly_count": 3,
                "high_severity_count": 1,
                "medium_severity_count": 2,
                "low_severity_count": 0,
                "detection_method": "zscore"
            }
        }

        # Send test notification
        success = notification_publisher.publish_event("anomaly", test_payload)

        if success:
            return jsonify({"message": f"Test {notification_type} notification sent successfully"}), 200
        else:
            return jsonify({"error": f"Failed to send test {notification_type} notification"}), 500

    except Exception as e:
        logger.error(f"Error sending test notification: {str(e)}")
        return jsonify({"error": "Failed to send test notification"}), 500


@notifications_bp.route('/notification-settings/validate', methods=['POST'])
@jwt_required()
def validate_notification_config():
    """Validate notification configuration without saving"""
    try:
        data = request.get_json()
        notification_type = data.get('type')
        config = data.get('config', {})

        errors = []

        if notification_type == 'email':
            required_fields = ['smtp_host', 'smtp_user', 'smtp_password', 'to_emails']
            for field in required_fields:
                if not config.get(field):
                    errors.append(f"Email {field} is required")

            # Validate email format
            if config.get('to_emails'):
                import re
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                for email in config['to_emails']:
                    if not re.match(email_pattern, email.strip()):
                        errors.append(f"Invalid email format: {email}")

        elif notification_type == 'slack':
            if not config.get('webhook_url'):
                errors.append("Slack webhook URL is required")
            elif not config['webhook_url'].startswith('https://hooks.slack.com/'):
                errors.append("Invalid Slack webhook URL format")

        elif notification_type == 'webhook':
            if not config.get('url'):
                errors.append("Webhook URL is required")
            elif not config['url'].startswith('http'):
                errors.append("Webhook URL must start with http:// or https://")

        if errors:
            return jsonify({"valid": False, "errors": errors}), 400
        else:
            return jsonify({"valid": True, "message": "Configuration is valid"}), 200

    except Exception as e:
        logger.error(f"Error validating notification config: {str(e)}")
        return jsonify({"error": "Failed to validate configuration"}), 500