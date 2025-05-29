-- Database
schema
for multi - tenant notification settings
-- This
table
stores
notification
preferences
per
organization

CREATE
TABLE
notification_settings(
    id
UUID
PRIMARY
KEY
DEFAULT
gen_random_uuid(),
organization_id
UUID
NOT
NULL
REFERENCES
organizations(id)
ON
DELETE
CASCADE,

-- Global
notification
preferences
notify_high_severity
BOOLEAN
DEFAULT
true,
notify_medium_severity
BOOLEAN
DEFAULT
true,
notify_low_severity
BOOLEAN
DEFAULT
false,

-- Email
settings
email_enabled
BOOLEAN
DEFAULT
false,
email_config
JSONB
DEFAULT
'{}'::jsonb,
-- email_config
structure:
-- {
    --   "smtp_host": "smtp.gmail.com",
    --   "smtp_port": 587,
    --   "smtp_user": "alerts@company.com",
    --   "smtp_password": "app-password",
    --   "from_email": "alerts@company.com",
    --   "to_emails": ["admin@company.com", "devops@company.com"],
    --   "use_tls": true
                    - -}

- - Slack
settings
slack_enabled
BOOLEAN
DEFAULT
false,
slack_config
JSONB
DEFAULT
'{}'::jsonb,
-- slack_config
structure:
-- {
    --   "webhook_url": "https://hooks.slack.com/services/...",
    --   "channel": "#alerts",
    --   "username": "Sparvi Bot"
                     - -}

- - Webhook
settings
webhook_enabled
BOOLEAN
DEFAULT
false,
webhook_config
JSONB
DEFAULT
'{}'::jsonb,
-- webhook_config
structure:
-- {
    --   "url": "https://client-system.com/webhooks/anomaly",
    --   "headers": {
                        --     "Authorization": "Bearer token",
                        --     "X-API-Key": "client-api-key"
                                            - -}
                    - -}

- - Microsoft
Teams
settings(future)
teams_enabled
BOOLEAN
DEFAULT
false,
teams_config
JSONB
DEFAULT
'{}'::jsonb,

-- SMS
settings(future)
sms_enabled
BOOLEAN
DEFAULT
false,
sms_config
JSONB
DEFAULT
'{}'::jsonb,

-- Audit
fields
created_at
TIMESTAMPTZ
DEFAULT
NOW(),
updated_at
TIMESTAMPTZ
DEFAULT
NOW(),
created_by
UUID
REFERENCES
auth.users(id),
updated_by
UUID
REFERENCES
auth.users(id),

-- Ensure
one
settings
record
per
organization
CONSTRAINT
unique_org_notification_settings
UNIQUE(organization_id)
);

-- Create
index
for faster lookups
    CREATE
    INDEX
    idx_notification_settings_org_id
    ON
    notification_settings(organization_id);

-- RLS
policies
ALTER
TABLE
notification_settings
ENABLE
ROW
LEVEL
SECURITY;

-- Users
can
only
see
their
organization
's notification settings
CREATE
POLICY
"Users can view own org notification settings"
ON
notification_settings
FOR
SELECT
USING(
    organization_id
IN(
    SELECT
organization_id
FROM
user_organizations
WHERE
user_id = auth.uid()
)
);

-- Users
can
update
their
organization
's notification settings (if admin)
CREATE
POLICY
"Admins can update org notification settings"
ON
notification_settings
FOR
ALL
USING(
    organization_id
IN(
    SELECT
organization_id
FROM
user_organizations
WHERE
user_id = auth.uid()
AND
role
IN('admin', 'owner')
)
);

-- Sample
data
for testing
    INSERT
    INTO
    notification_settings(organization_id, email_enabled, email_config, slack_enabled, slack_config)
VALUES
(
    'your-org-id-here',
    true,
    '{
    "smtp_host": "smtp.gmail.com",
"smtp_port": 587,
"smtp_user": "alerts@yourcompany.com",
"smtp_password": "your-app-password",
"from_email": "Sparvi Alerts <alerts@yourcompany.com>",
"to_emails":["admin@yourcompany.com", "devops@yourcompany.com"],
"use_tls": true
}'::jsonb,
true,
'{
"webhook_url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
"channel": "#data-alerts",
"username": "Sparvi Bot"
}'::jsonb
);