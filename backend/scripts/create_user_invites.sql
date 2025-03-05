-- Create table for user invites
CREATE TABLE user_invites (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  organization_id UUID REFERENCES organizations(id) NOT NULL,
  email TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'member',
  first_name TEXT,
  last_name TEXT,
  invite_token TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  accepted_at TIMESTAMP WITH TIME ZONE,
  accepted_by UUID REFERENCES auth.users(id)
);

-- Create index for faster lookups by token
CREATE INDEX idx_user_invites_token ON user_invites(invite_token);

-- Create index for organization lookups
CREATE INDEX idx_user_invites_org ON user_invites(organization_id);

-- Enable Row Level Security
ALTER TABLE user_invites ENABLE ROW LEVEL SECURITY;

-- Create policy for users to view invites in their organization
CREATE POLICY "Users can view invites in their organization"
  ON user_invites FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid()
    )
  );

-- Create policy for admins to manage invites in their organization
CREATE POLICY "Admins can manage invites in their organization"
  ON user_invites FOR ALL
  USING (
    organization_id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid() AND profiles.role = 'admin'
    )
  );

-- Add an updated_at timestamp trigger to the users_invites table
CREATE TRIGGER update_user_invites_updated_at
  BEFORE UPDATE ON user_invites
  FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();