-- Create schema for organization management
CREATE TABLE organizations (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create profiles table linked to auth.users
CREATE TABLE profiles (
  id UUID REFERENCES auth.users PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  first_name TEXT,
  last_name TEXT,
  organization_id UUID REFERENCES organizations(id),
  role TEXT NOT NULL DEFAULT 'member',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create table for profiling data history
CREATE TABLE profiling_history (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  organization_id UUID REFERENCES organizations(id) NOT NULL,
  profile_id UUID REFERENCES profiles(id),
  connection_string TEXT,
  table_name TEXT NOT NULL,
  data JSONB NOT NULL,
  collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create table for validation rules
CREATE TABLE validation_rules (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  organization_id UUID REFERENCES organizations(id) NOT NULL,
  table_name TEXT NOT NULL,
  rule_name TEXT NOT NULL,
  description TEXT,
  query TEXT NOT NULL,
  operator TEXT NOT NULL,
  expected_value TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(organization_id, table_name, rule_name)
);

-- Create table for validation results
CREATE TABLE validation_results (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  rule_id UUID REFERENCES validation_rules(id) NOT NULL,
  organization_id UUID REFERENCES organizations(id) NOT NULL,
  is_valid BOOLEAN NOT NULL,
  actual_value TEXT,
  run_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

create table public.connection_metadata (
  id uuid not null default extensions.uuid_generate_v4 (),
  connection_id uuid not null,
  metadata_type character varying(50) not null,
  metadata jsonb not null default '{}'::jsonb,
  collected_at timestamp with time zone null default now(),
  refresh_frequency interval null default '1 day'::interval,
  constraint connection_metadata_pkey primary key (id),
  constraint connection_metadata_connection_id_metadata_type_key unique (connection_id, metadata_type),
  constraint connection_metadata_connection_id_fkey foreign KEY (connection_id) references database_connections (id)
) TABLESPACE pg_default;

create table public.schema_changes (
  id uuid not null default extensions.uuid_generate_v4 (),
  connection_id uuid not null,
  organization_id uuid not null,
  table_name text not null,
  column_name text null,
  change_type text not null,
  details jsonb null,
  detected_at timestamp with time zone null default now(),
  acknowledged boolean null default false,
  acknowledged_at timestamp with time zone null,
  acknowledged_by uuid null,
  baseline_metadata_id uuid null,
  constraint schema_changes_pkey primary key (id),
  constraint schema_changes_acknowledged_by_fkey foreign KEY (acknowledged_by) references profiles (id) on delete set null,
  constraint schema_changes_baseline_metadata_id_fkey foreign KEY (baseline_metadata_id) references connection_metadata (id) on delete set null,
  constraint schema_changes_connection_id_fkey foreign KEY (connection_id) references database_connections (id)
) TABLESPACE pg_default;


-- Add indexes for performance
CREATE INDEX idx_profiling_history_org ON profiling_history(organization_id);
CREATE INDEX idx_profiling_history_collected_at ON profiling_history(collected_at);
CREATE INDEX idx_validation_rules_org_table ON validation_rules(organization_id, table_name);
CREATE INDEX idx_validation_results_rule ON validation_results(rule_id);
create index IF not exists idx_connection_metadata_collected_at on public.connection_metadata using btree (collected_at desc) TABLESPACE pg_default;
create index IF not exists idx_connection_metadata_conn_type on public.connection_metadata using btree (connection_id, metadata_type) TABLESPACE pg_default;
create index IF not exists idx_schema_changes_connection_id on public.schema_changes using btree (connection_id) TABLESPACE pg_default;
create index IF not exists idx_schema_changes_baseline_metadata_id on public.schema_changes using btree (baseline_metadata_id) TABLESPACE pg_default;
create index IF not exists idx_schema_changes_duplicate_check on public.schema_changes using btree (
  connection_id,
  organization_id,
  baseline_metadata_id,
  table_name,
  column_name,
  change_type
) TABLESPACE pg_default;
create index IF not exists idx_schema_changes_organization_id on public.schema_changes using btree (organization_id) TABLESPACE pg_default;
create index IF not exists idx_schema_changes_acknowledged on public.schema_changes using btree (acknowledged) TABLESPACE pg_default;
create index IF not exists idx_schema_changes_detected_at on public.schema_changes using btree (detected_at) TABLESPACE pg_default;
create index IF not exists idx_schema_changes_table_name on public.schema_changes using btree (table_name) TABLESPACE pg_default;
create index IF not exists idx_schema_changes_change_type on public.schema_changes using btree (change_type) TABLESPACE pg_default;

-- Set up Row Level Security (RLS)
ALTER TABLE organizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE profiling_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE validation_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE validation_results ENABLE ROW LEVEL SECURITY;

-- Create policies for organizations
CREATE POLICY "Users can view their own organization"
  ON organizations FOR SELECT
  USING (
    id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid()
    )
  );

CREATE POLICY "Organization admins can update their organization"
  ON organizations FOR UPDATE
  USING (
    id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid() AND profiles.role = 'admin'
    )
  );

-- Create policies for profiles
CREATE POLICY "Users can view profiles in their organization"
  ON profiles FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid()
    )
  );

CREATE POLICY "Users can update their own profile"
  ON profiles FOR UPDATE
  USING (id = auth.uid());

-- Create policies for profiling data
CREATE POLICY "Users can view their organization's profiling data"
  ON profiling_history FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid()
    )
  );

CREATE POLICY "Users can insert profiling data for their organization"
  ON profiling_history FOR INSERT
  WITH CHECK (
    organization_id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid()
    )
  );

-- Create policies for validation rules
CREATE POLICY "Users can view their organization's validation rules"
  ON validation_rules FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid()
    )
  );

CREATE POLICY "Users can manage validation rules for their organization"
  ON validation_rules FOR ALL
  USING (
    organization_id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid()
    )
  );

-- Create policies for validation results
CREATE POLICY "Users can view their organization's validation results"
  ON validation_results FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM profiles
      WHERE profiles.id = auth.uid()
    )
  );

-- Create functions for managing timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_organizations_updated_at
  BEFORE UPDATE ON organizations
  FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_profiles_updated_at
  BEFORE UPDATE ON profiles
  FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();