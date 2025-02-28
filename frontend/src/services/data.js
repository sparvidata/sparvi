import { supabase } from '../lib/supabase';

// Get current user's profile
export async function getCurrentProfile() {
  try {
    const { data: { user } } = await supabase.auth.getUser();

    if (!user) throw new Error('Not authenticated');

    const { data, error } = await supabase
      .from('profiles')
      .select(`
        id,
        email,
        first_name,
        last_name,
        role,
        organizations (
          id,
          name
        )
      `)
      .eq('id', user.id)
      .single();

    if (error) throw error;
    return { profile: data, error: null };
  } catch (error) {
    return { profile: null, error };
  }
}

// Save profiling data
export async function saveProfilingData(organizationId, connectionString, tableName, data) {
  try {
    const { data: result, error } = await supabase
      .from('profiling_history')
      .insert([{
        organization_id: organizationId,
        connection_string: connectionString,
        table_name: tableName,
        data: data
      }])
      .select();

    if (error) throw error;
    return { data: result, error: null };
  } catch (error) {
    return { data: null, error };
  }
}

// Get profiling data history for a table
export async function getProfilingHistory(organizationId, tableName, limit = 10) {
  try {
    const { data, error } = await supabase
      .from('profiling_history')
      .select('*')
      .eq('organization_id', organizationId)
      .eq('table_name', tableName)
      .order('collected_at', { ascending: false })
      .limit(limit);

    if (error) throw error;
    return { data, error: null };
  } catch (error) {
    return { data: null, error };
  }
}

// Get validation rules for a table
export async function getValidationRules(organizationId, tableName) {
  try {
    const { data, error } = await supabase
      .from('validation_rules')
      .select('*')
      .eq('organization_id', organizationId)
      .eq('table_name', tableName);

    if (error) throw error;
    return { data, error: null };
  } catch (error) {
    return { data: null, error };
  }
}

// Add a validation rule
export async function addValidationRule(organizationId, tableName, rule) {
  try {
    const { data, error } = await supabase
      .from('validation_rules')
      .insert([{
        organization_id: organizationId,
        table_name: tableName,
        rule_name: rule.name,
        description: rule.description,
        query: rule.query,
        operator: rule.operator,
        expected_value: JSON.stringify(rule.expected_value)
      }])
      .select();

    if (error) throw error;
    return { data, error: null };
  } catch (error) {
    return { data: null, error };
  }
}

// Save validation result
export async function saveValidationResult(organizationId, ruleId, isValid, actualValue) {
  try {
    const { data, error } = await supabase
      .from('validation_results')
      .insert([{
        organization_id: organizationId,
        rule_id: ruleId,
        is_valid: isValid,
        actual_value: JSON.stringify(actualValue)
      }])
      .select();

    if (error) throw error;
    return { data, error: null };
  } catch (error) {
    return { data: null, error };
  }
}