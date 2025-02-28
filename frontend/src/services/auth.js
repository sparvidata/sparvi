import { supabase } from '../lib/supabase';

// Sign up a new user
export async function signUp(email, password) {
  try {
    const { data, error } = await supabase.auth.signUp({
      email,
      password,
    });

    if (error) throw error;

    // Create initial profile and organization upon signup
    if (data?.user) {
      // Create organization
      const { data: orgData, error: orgError } = await supabase
        .from('organizations')
        .insert([{ name: email.split('@')[0] + "'s Organization" }])
        .select();

      if (orgError) throw orgError;

      // Create profile
      const { error: profileError } = await supabase
        .from('profiles')
        .insert([{
          id: data.user.id,
          email: email,
          organization_id: orgData[0].id,
          role: 'admin'
        }]);

      if (profileError) throw profileError;
    }

    return { data, error: null };
  } catch (error) {
    return { data: null, error };
  }
}

// Sign in an existing user
export async function signIn(email, password) {
  try {
    const { data, error } = await supabase.auth.signInWithPassword({
      email,
      password,
    });

    if (error) throw error;
    return { data, error: null };
  } catch (error) {
    return { data: null, error };
  }
}

// Sign out
export async function signOut() {
  const { error } = await supabase.auth.signOut();
  return { error };
}