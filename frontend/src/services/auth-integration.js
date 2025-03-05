import { supabase } from '../lib/supabase';

/**
 * Creates profile and organization for a newly registered user
 *
 * @param {string} userId - The ID of the user
 * @param {object} userData - User data including first_name, last_name, etc.
 * @returns {Promise<{ success: boolean, error: Error|null }>}
 */
export async function createUserProfile(userId, userData) {
  console.log("Creating profile for user:", userId);

  try {
    // Check if profile already exists
    const { data: existingProfile } = await supabase
      .from('profiles')
      .select('*')
      .eq('id', userId)
      .single();

    if (existingProfile) {
      console.log("Profile already exists:", existingProfile);
      // If profile exists but doesn't have an organization, create one
      if (!existingProfile.organization_id) {
        return await createAndLinkOrganization(userId, userData);
      }
      return { success: true, error: null };
    }

    // Create new organization
    const orgName = userData.organization_name ||
      `${userData.first_name || userData.email.split('@')[0]}'s Organization`;

    console.log("Creating organization:", orgName);

    const { data: orgData, error: orgError } = await supabase
      .from('organizations')
      .insert([{ name: orgName }])
      .select();

    if (orgError) {
      console.error("Failed to create organization:", orgError);
      return { success: false, error: orgError };
    }

    if (!orgData || orgData.length === 0) {
      console.error("Organization was created but no data returned");
      return { success: false, error: new Error("Failed to create organization") };
    }

    console.log("Organization created successfully:", orgData[0]);

    // Create profile and link to organization
    const { data: profileData, error: profileError } = await supabase
      .from('profiles')
      .insert([{
        id: userId,
        email: userData.email,
        first_name: userData.first_name || "",
        last_name: userData.last_name || "",
        organization_id: orgData[0].id,
        role: 'admin'
      }])
      .select();

    if (profileError) {
      console.error("Failed to create profile:", profileError);
      return { success: false, error: profileError };
    }

    console.log("Profile created successfully:", profileData);
    return { success: true, error: null };

  } catch (error) {
    console.error("Error in createUserProfile:", error.message);
    return { success: false, error };
  }
}

/**
 * Creates an organization and links it to an existing user profile
 */
async function createAndLinkOrganization(userId, userData) {
  try {
    const orgName = userData.organization_name ||
      `${userData.first_name || userData.email.split('@')[0]}'s Organization`;

    const { data: orgData, error: orgError } = await supabase
      .from('organizations')
      .insert([{ name: orgName }])
      .select();

    if (orgError) {
      console.error("Failed to create organization:", orgError);
      return { success: false, error: orgError };
    }

    // Update the user's profile with the organization ID
    const { error: updateError } = await supabase
      .from('profiles')
      .update({ organization_id: orgData[0].id })
      .eq('id', userId);

    if (updateError) {
      console.error("Failed to update profile with organization:", updateError);
      return { success: false, error: updateError };
    }

    return { success: true, error: null };
  } catch (error) {
    console.error("Error in createAndLinkOrganization:", error.message);
    return { success: false, error };
  }
}