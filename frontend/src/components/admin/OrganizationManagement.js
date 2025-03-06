// frontend/src/components/admin/OrganizationManagement.js
import React, { useState, useEffect } from 'react';
import { fetchOrganization, updateOrganization } from '../../api';

function OrganizationManagement() {
 const [organization, setOrganization] = useState(null);
 const [loading, setLoading] = useState(true);
 const [saving, setSaving] = useState(false);
 const [error, setError] = useState(null);
 const [success, setSuccess] = useState(null);
 const [editMode, setEditMode] = useState(false);

 // Form state
 const [formData, setFormData] = useState({
   name: '',
   logo_url: ''
 });

 // Preview settings state
 const [previewSettings, setPreviewSettings] = useState({
   enable_previews: true,
   max_preview_rows: 50,
   restricted_preview_columns: {}
 });

 // Table and column management for restrictions
 const [selectedTable, setSelectedTable] = useState('');
 const [newColumnName, setNewColumnName] = useState('');
 const [knownTables, setKnownTables] = useState([]);

 // Load organization details on component mount
 useEffect(() => {
   loadOrganization();
 }, []);

 // Function to load/refresh organization details
 const loadOrganization = async () => {
   try {
     setLoading(true);
     setError(null);
     const data = await fetchOrganization();
     setOrganization(data);

     // Initialize form with organization data
     setFormData({
       name: data.name || '',
       logo_url: data.logo_url || ''
     });

     // Initialize preview settings
     if (data.settings && data.settings.preview_settings) {
       setPreviewSettings(data.settings.preview_settings);
     }

     // Extract known tables from restricted columns
     if (data.settings && data.settings.preview_settings && data.settings.preview_settings.restricted_preview_columns) {
       setKnownTables(Object.keys(data.settings.preview_settings.restricted_preview_columns));
     }
   } catch (err) {
     setError('Failed to load organization details: ' + (err.response?.data?.error || err.message));
   } finally {
     setLoading(false);
   }
 };

 // Handle input changes
 const handleInputChange = (e) => {
   const { name, value } = e.target;
   setFormData({
     ...formData,
     [name]: value
   });
 };

 // Start editing
 const handleStartEdit = () => {
   setEditMode(true);
 };

 // Cancel editing
 const handleCancelEdit = () => {
   setEditMode(false);
   // Reset form data to original values
   setFormData({
     name: organization?.name || '',
     logo_url: organization?.logo_url || ''
   });
 };

 // Submit form
 const handleSubmit = async (e) => {
   e.preventDefault();
   setSuccess(null);
   setError(null);

   try {
     setSaving(true);
     await updateOrganization(formData);
     setSuccess('Organization details updated successfully');

     // Refresh organization data
     await loadOrganization();

     // Exit edit mode
     setEditMode(false);
   } catch (err) {
     setError('Failed to update organization: ' + (err.response?.data?.error || err.message));
   } finally {
     setSaving(false);
   }
 };

 // Toggle data preview setting
 const handleTogglePreview = () => {
   setPreviewSettings({
     ...previewSettings,
     enable_previews: !previewSettings.enable_previews
   });
 };

 // Handle max rows change
 const handleMaxRowsChange = (e) => {
   setPreviewSettings({
     ...previewSettings,
     max_preview_rows: parseInt(e.target.value, 10)
   });
 };

 // Add a restricted column
 const handleAddRestrictedColumn = () => {
   if (!selectedTable || !newColumnName) return;

   const updatedRestrictions = { ...previewSettings.restricted_preview_columns };

   if (!updatedRestrictions[selectedTable]) {
     updatedRestrictions[selectedTable] = [];
   }

   if (!updatedRestrictions[selectedTable].includes(newColumnName)) {
     updatedRestrictions[selectedTable] = [...updatedRestrictions[selectedTable], newColumnName];
   }

   setPreviewSettings({
     ...previewSettings,
     restricted_preview_columns: updatedRestrictions
   });

   // Update known tables if this is a new table
   if (!knownTables.includes(selectedTable)) {
     setKnownTables([...knownTables, selectedTable]);
   }

   // Reset input fields
   setNewColumnName('');
 };

 // Remove a restricted column
 const handleRemoveRestrictedColumn = (tableName, columnName) => {
   const updatedRestrictions = { ...previewSettings.restricted_preview_columns };

   if (updatedRestrictions[tableName]) {
     updatedRestrictions[tableName] = updatedRestrictions[tableName].filter(
       col => col !== columnName
     );

     // Remove table entry if empty
     if (updatedRestrictions[tableName].length === 0) {
       delete updatedRestrictions[tableName];
       setKnownTables(knownTables.filter(t => t !== tableName));
     }
   }

   setPreviewSettings({
     ...previewSettings,
     restricted_preview_columns: updatedRestrictions
   });
 };

 // Save preview settings
 const savePreviewSettings = async () => {
   setSuccess(null);
   setError(null);
   setSaving(true);

   try {
     // Prepare updated organization settings
     const updatedSettings = {
       ...(organization.settings || {}),
       preview_settings: previewSettings
     };

     // Update organization with new settings
     await updateOrganization({
       settings: updatedSettings
     });

     setSuccess('Preview settings updated successfully');

     // Refresh organization data
     await loadOrganization();
   } catch (err) {
     setError('Failed to save preview settings: ' + (err.response?.data?.error || err.message));
   } finally {
     setSaving(false);
   }
 };

 return (
   <div className="card mb-4 shadow-sm">
     <div className="card-header bg-light">
       <h5 className="mb-0">
         <i className="bi bi-building me-2"></i>
         Organization Management
       </h5>
     </div>
     <div className="card-body">
       {error && (
         <div className="alert alert-danger alert-dismissible fade show" role="alert">
           <i className="bi bi-exclamation-triangle-fill me-2"></i>
           {error}
           <button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close" onClick={() => setError(null)}></button>
         </div>
       )}

       {success && (
         <div className="alert alert-success alert-dismissible fade show" role="alert">
           <i className="bi bi-check-circle-fill me-2"></i>
           {success}
           <button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close" onClick={() => setSuccess(null)}></button>
         </div>
       )}

       {loading ? (
         <div className="d-flex justify-content-center my-5">
           <div className="spinner-border text-primary" role="status">
             <span className="visually-hidden">Loading...</span>
           </div>
         </div>
       ) : (
         <>
           {!editMode ? (
             <div className="d-flex flex-column flex-md-row align-items-md-center mb-3">
               <div className="me-md-4 mb-3 mb-md-0">
                 {organization?.logo_url ? (
                   <img
                     src={organization.logo_url}
                     alt={`${organization.name} logo`}
                     className="rounded-circle"
                     style={{ width: '80px', height: '80px', objectFit: 'cover' }}
                   />
                 ) : (
                   <div
                     className="d-flex align-items-center justify-content-center bg-light rounded-circle"
                     style={{ width: '80px', height: '80px' }}
                   >
                     <i className="bi bi-building" style={{ fontSize: '2rem' }}></i>
                   </div>
                 )}
               </div>

               <div className="flex-grow-1">
                 <h2 className="h4 mb-1">{organization?.name || 'Your Organization'}</h2>
                 <p className="text-muted small mb-2">Created: {organization?.created_at ? new Date(organization.created_at).toLocaleDateString() : 'Unknown'}</p>

                 <button
                   className="btn btn-sm btn-outline-primary"
                   onClick={handleStartEdit}
                 >
                   <i className="bi bi-pencil me-1"></i>
                   Edit Details
                 </button>
               </div>
             </div>
           ) : (
             <form onSubmit={handleSubmit}>
               <div className="mb-3">
                 <label htmlFor="name" className="form-label">Organization Name*</label>
                 <input
                   type="text"
                   className="form-control"
                   id="name"
                   name="name"
                   value={formData.name}
                   onChange={handleInputChange}
                   required
                 />
               </div>

               <div className="mb-3">
                 <label htmlFor="logo_url" className="form-label">Logo URL</label>
                 <input
                   type="url"
                   className="form-control"
                   id="logo_url"
                   name="logo_url"
                   value={formData.logo_url}
                   onChange={handleInputChange}
                   placeholder="https://example.com/logo.png"
                 />
                 <div className="form-text">
                   Enter a URL to your organization's logo (optional)
                 </div>
               </div>

               <div className="d-flex">
                 <button type="submit" className="btn btn-primary me-2" disabled={saving}>
                   {saving ? (
                     <>
                       <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                       Saving...
                     </>
                   ) : (
                     <>
                       <i className="bi bi-save me-1"></i>
                       Save Changes
                     </>
                   )}
                 </button>

                 <button
                   type="button"
                   className="btn btn-secondary"
                   onClick={handleCancelEdit}
                   disabled={saving}
                 >
                   Cancel
                 </button>
               </div>
             </form>
           )}

           <hr className="my-4" />

           {/* Data Preview Settings */}
           <div className="card">
             <div className="card-header bg-light">
               <h5 className="mb-0">
                 <i className="bi bi-eye me-2"></i>
                 Data Preview Settings
               </h5>
             </div>
             <div className="card-body">
               <div className="form-check form-switch mb-3">
                 <input
                   className="form-check-input"
                   type="checkbox"
                   id="enablePreviews"
                   checked={previewSettings.enable_previews}
                   onChange={handleTogglePreview}
                 />
                 <label className="form-check-label" htmlFor="enablePreviews">
                   Enable data previews
                 </label>
                 <div className="form-text">
                   When disabled, no row-level data can be viewed by any users
                 </div>
               </div>

               <div className="mb-3">
                 <label htmlFor="maxRows" className="form-label">Maximum preview rows</label>
                 <select
                   className="form-select"
                   id="maxRows"
                   value={previewSettings.max_preview_rows}
                   onChange={handleMaxRowsChange}
                 >
                   <option value="10">10 rows</option>
                   <option value="25">25 rows</option>
                   <option value="50">50 rows</option>
                   <option value="100">100 rows</option>
                 </select>
                 <div className="form-text">
                   Limit the number of rows that can be previewed at once
                 </div>
               </div>

               <div className="mb-3">
                 <label className="form-label">Restricted columns</label>
                 <div className="card">
                   <div className="card-body">
                     <div className="mb-3">
                       <label htmlFor="tableName" className="form-label">Table name</label>
                       <div className="input-group">
                         <input
                           type="text"
                           className="form-control"
                           id="tableName"
                           value={selectedTable}
                           onChange={(e) => setSelectedTable(e.target.value)}
                           list="knownTables"
                           placeholder="Enter table name"
                         />
                         <datalist id="knownTables">
                           {knownTables.map(table => (
                             <option key={table} value={table} />
                           ))}
                         </datalist>
                       </div>
                     </div>
                     <div className="mb-3">
                       <label htmlFor="columnName" className="form-label">Column name</label>
                       <div className="input-group">
                         <input
                           type="text"
                           className="form-control"
                           id="columnName"
                           value={newColumnName}
                           onChange={(e) => setNewColumnName(e.target.value)}
                           placeholder="Enter column name to restrict"
                         />
                         <button
                           type="button"
                           className="btn btn-outline-primary"
                           onClick={handleAddRestrictedColumn}
                           disabled={!selectedTable || !newColumnName}
                         >
                           <i className="bi bi-plus-circle me-1"></i>
                           Add
                         </button>
                       </div>
                       <div className="form-text">
                         Restricted columns will not be visible in data previews
                       </div>
                     </div>

                     {/* Display existing restrictions */}
                     {Object.keys(previewSettings.restricted_preview_columns || {}).length > 0 ? (
                       <div className="mt-3">
                         <h6>Current restrictions</h6>
                         {Object.entries(previewSettings.restricted_preview_columns).map(([table, columns]) => (
                           <div key={table} className="mb-2">
                             <div className="fw-bold">{table}</div>
                             <div>
                               {columns.map(column => (
                                 <span key={`${table}-${column}`} className="badge bg-secondary me-1 mb-1">
                                   {column}
                                   <button
                                     type="button"
                                     className="btn-close btn-close-white ms-1"
                                     style={{ fontSize: '0.5rem' }}
                                     onClick={() => handleRemoveRestrictedColumn(table, column)}
                                     aria-label="Remove"
                                   ></button>
                                 </span>
                               ))}
                             </div>
                           </div>
                         ))}
                       </div>
                     ) : (
                       <div className="alert alert-info">
                         <i className="bi bi-info-circle me-2"></i>
                         No column restrictions defined. All columns will be visible in previews.
                       </div>
                     )}
                   </div>
                 </div>
               </div>

               <div className="alert alert-info mt-3">
                 <i className="bi bi-shield-lock me-2"></i>
                 <strong>Privacy Policy:</strong> Sample data is only generated on-demand and is never stored in our system.
                 We only log when previews are accessed, not what data was viewed.
               </div>

               <div className="d-flex justify-content-end mt-3">
                 <button
                   type="button"
                   className="btn btn-primary"
                   onClick={savePreviewSettings}
                   disabled={saving}
                 >
                   {saving ? (
                     <>
                       <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                       Saving...
                     </>
                   ) : (
                     <>
                       <i className="bi bi-save me-1"></i>
                       Save Preview Settings
                     </>
                   )}
                 </button>
               </div>
             </div>
           </div>

           <div className="row mb-2 mt-4">
             <div className="col-md-6">
               <div className="card border">
                 <div className="card-body">
                   <h5 className="card-title">
                     <i className="bi bi-people me-2"></i>
                     Members
                   </h5>
                   <p className="card-text">
                     Manage users and invitations for your organization
                   </p>
                   <button
                     className="btn btn-sm btn-outline-primary"
                     onClick={() => document.getElementById('userManagementSection').scrollIntoView({ behavior: 'smooth' })}
                   >
                     <i className="bi bi-person-gear me-1"></i>
                     Manage Users
                   </button>
                 </div>
               </div>
             </div>

             <div className="col-md-6 mt-3 mt-md-0">
               <div className="card border">
                 <div className="card-body">
                   <h5 className="card-title">
                     <i className="bi bi-gear me-2"></i>
                     Advanced Settings
                   </h5>
                   <p className="card-text">
                     Configure organization-wide settings and preferences
                   </p>
                   <button
                     className="btn btn-sm btn-outline-primary disabled"
                     title="Coming soon"
                   >
                     <i className="bi bi-sliders me-1"></i>
                     Manage Settings
                   </button>
                 </div>
               </div>
             </div>
           </div>
         </>
       )}
     </div>
   </div>
 );
}

export default OrganizationManagement;