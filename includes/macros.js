// includes/macros.js
// Shared helper functions for Dataform models

/**
 * Returns a SAFE_CAST expression with a fallback default.
 */
function safe_cast(column, type, default_val = "NULL") {
  return `COALESCE(SAFE_CAST(${column} AS ${type}), ${default_val})`;
}

/**
 * Standard audit columns added to every staging model.
 */
function audit_columns() {
  return `
    CURRENT_TIMESTAMP() AS _transformed_at,
    '${dataform.projectConfig.vars.raw_dataset}' AS _source_dataset
  `;
}

/**
 * Generates a surrogate key from one or more columns using MD5.
 */
function surrogate_key(columns) {
  const concat = columns.map(c => `COALESCE(CAST(${c} AS STRING), '')`).join(`, '-', `);
  return `TO_HEX(MD5(CONCAT(${concat})))`;
}

module.exports = { safe_cast, audit_columns, surrogate_key };
