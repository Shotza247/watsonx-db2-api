// Standalone DB2 REST API for Credit Applications
// Install dependencies: npm install express ibm_db dotenv cors body-parser

const express = require('express');
const ibmdb = require('ibm_db');
const dotenv = require('dotenv');
const cors = require('cors');
const bodyParser = require('body-parser');

dotenv.config();

const app = express();
const PORT = process.env.PORT || 4000;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// ============================================
// DB2 CONFIGURATION
// ============================================

// DB2 Connection String
const DB2_CONN_STR = `DATABASE=${process.env.DB2_DATABASE};` +
                     `HOSTNAME=${process.env.DB2_HOST};` +
                     `PORT=${process.env.DB2_PORT};` +
                     `PROTOCOL=TCPIP;` +
                     `UID=${process.env.DB2_USERNAME};` +
                     `PWD=${process.env.DB2_PASSWORD};` +
                     `Security=SSL;`;

const DB2_SCHEMA = process.env.DB2_SCHEMA || 'kwc18410';
const DB2_TABLE = 'CREDITAPPLICATIONS';

// ============================================
// DB2 HELPER FUNCTIONS
// ============================================

// Execute DB2 query with connection pooling
async function executeDB2Query(sql, params = []) {
    return new Promise((resolve, reject) => {
        ibmdb.open(DB2_CONN_STR, (err, conn) => {
            if (err) {
                console.error('DB2 Connection Error:', err);
                return reject(err);
            }
            
            console.log('Executing query:', sql.substring(0, 100) + '...');
            
            conn.query(sql, params, (err, data) => {
                conn.close(() => {
                    if (err) {
                        console.error('Query Error:', err);
                        return reject(err);
                    }
                    console.log('Query successful. Rows returned:', data.length);
                    resolve(data);
                });
            });
        });
    });
}

// ============================================
// HEALTH & CONNECTION ENDPOINTS
// ============================================

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'OK',
        timestamp: new Date().toISOString(),
        service: 'DB2 Credit Applications API',
        version: '1.0.0'
    });
});

// Test DB2 connection
app.get('/api/db2/test', async (req, res) => {
    try {
        const result = await executeDB2Query('SELECT 1 AS TEST FROM SYSIBM.SYSDUMMY1');
        res.json({
            success: true,
            message: 'DB2 connection successful',
            timestamp: new Date().toISOString(),
            database: process.env.DB2_DATABASE,
            schema: DB2_SCHEMA,
            result: result
        });
    } catch (error) {
        console.error('DB2 connection error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            hint: 'Check your DB2 credentials in .env file',
            connection_string: DB2_CONN_STR.replace(/PWD=.*?;/, 'PWD=***;')
        });
    }
});

// Get database info
app.get('/api/db2/info', async (req, res) => {
    try {
        const countSql = `SELECT COUNT(*) as TOTAL FROM ${DB2_SCHEMA}.${DB2_TABLE}`;
        const countResult = await executeDB2Query(countSql);
        
        res.json({
            success: true,
            database: process.env.DB2_DATABASE,
            schema: DB2_SCHEMA,
            table: DB2_TABLE,
            total_records: countResult[0].TOTAL
        });
    } catch (error) {
        console.error('Error getting database info:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ============================================
// READ ENDPOINTS
// ============================================

// Get all credit applications with filtering
app.get('/api/applications', async (req, res) => {
    try {
        const { 
            status, 
            customer_id, 
            product_code,
            min_amount,
            max_amount,
            limit = 100,
            offset = 0 
        } = req.query;
        
        let sql = `SELECT * FROM ${DB2_SCHEMA}.${DB2_TABLE}`;
        let params = [];
        let whereClauses = [];
        
        // Build WHERE clause dynamically
        if (status) {
            whereClauses.push('APP_STATUS = ?');
            params.push(status.toUpperCase());
        }
        
        if (customer_id) {
            whereClauses.push('CIS_CUSTOMER_NUMBER = ?');
            params.push(customer_id.toUpperCase());
        }
        
        if (product_code) {
            whereClauses.push('REQ_PRODUCT_CODE = ?');
            params.push(product_code.toUpperCase());
        }
        
        if (min_amount) {
            whereClauses.push('REQ_AMOUNT >= ?');
            params.push(parseFloat(min_amount));
        }
        
        if (max_amount) {
            whereClauses.push('REQ_AMOUNT <= ?');
            params.push(parseFloat(max_amount));
        }
        
        if (whereClauses.length > 0) {
            sql += ' WHERE ' + whereClauses.join(' AND ');
        }
        
        sql += ' ORDER BY APP_SUBMITTED_AT DESC';
        sql += ` OFFSET ${parseInt(offset)} ROWS FETCH FIRST ${parseInt(limit)} ROWS ONLY`;
        
        const results = await executeDB2Query(sql, params);
        
        res.json({
            success: true,
            count: results.length,
            filters: { status, customer_id, product_code, min_amount, max_amount },
            pagination: { limit: parseInt(limit), offset: parseInt(offset) },
            data: results
        });
    } catch (error) {
        console.error('Error fetching applications:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get single application by APP_REF
app.get('/api/application/:app_ref', async (req, res) => {
    try {
        const { app_ref } = req.params;
        
        const sql = `SELECT * FROM ${DB2_SCHEMA}.${DB2_TABLE} WHERE APP_REF = ?`;
        const results = await executeDB2Query(sql, [app_ref]);
        
        if (results.length === 0) {
            return res.status(404).json({
                success: false,
                error: 'Application not found',
                app_ref: app_ref
            });
        }
        
        res.json({
            success: true,
            data: results[0]
        });
    } catch (error) {
        console.error('Error fetching application:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get all applications for a specific customer
app.get('/api/customer/:cis_number/applications', async (req, res) => {
    try {
        const { cis_number } = req.params;
        
        const sql = `SELECT * FROM ${DB2_SCHEMA}.${DB2_TABLE} 
                     WHERE CIS_CUSTOMER_NUMBER = ? 
                     ORDER BY APP_SUBMITTED_AT DESC`;
        const results = await executeDB2Query(sql, [cis_number.toUpperCase()]);
        
        res.json({
            success: true,
            customer: cis_number.toUpperCase(),
            count: results.length,
            applications: results
        });
    } catch (error) {
        console.error('Error fetching customer applications:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get customer summary
app.get('/api/customer/:cis_number/summary', async (req, res) => {
    try {
        const { cis_number } = req.params;
        
        const sql = `
            SELECT 
                CIS_CUSTOMER_NUMBER,
                CUST_FIRST_NAME,
                CUST_LAST_NAME,
                CUST_EMAIL,
                CUST_PHONE,
                COUNT(*) as TOTAL_APPLICATIONS,
                SUM(CASE WHEN APP_STATUS = 'APPROVED' THEN 1 ELSE 0 END) as APPROVED_COUNT,
                SUM(CASE WHEN APP_STATUS = 'REJECTED' THEN 1 ELSE 0 END) as REJECTED_COUNT,
                SUM(CASE WHEN APP_STATUS = 'IN_REVIEW' THEN 1 ELSE 0 END) as IN_REVIEW_COUNT,
                SUM(REQ_AMOUNT) as TOTAL_REQUESTED,
                AVG(REQ_AMOUNT) as AVG_REQUESTED
            FROM ${DB2_SCHEMA}.${DB2_TABLE}
            WHERE CIS_CUSTOMER_NUMBER = ?
            GROUP BY CIS_CUSTOMER_NUMBER, CUST_FIRST_NAME, CUST_LAST_NAME, 
                     CUST_EMAIL, CUST_PHONE
        `;
        
        const results = await executeDB2Query(sql, [cis_number.toUpperCase()]);
        
        if (results.length === 0) {
            return res.status(404).json({
                success: false,
                error: 'Customer not found',
                cis_number: cis_number
            });
        }
        
        res.json({
            success: true,
            customer_summary: results[0]
        });
    } catch (error) {
        console.error('Error fetching customer summary:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Search applications
app.get('/api/search', async (req, res) => {
    try {
        const { query } = req.query;
        
        if (!query || query.length < 2) {
            return res.status(400).json({
                success: false,
                error: 'Search query must be at least 2 characters'
            });
        }
        
        const sql = `
            SELECT * FROM ${DB2_SCHEMA}.${DB2_TABLE}
            WHERE 
                UPPER(CUST_FIRST_NAME) LIKE ? OR
                UPPER(CUST_LAST_NAME) LIKE ? OR
                UPPER(CUST_EMAIL) LIKE ? OR
                UPPER(CIS_CUSTOMER_NUMBER) LIKE ? OR
                UPPER(APP_REF) LIKE ?
            ORDER BY APP_SUBMITTED_AT DESC
            FETCH FIRST 50 ROWS ONLY
        `;
        
        const searchPattern = `%${query.toUpperCase()}%`;
        const results = await executeDB2Query(sql, [
            searchPattern, searchPattern, searchPattern, searchPattern, searchPattern
        ]);
        
        res.json({
            success: true,
            query: query,
            count: results.length,
            results: results
        });
    } catch (error) {
        console.error('Error searching applications:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ============================================
// WRITE ENDPOINTS
// ============================================

// Create new credit application
app.post('/api/application', async (req, res) => {
    try {
        const app = req.body;
        
        // Validate required fields
        const requiredFields = ['APP_REF', 'APP_STATUS', 'CIS_CUSTOMER_NUMBER'];
        for (let field of requiredFields) {
            if (!app[field]) {
                return res.status(400).json({
                    success: false,
                    error: `Missing required field: ${field}`
                });
            }
        }
        
        // Check if APP_REF already exists
        const checkSql = `SELECT APP_REF FROM ${DB2_SCHEMA}.${DB2_TABLE} WHERE APP_REF = ?`;
        const existing = await executeDB2Query(checkSql, [app.APP_REF]);
        
        if (existing.length > 0) {
            return res.status(409).json({
                success: false,
                error: 'Application reference already exists',
                app_ref: app.APP_REF
            });
        }
        
        const sql = `INSERT INTO ${DB2_SCHEMA}.${DB2_TABLE} (
            APP_REF, APP_SUBMITTED_AT, APP_STATUS, APP_PURPOSE, APP_CHANNEL,
            REQ_PRODUCT_CODE, REQ_PRODUCT_NAME, REQ_PRODUCT_TYPE, REQ_AMOUNT, 
            REQ_TERM_MONTHS, REQ_CCY, REQ_PROD_MIN_AMOUNT, REQ_PROD_MAX_AMOUNT, 
            REQ_PROD_MIN_TERM, REQ_PROD_MAX_TERM, REQ_PROD_BASE_RATE_BPS,
            CUST_ID, CIS_CUSTOMER_NUMBER, CUST_FIRST_NAME, CUST_LAST_NAME, 
            CUST_DOB, CUST_TYPE, CUST_SEGMENT, CUST_RISK_BAND, CUST_EMAIL, 
            CUST_PHONE, ADDR_LINE1, ADDR_CITY, ADDR_PROVINCE, ADDR_POSTAL_CODE,
            EMPLOYER_NAME, EMPLOYMENT_TYPE, POSITION_TITLE, GROSS_MONTHLY_INCOME, 
            OTHER_INCOME, INCOME_CCY, INCOME_VERIFIED_FLAG, AVG_BAL_6M, 
            OD_LIMIT_TOTAL, NSF_12M, MAX_DPD, AVG_UTIL_PCT,
            SCORE_PROVIDER, SCORE_VALUE, SCORE_BAND, SCORE_AS_OF_DATE, 
            ELIGIBLE_FLAG, ELIG_FAIL_REASONS, HAS_COLLATERAL, HAS_GUARANTOR, 
            REC_PRODUCT_CODE, REC_AMOUNT, REC_TERM, REC_APR_BPS, 
            REC_CONDITIONS, REC_RATIONALE
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )`;
        
        const params = [
            app.APP_REF,
            app.APP_SUBMITTED_AT || new Date().toISOString().replace('T', ' ').substring(0, 19),
            app.APP_STATUS,
            app.APP_PURPOSE || null,
            app.APP_CHANNEL || null,
            app.REQ_PRODUCT_CODE || null,
            app.REQ_PRODUCT_NAME || null,
            app.REQ_PRODUCT_TYPE || null,
            app.REQ_AMOUNT || null,
            app.REQ_TERM_MONTHS || null,
            app.REQ_CCY || 'ZAR',
            app.REQ_PROD_MIN_AMOUNT || null,
            app.REQ_PROD_MAX_AMOUNT || null,
            app.REQ_PROD_MIN_TERM || null,
            app.REQ_PROD_MAX_TERM || null,
            app.REQ_PROD_BASE_RATE_BPS || null,
            app.CUST_ID || null,
            app.CIS_CUSTOMER_NUMBER,
            app.CUST_FIRST_NAME || null,
            app.CUST_LAST_NAME || null,
            app.CUST_DOB || null,
            app.CUST_TYPE || null,
            app.CUST_SEGMENT || null,
            app.CUST_RISK_BAND || null,
            app.CUST_EMAIL || null,
            app.CUST_PHONE || null,
            app.ADDR_LINE1 || null,
            app.ADDR_CITY || null,
            app.ADDR_PROVINCE || null,
            app.ADDR_POSTAL_CODE || null,
            app.EMPLOYER_NAME || null,
            app.EMPLOYMENT_TYPE || null,
            app.POSITION_TITLE || null,
            app.GROSS_MONTHLY_INCOME || null,
            app.OTHER_INCOME || null,
            app.INCOME_CCY || null,
            app.INCOME_VERIFIED_FLAG || null,
            app.AVG_BAL_6M || null,
            app.OD_LIMIT_TOTAL || null,
            app.NSF_12M || null,
            app.MAX_DPD || null,
            app.AVG_UTIL_PCT || null,
            app.SCORE_PROVIDER || null,
            app.SCORE_VALUE || null,
            app.SCORE_BAND || null,
            app.SCORE_AS_OF_DATE || null,
            app.ELIGIBLE_FLAG || null,
            app.ELIG_FAIL_REASONS || null,
            app.HAS_COLLATERAL || null,
            app.HAS_GUARANTOR || null,
            app.REC_PRODUCT_CODE || null,
            app.REC_AMOUNT || null,
            app.REC_TERM || null,
            app.REC_APR_BPS || null,
            app.REC_CONDITIONS || null,
            app.REC_RATIONALE || null
        ];
        
        await executeDB2Query(sql, params);
        
        res.status(201).json({
            success: true,
            message: 'Application created successfully',
            app_ref: app.APP_REF,
            customer: app.CIS_CUSTOMER_NUMBER
        });
    } catch (error) {
        console.error('Error creating application:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Update application status
app.patch('/api/application/:app_ref/status', async (req, res) => {
    try {
        const { app_ref } = req.params;
        const { status, reason } = req.body;
        
        if (!status) {
            return res.status(400).json({
                success: false,
                error: 'Missing required field: status'
            });
        }
        
        const validStatuses = ['APPROVED', 'REJECTED', 'IN_REVIEW', 'PENDING'];
        if (!validStatuses.includes(status.toUpperCase())) {
            return res.status(400).json({
                success: false,
                error: `Invalid status. Must be one of: ${validStatuses.join(', ')}`
            });
        }
        
        // Check if application exists
        const checkSql = `SELECT APP_REF FROM ${DB2_SCHEMA}.${DB2_TABLE} WHERE APP_REF = ?`;
        const existing = await executeDB2Query(checkSql, [app_ref]);
        
        if (existing.length === 0) {
            return res.status(404).json({
                success: false,
                error: 'Application not found',
                app_ref: app_ref
            });
        }
        
        let sql = `UPDATE ${DB2_SCHEMA}.${DB2_TABLE} SET APP_STATUS = ?`;
        let params = [status.toUpperCase()];
        
        if (reason && status.toUpperCase() === 'REJECTED') {
            sql += `, ELIG_FAIL_REASONS = ?`;
            params.push(reason);
        }
        
        sql += ` WHERE APP_REF = ?`;
        params.push(app_ref);
        
        await executeDB2Query(sql, params);
        
        res.json({
            success: true,
            message: 'Application status updated',
            app_ref: app_ref,
            new_status: status.toUpperCase()
        });
    } catch (error) {
        console.error('Error updating application status:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Update application (full or partial update)
app.put('/api/application/:app_ref', async (req, res) => {
    try {
        const { app_ref } = req.params;
        const updates = req.body;
        
        // Check if application exists
        const checkSql = `SELECT APP_REF FROM ${DB2_SCHEMA}.${DB2_TABLE} WHERE APP_REF = ?`;
        const existing = await executeDB2Query(checkSql, [app_ref]);
        
        if (existing.length === 0) {
            return res.status(404).json({
                success: false,
                error: 'Application not found',
                app_ref: app_ref
            });
        }
        
        // Build dynamic UPDATE query
        const fields = Object.keys(updates).filter(key => key !== 'APP_REF');
        
        if (fields.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'No fields to update'
            });
        }
        
        const setClause = fields.map(field => `${field} = ?`).join(', ');
        const values = fields.map(field => updates[field]);
        values.push(app_ref);
        
        const sql = `UPDATE ${DB2_SCHEMA}.${DB2_TABLE} SET ${setClause} WHERE APP_REF = ?`;
        
        await executeDB2Query(sql, values);
        
        res.json({
            success: true,
            message: 'Application updated successfully',
            app_ref: app_ref,
            updated_fields: fields
        });
    } catch (error) {
        console.error('Error updating application:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Delete application
app.delete('/api/application/:app_ref', async (req, res) => {
    try {
        const { app_ref } = req.params;
        
        // Check if application exists
        const checkSql = `SELECT APP_REF FROM ${DB2_SCHEMA}.${DB2_TABLE} WHERE APP_REF = ?`;
        const existing = await executeDB2Query(checkSql, [app_ref]);
        
        if (existing.length === 0) {
            return res.status(404).json({
                success: false,
                error: 'Application not found',
                app_ref: app_ref
            });
        }
        
        const sql = `DELETE FROM ${DB2_SCHEMA}.${DB2_TABLE} WHERE APP_REF = ?`;
        await executeDB2Query(sql, [app_ref]);
        
        res.json({
            success: true,
            message: 'Application deleted successfully',
            app_ref: app_ref
        });
    } catch (error) {
        console.error('Error deleting application:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ============================================
// STATISTICS & ANALYTICS ENDPOINTS
// ============================================

// Get application statistics
app.get('/api/stats/overview', async (req, res) => {
    try {
        const sql = `
            SELECT 
                COUNT(*) as TOTAL_APPLICATIONS,
                SUM(CASE WHEN APP_STATUS = 'APPROVED' THEN 1 ELSE 0 END) as APPROVED,
                SUM(CASE WHEN APP_STATUS = 'REJECTED' THEN 1 ELSE 0 END) as REJECTED,
                SUM(CASE WHEN APP_STATUS = 'IN_REVIEW' THEN 1 ELSE 0 END) as IN_REVIEW,
                SUM(CASE WHEN APP_STATUS = 'PENDING' THEN 1 ELSE 0 END) as PENDING,
                AVG(REQ_AMOUNT) as AVG_AMOUNT,
                SUM(REQ_AMOUNT) as TOTAL_AMOUNT,
                MIN(REQ_AMOUNT) as MIN_AMOUNT,
                MAX(REQ_AMOUNT) as MAX_AMOUNT,
                COUNT(DISTINCT CIS_CUSTOMER_NUMBER) as UNIQUE_CUSTOMERS
            FROM ${DB2_SCHEMA}.${DB2_TABLE}
        `;
        
        const results = await executeDB2Query(sql);
        
        res.json({
            success: true,
            statistics: results[0]
        });
    } catch (error) {
        console.error('Error fetching statistics:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get statistics by status
app.get('/api/stats/by-status', async (req, res) => {
    try {
        const sql = `
            SELECT 
                APP_STATUS,
                COUNT(*) as COUNT,
                AVG(REQ_AMOUNT) as AVG_AMOUNT,
                SUM(REQ_AMOUNT) as TOTAL_AMOUNT,
                MIN(REQ_AMOUNT) as MIN_AMOUNT,
                MAX(REQ_AMOUNT) as MAX_AMOUNT
            FROM ${DB2_SCHEMA}.${DB2_TABLE}
            GROUP BY APP_STATUS
            ORDER BY COUNT DESC
        `;
        
        const results = await executeDB2Query(sql);
        
        res.json({
            success: true,
            statistics_by_status: results
        });
    } catch (error) {
        console.error('Error fetching statistics:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get statistics by product
app.get('/api/stats/by-product', async (req, res) => {
    try {
        const sql = `
            SELECT 
                REQ_PRODUCT_CODE,
                REQ_PRODUCT_NAME,
                COUNT(*) as COUNT,
                AVG(REQ_AMOUNT) as AVG_AMOUNT,
                SUM(REQ_AMOUNT) as TOTAL_AMOUNT
            FROM ${DB2_SCHEMA}.${DB2_TABLE}
            WHERE REQ_PRODUCT_CODE IS NOT NULL
            GROUP BY REQ_PRODUCT_CODE, REQ_PRODUCT_NAME
            ORDER BY COUNT DESC
        `;
        
        const results = await executeDB2Query(sql);
        
        res.json({
            success: true,
            statistics_by_product: results
        });
    } catch (error) {
        console.error('Error fetching statistics:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ============================================
// ERROR HANDLING
// ============================================

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        success: false,
        error: 'Endpoint not found',
        path: req.path,
        method: req.method
    });
});

// Global error handler
app.use((err, req, res, next) => {
    console.error('Global error handler:', err);
    res.status(err.status || 500).json({
        success: false,
        error: err.message || 'Internal server error',
        stack: process.env.NODE_ENV === 'development' ? err.stack : undefined
    });
});

// ============================================
// START SERVER
// ============================================

app.listen(PORT, () => {
    console.log(`
    ========================================
    DB2 Credit Applications REST API
    ========================================
    Server running on: http://localhost:${PORT}
    Health check: http://localhost:${PORT}/health
    DB2 Test: http://localhost:${PORT}/api/db2/test
    Database: ${process.env.DB2_DATABASE}
    Schema: ${DB2_SCHEMA}
    Table: ${DB2_TABLE}
    ========================================
    `);
});

module.exports = app;