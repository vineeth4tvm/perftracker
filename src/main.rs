use actix_multipart::Multipart;
use actix_web::{middleware::Logger, web, App, HttpResponse, HttpServer, Result};
use calamine::{open_workbook_auto, Data, Range, Reader};
use futures_util::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tempfile::NamedTempFile;
use tokio_postgres::{NoTls, Client};
use chrono::NaiveDate;
use log::{info, warn, error};

// Combined virtual table structure for search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CombinedSchemeData {
    // From funds table
    pub fund_id: Option<i32>,
    pub fund_category: Option<String>,
    pub launch_date: Option<String>,
    pub fund_size_apr25: Option<f32>,
    pub fund_size_may25: Option<f32>,
    pub latest_nav: Option<f32>,
    pub month_1: Option<f32>,
    pub months_3: Option<f32>,
    pub months_6: Option<f32>,
    pub ytd: Option<f32>,
    pub year_1: Option<f32>,
    pub years_2: Option<f32>,
    pub years_3: Option<f32>,
    pub years_5: Option<f32>,

    // From scheme_rates table
    pub rate_id: Option<i32>,
    pub arn: Option<String>,
    pub company: Option<String>,
    pub scheme_category: Option<String>,
    pub brokerage_type: Option<String>,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub base_year_1: Option<f32>,
    pub base_year_2: Option<f32>,
    pub base_year_3: Option<f32>,

    // Common fields
    pub scheme_name: String,
    pub normalized_name: String,
}

// In-memory virtual table
#[derive(Debug, Clone)]
pub struct VirtualTable {
    pub data: Vec<CombinedSchemeData>,
    pub name_index: HashMap<String, Vec<usize>>, // For fast lookups
}

impl VirtualTable {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            name_index: HashMap::new(),
        }
    }

    pub fn add_record(&mut self, record: CombinedSchemeData) {
        let index = self.data.len();
        let normalized = normalize_scheme_name(&record.scheme_name);

        self.name_index
            .entry(normalized.clone())
            .or_insert_with(Vec::new)
            .push(index);

        self.data.push(record);
    }

    pub fn search(&self, query: &str, limit: usize) -> Vec<CombinedSchemeData> {
        let normalized_query = normalize_scheme_name(query);
        let mut results = Vec::new();

        // Exact match first
        if let Some(indices) = self.name_index.get(&normalized_query) {
            for &idx in indices {
                if results.len() >= limit { break; }
                results.push(self.data[idx].clone());
            }
        }

        // Partial matches if we need more results
        if results.len() < limit {
            for (name, indices) in &self.name_index {
                if name.contains(&normalized_query) && name != &normalized_query {
                    for &idx in indices {
                        if results.len() >= limit { break; }
                        // Avoid duplicates
                        if !results.iter().any(|r| r.normalized_name == self.data[idx].normalized_name) {
                            results.push(self.data[idx].clone());
                        }
                    }
                }
                if results.len() >= limit { break; }
            }
        }

        results
    }
}

#[derive(Debug)]
struct FundData {
    category: String,
    scheme_name: String,
    launch_date: String,
    fund_size_apr25: Option<f32>,
    fund_size_may25: Option<f32>,
    latest_nav: Option<f32>,
    month_1: Option<f32>,
    months_3: Option<f32>,
    months_6: Option<f32>,
    ytd: Option<f32>,
    year_1: Option<f32>,
    years_2: Option<f32>,
    years_3: Option<f32>,
    years_5: Option<f32>,
}

// Application state to hold the virtual table
#[derive(Debug, Clone)]
pub struct AppState {
    pub virtual_table: Arc<RwLock<VirtualTable>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            virtual_table: Arc::new(RwLock::new(VirtualTable::new())),
        }
    }
}

async fn get_postgres_client() -> Result<Client, Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost user=vineeth password=Bluebridge@2025 dbname=funds_db",
        NoTls,
    ).await?;

    // Spawn the connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

// Fixed table initialization with proper constraint creation
async fn initialize_postgres_tables(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // Drop existing tables to recreate with proper constraints (be careful with this in production!)
    client.execute("DROP TABLE IF EXISTS funds CASCADE", &[]).await?;
    client.execute("DROP TABLE IF EXISTS scheme_rates CASCADE", &[]).await?;

    // Create funds table with proper UNIQUE constraint
    client.execute(
        "CREATE TABLE funds (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            scheme_name TEXT NOT NULL,
            launch_date TEXT,
            fund_size_apr25 REAL,
            fund_size_may25 REAL,
            latest_nav REAL,
            month_1 REAL,
            months_3 REAL,
            months_6 REAL,
            ytd REAL,
            year_1 REAL,
            years_2 REAL,
            years_3 REAL,
            years_5 REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_scheme_name UNIQUE (scheme_name)
        )",
        &[],
    ).await?;

    // Create scheme_rates table
    client.execute(
        "CREATE TABLE scheme_rates (
            id SERIAL PRIMARY KEY,
            arn TEXT NOT NULL,
            company TEXT NOT NULL,
            scheme_name TEXT NOT NULL,
            scheme_category TEXT NOT NULL,
            brokerage_type TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            source_file TEXT NOT NULL,
            is_approved BOOLEAN DEFAULT true,
            base_year_1 REAL,
            base_year_2 REAL,
            base_year_3 REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
        &[],
    ).await?;

    // Create indexes for better performance
    client.execute(
        "CREATE INDEX IF NOT EXISTS idx_funds_scheme_name ON funds USING gin(to_tsvector('english', scheme_name))",
        &[],
    ).await?;

    client.execute(
        "CREATE INDEX IF NOT EXISTS idx_scheme_rates_scheme_name ON scheme_rates USING gin(to_tsvector('english', scheme_name))",
        &[],
    ).await?;

    info!("Database tables initialized successfully");
    Ok(())
}

async fn build_virtual_table(client: &Client) -> Result<VirtualTable, Box<dyn std::error::Error>> {
    info!("Building virtual table from combined data...");

    let mut virtual_table = VirtualTable::new();

    // Query to get combined data - LEFT JOIN to get all funds even if no scheme_rates match
    let query = "
        SELECT
            f.id as fund_id,
            f.category as fund_category,
            f.launch_date,
            f.fund_size_apr25,
            f.fund_size_may25,
            f.latest_nav,
            f.month_1,
            f.months_3,
            f.months_6,
            f.ytd,
            f.year_1,
            f.years_2,
            f.years_3,
            f.years_5,
            sr.id as rate_id,
            sr.arn,
            sr.company,
            sr.scheme_category,
            sr.brokerage_type,
            sr.start_date,
            sr.end_date,
            sr.base_year_1,
            sr.base_year_2,
            sr.base_year_3,
            f.scheme_name
        FROM funds f
        LEFT JOIN scheme_rates sr ON
            LOWER(REGEXP_REPLACE(f.scheme_name, '[^a-zA-Z0-9\\s]', '', 'g')) =
            LOWER(REGEXP_REPLACE(sr.scheme_name, '[^a-zA-Z0-9\\s]', '', 'g'))
            AND (sr.is_approved IS NULL OR sr.is_approved = true)
            AND (sr.end_date IS NULL OR sr.end_date >= CURRENT_DATE)
    ";

    let rows = client.query(query, &[]).await?;

    for row in rows {
        let scheme_name: String = row.get("scheme_name");
        let normalized_name = normalize_scheme_name(&scheme_name);

        let combined_data = CombinedSchemeData {
            fund_id: row.get("fund_id"),
            fund_category: row.get("fund_category"),
            launch_date: row.get("launch_date"),
            fund_size_apr25: row.get("fund_size_apr25"),
            fund_size_may25: row.get("fund_size_may25"),
            latest_nav: row.get("latest_nav"),
            month_1: row.get("month_1"),
            months_3: row.get("months_3"),
            months_6: row.get("months_6"),
            ytd: row.get("ytd"),
            year_1: row.get("year_1"),
            years_2: row.get("years_2"),
            years_3: row.get("years_3"),
            years_5: row.get("years_5"),
            rate_id: row.get("rate_id"),
            arn: row.get("arn"),
            company: row.get("company"),
            scheme_category: row.get("scheme_category"),
            brokerage_type: row.get("brokerage_type"),
            start_date: row.get("start_date"),
            end_date: row.get("end_date"),
            base_year_1: row.get("base_year_1"),
            base_year_2: row.get("base_year_2"),
            base_year_3: row.get("base_year_3"),
            scheme_name: scheme_name.clone(),
            normalized_name,
        };

        virtual_table.add_record(combined_data);
    }

    info!("Virtual table built with {} combined records", virtual_table.data.len());
    Ok(virtual_table)
}

async fn refresh_virtual_table(state: &AppState) -> Result<(), Box<dyn std::error::Error>> {
    let client = get_postgres_client().await?;
    let new_table = build_virtual_table(&client).await?;

    // Update the virtual table in state
    let mut virtual_table = state.virtual_table.write().unwrap();
    *virtual_table = new_table;

    Ok(())
}

async fn upload_page() -> Result<HttpResponse> {
    let html = r#"
<!DOCTYPE html>
<html>
<head>
    <title>Combined Fund & Scheme Search</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 800px; margin: 0 auto; }
        .search-section { margin: 20px 0; }
        .upload-area {
            border: 2px dashed #ccc;
            padding: 40px;
            text-align: center;
            margin: 20px 0;
        }
        input[type="file"], input[type="text"] { margin: 10px 0; padding: 8px; }
        button {
            background: #007bff;
            color: white;
            padding: 10px 20px;
            border: none;
            cursor: pointer;
            margin: 5px;
        }
        .result { margin-top: 20px; padding: 20px; background: #f8f9fa; }
        .search-results { margin-top: 20px; }
        .scheme-card {
            border: 1px solid #ddd;
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
        }
        .performance { color: #28a745; }
        .brokerage { color: #dc3545; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Combined Fund & Scheme Search System</h1>

        <div class="search-section">
            <h2>Search Combined Data</h2>
            <input type="text" id="searchQuery" placeholder="Enter scheme name to search..." style="width: 300px;">
            <button onclick="searchSchemes()">Search</button>
            <button onclick="refreshData()">Refresh Data</button>
        </div>

        <div id="searchResults" class="search-results"></div>

        <hr>

        <h2>Upload Excel Data</h2>
        <form action="/upload" method="post" enctype="multipart/form-data">
            <div class="upload-area">
                <p>Select your Excel file containing mutual fund data</p>
                <input type="file" name="excel_file" accept=".xlsx,.xls" required>
                <br>
                <button type="submit">Upload and Convert</button>
            </div>
        </form>
    </div>

    <script>
        async function searchSchemes() {
            const query = document.getElementById('searchQuery').value;
            if (!query.trim()) return;

            try {
                const response = await fetch(`/search?q=${encodeURIComponent(query)}`);
                const results = await response.json();
                displayResults(results);
            } catch (error) {
                console.error('Search error:', error);
            }
        }

        async function refreshData() {
            try {
                const response = await fetch('/refresh', { method: 'POST' });
                const result = await response.json();
                alert(result.message);
            } catch (error) {
                console.error('Refresh error:', error);
            }
        }

        function displayResults(results) {
            const container = document.getElementById('searchResults');
            if (!results.data || results.data.length === 0) {
                container.innerHTML = '<p>No results found.</p>';
                return;
            }

            let html = `<h3>Found ${results.data.length} combined schemes:</h3>`;

            results.data.forEach(scheme => {
                html += `
                    <div class="scheme-card">
                        <h4>${scheme.scheme_name}</h4>
                        <div class="performance">
                            <strong>Performance:</strong>
                            1M: ${scheme.month_1 || 'N/A'}% |
                            3M: ${scheme.months_3 || 'N/A'}% |
                            1Y: ${scheme.year_1 || 'N/A'}% |
                            NAV: ₹${scheme.latest_nav || 'N/A'}
                        </div>
                        <div class="brokerage">
                            <strong>Brokerage:</strong>
                            Company: ${scheme.company || 'N/A'} |
                            Type: ${scheme.brokerage_type || 'N/A'} |
                            Year 1: ${scheme.base_year_1 || 'N/A'}%
                        </div>
                        <small>Category: ${scheme.fund_category || scheme.scheme_category || 'N/A'}</small>
                    </div>
                `;
            });

            container.innerHTML = html;
        }

        // Search on Enter key
        document.getElementById('searchQuery').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                searchSchemes();
            }
        });
    </script>
</body>
</html>
"#;
    Ok(HttpResponse::Ok().content_type("text/html").body(html))
}

async fn search_schemes(
    query: web::Query<HashMap<String, String>>,
    state: web::Data<AppState>,
) -> Result<HttpResponse> {
    let search_term = match query.get("q") {
        Some(q) => q,
        None => return Ok(HttpResponse::BadRequest().json(json!({"error": "Query parameter 'q' is required"}))),
    };

    let results = {
        let virtual_table = state.virtual_table.read().unwrap();
        virtual_table.search(search_term, 20)
    };

    Ok(HttpResponse::Ok().json(json!({
        "status": "success",
        "query": search_term,
        "count": results.len(),
        "data": results
    })))
}

async fn refresh_virtual_table_endpoint(state: web::Data<AppState>) -> Result<HttpResponse> {
    match refresh_virtual_table(&state).await {
        Ok(_) => {
            let count = {
                let virtual_table = state.virtual_table.read().unwrap();
                virtual_table.data.len()
            };
            Ok(HttpResponse::Ok().json(json!({
                "status": "success",
                "message": format!("Virtual table refreshed with {} records", count)
            })))
        }
        Err(e) => {
            error!("Failed to refresh virtual table: {}", e);
            Ok(HttpResponse::InternalServerError().json(json!({
                "status": "error",
                "message": format!("Failed to refresh: {}", e)
            })))
        }
    }
}

async fn upload_excel(mut payload: Multipart, state: web::Data<AppState>) -> Result<HttpResponse> {
    let mut temp_file = NamedTempFile::new().map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to create temp file: {}", e))
    })?;

    while let Some(mut field) = payload.try_next().await? {
        while let Some(chunk) = field.try_next().await? {
            temp_file.write_all(&chunk).map_err(|e| {
                actix_web::error::ErrorInternalServerError(format!("Failed to write chunk: {}", e))
            })?;
        }
    }

    let temp_path = temp_file.path();

    match process_excel_file(temp_path).await {
        Ok(count) => {
            // Refresh virtual table after upload
            if let Err(e) = refresh_virtual_table(&state).await {
                warn!("Failed to refresh virtual table after upload: {}", e);
            }

            let response = json!({
                "status": "success",
                "message": format!("Successfully processed {} fund records and refreshed search index", count)
            });
            Ok(HttpResponse::Ok().json(response))
        }
        Err(e) => {
            let response = json!({
                "status": "error",
                "message": format!("Error processing file: {}", e)
            });
            Ok(HttpResponse::InternalServerError().json(response))
        }
    }
}

async fn process_excel_file(file_path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let mut workbook = open_workbook_auto(file_path)?;
    let client = get_postgres_client().await?;

    let skip_sheets = vec!["Main Page", "Summary", "Glossary", "Load", "Disclaimer"];
    let mut all_funds = Vec::new();

    // Process Excel sheets
    for sheet_name in workbook.sheet_names().clone() {
        if skip_sheets.contains(&sheet_name.as_str()) {
            continue;
        }

        info!("Processing sheet: {}", sheet_name);

        if let Ok(range) = workbook.worksheet_range(&sheet_name) {
            let mut records = extract_fund_data(&sheet_name, &range)?;
            all_funds.append(&mut records);
            info!("Collected {} records from sheet: {}", records.len(), sheet_name);
        }
    }

    // Remove duplicates and insert
    let unique_funds = remove_all_duplicates(all_funds);
    let total_records = insert_fund_data(&client, unique_funds).await?;

    Ok(total_records)
}

// Helper functions (keeping the existing logic but adapting for PostgreSQL)
fn normalize_scheme_name(scheme_name: &str) -> String {
    scheme_name
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn extract_fund_data(
    category: &str,
    range: &Range<Data>,
) -> Result<Vec<FundData>, Box<dyn std::error::Error>> {
    let mut funds = Vec::new();
    let header_row_idx = find_header_row(range)?;

    for row_idx in (header_row_idx + 1)..range.height() {
        if let Some(fund) = parse_fund_row(category, range, row_idx) {
            funds.push(fund);
        }
    }

    Ok(funds)
}

fn find_header_row(range: &Range<Data>) -> Result<usize, &'static str> {
    for row_idx in 0..std::cmp::min(15, range.height()) {
        if let Some(cell) = range.get((row_idx, 0)) {
            if cell.to_string().to_lowercase().contains("scheme name") {
                return Ok(row_idx);
            }
        }
    }
    Err("Header row not found")
}

fn parse_fund_row(category: &str, range: &Range<Data>, row_idx: usize) -> Option<FundData> {
    let scheme_name = range.get((row_idx, 0))?.to_string();
    let launch_date = range.get((row_idx, 1)).map(|c| c.to_string()).unwrap_or_default();

    if scheme_name.is_empty() || launch_date.is_empty() {
        return None;
    }

    Some(FundData {
        category: category.to_string(),
        scheme_name,
        launch_date,
        fund_size_apr25: parse_float_option(range.get((row_idx, 2))),
        fund_size_may25: parse_float_option(range.get((row_idx, 3))),
        latest_nav: parse_float_option(range.get((row_idx, 4))),
        month_1: parse_float_option(range.get((row_idx, 5))),
        months_3: parse_float_option(range.get((row_idx, 6))),
        months_6: parse_float_option(range.get((row_idx, 7))),
        ytd: parse_float_option(range.get((row_idx, 8))),
        year_1: parse_float_option(range.get((row_idx, 9))),
        years_2: parse_float_option(range.get((row_idx, 10))),
        years_3: parse_float_option(range.get((row_idx, 11))),
        years_5: parse_float_option(range.get((row_idx, 13))),
    })
}

fn remove_all_duplicates(all_funds: Vec<FundData>) -> Vec<FundData> {
    let mut unique_funds = Vec::new();
    let mut seen_names = std::collections::HashSet::new();

    for fund in all_funds {
        let normalized = normalize_scheme_name(&fund.scheme_name);
        if seen_names.insert(normalized) {
            unique_funds.push(fund);
        }
    }

    unique_funds
}

// Updated parse functions to handle None values properly
fn parse_float_option(cell: Option<&Data>) -> Option<f32> {
    match cell {
        Some(Data::Float(f)) => {
            if f.is_finite() && !f.is_nan() {
                Some(*f as f32)
            } else {
                None
            }
        },
        Some(Data::Int(i)) => Some(*i as f32),
        Some(val) => {
            let s_owned = val.to_string();
            let s = s_owned.trim();
            if s.is_empty() || s == "N/A" || s == "-" {
                None
            } else {
                s.parse().ok()
            }
        },
        None => None,
    }
}

// Keep this for backward compatibility if needed elsewhere
fn parse_float(cell: Option<&Data>) -> f32 {
    parse_float_option(cell).unwrap_or(0.0)
}

// Fixed insert function with proper error handling
async fn insert_fund_data(client: &Client, funds: Vec<FundData>) -> Result<usize, Box<dyn std::error::Error>> {
    let mut inserted = 0;

    for fund in funds {
        // Clean the scheme name before insertion or update
        let cleaned_scheme_name = clean_scheme_name(fund.scheme_name.clone());

        // First try to update existing record
        let update_result = client.execute(
            "UPDATE funds SET
                category = $2,
                launch_date = $3,
                fund_size_apr25 = $4,
                fund_size_may25 = $5,
                latest_nav = $6,
                month_1 = $7,
                months_3 = $8,
                months_6 = $9,
                ytd = $10,
                year_1 = $11,
                years_2 = $12,
                years_3 = $13,
                years_5 = $14
            WHERE scheme_name = $1",
            &[
                &cleaned_scheme_name, // Use cleaned scheme name
                &fund.category,
                &fund.launch_date,
                &fund.fund_size_apr25,
                &fund.fund_size_may25,
                &fund.latest_nav,
                &fund.month_1,
                &fund.months_3,
                &fund.months_6,
                &fund.ytd,
                &fund.year_1,
                &fund.years_2,
                &fund.years_3,
                &fund.years_5,
            ],
        ).await?;

        if update_result == 0 {
            // If no rows were updated, insert new record
            let insert_result = client.execute(
                "INSERT INTO funds (
                    category, scheme_name, launch_date, fund_size_apr25, fund_size_may25,
                    latest_nav, month_1, months_3, months_6, ytd, year_1, years_2, years_3, years_5
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                &[
                    &fund.category,
                    &cleaned_scheme_name, // Use cleaned scheme name
                    &fund.launch_date,
                    &fund.fund_size_apr25,
                    &fund.fund_size_may25,
                    &fund.latest_nav,
                    &fund.month_1,
                    &fund.months_3,
                    &fund.months_6,
                    &fund.ytd,
                    &fund.year_1,
                    &fund.years_2,
                    &fund.years_3,
                    &fund.years_5,
                ],
            ).await;

            match insert_result {
                Ok(rows) => {
                    if rows > 0 {
                        inserted += 1;
                    }
                }
                Err(e) => {
                    // Log error but continue processing other records
                    warn!("Failed to insert fund '{}': {}", cleaned_scheme_name, e);
                }
            }
        } else {
            inserted += 1; // Count updates as well
        }
    }

    Ok(inserted)
}


fn clean_scheme_name(mut name: String) -> String {
    // Step 1: Initial trim of whitespace and special characters
    // Remove leading special characters
    while let Some(first_char) = name.chars().next() {
        if first_char.is_alphanumeric() {
            break;
        }
        name = name.chars().skip(1).collect();
    }

    // Remove trailing special characters
    while let Some(last_char) = name.chars().last() {
        if last_char.is_alphanumeric() {
            break;
        }
        name = name.chars().take(name.len() - 1).collect();
    }

    // Trim whitespace and normalize multiple spaces
    name = name.trim().split_whitespace().collect::<Vec<&str>>().join(" ");


    // Step 2: Remove specific strings globally (anywhere in the string)
    let global_remove = [
        "- Reg - Growth",// Common parenthetical terms
        " - Reg - Growth",// Common parenthetical terms
        "- Reg - Gth",
        " - Reg - Gth",
        " - Reg - G P",
        "- Reg - G P",
        " - Reg ",
        "- Reg ",
        "-Reg-Growth",
    ];

    for pattern in global_remove.iter() {
        name = name.replace(pattern, "");
    }


    // Step 2: Remove specific suffixes (in order of preference, longest to shortest)
    let suffixes = [
        "- Reg - Growth",
        "- Reg - Gth",
        "- Growth",
        "-Reg",
        "-Reg ",
        "-Growth",
        "Growth",
        "- Reg",
        "Regular",
        " Regular",
        "- Reg - G P",
        " - Reg",
        " - Reg - G P",
        " - Reg - Growth (Re-launched",
        " - Regular",
        " -Reg",
        " - Reg ",
        "- Regular",
        "- Regular ",
        " - Regular ",
        " -Reg ",
        "-Reg-Growth",
    ];

    for suffix in suffixes.iter() {
        name = name.strip_suffix(suffix).unwrap_or(&name).to_string();
    }

    // Step 4: Second trim to clean up residual whitespace or special characters
    // Remove leading special characters
    while let Some(first_char) = name.chars().next() {
        if first_char.is_alphanumeric() {
            break;
        }
        name = name.chars().skip(1).collect();
    }

    // Remove trailing special characters
    while let Some(last_char) = name.chars().last() {
        if last_char.is_alphanumeric() {
            break;
        }
        name = name.chars().take(name.len() - 1).collect();
    }

    // Final trim and normalize multiple spaces
    name.trim().split_whitespace().collect::<Vec<&str>>().join(" ")
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    // Create application state
    let app_state = AppState::new();

    // Initialize database and virtual table
    let client = get_postgres_client().await.expect("Failed to connect to PostgreSQL");
    initialize_postgres_tables(&client).await.expect("Failed to initialize tables");

    // Build initial virtual table
    match build_virtual_table(&client).await {
        Ok(table) => {
            info!("Initial virtual table built with {} records", table.data.len());
            let mut virtual_table = app_state.virtual_table.write().unwrap();
            *virtual_table = table;
        }
        Err(e) => {
            warn!("Failed to build initial virtual table: {}", e);
        }
    }

    info!("Starting server at http://0.0.0.0:8081");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app_state.clone()))
            .wrap(Logger::default())
            .route("/", web::get().to(upload_page))
            .route("/upload", web::post().to(upload_excel))
            .route("/search", web::get().to(search_schemes))
            .route("/refresh", web::post().to(refresh_virtual_table_endpoint))
    })
        .bind("0.0.0.0:8081")?
        .run()
        .await
}