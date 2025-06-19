use actix_multipart::Multipart;
use actix_web::{middleware::Logger, web, App, HttpResponse, HttpServer, Result};
use calamine::{open_workbook_auto, Reader, RangeDeserializerBuilder};
use futures_util::TryStreamExt as _;
use rusqlite::{params, Connection, Result as SqliteResult};
use serde_json::json;
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;

#[derive(Debug)]
struct FundData {
    category: String,
    scheme_name: String,
    launch_date: String,
    fund_size_apr25: f64,
    fund_size_may25: f64,
    latest_nav: f64,
    days_7: f64,
    days_14: f64,
    days_21: f64,
    month_1: f64,
    months_3: f64,
    months_6: f64,
    ytd: f64,
    year_1: f64,
    years_2: f64,
    years_3: f64,
}

async fn upload_page() -> Result<HttpResponse> {
    let html = r#"
<!DOCTYPE html>
<html>
<head>
    <title>Excel to SQLite Converter</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 600px; margin: 0 auto; }
        .upload-area {
            border: 2px dashed #ccc;
            padding: 40px;
            text-align: center;
            margin: 20px 0;
        }
        input[type="file"] { margin: 20px 0; }
        button {
            background: #007bff;
            color: white;
            padding: 10px 20px;
            border: none;
            cursor: pointer;
        }
        .result { margin-top: 20px; padding: 20px; background: #f8f9fa; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Fund Barometer Excel to SQLite Converter</h1>
        <form action="/upload" method="post" enctype="multipart/form-data">
            <div class="upload-area">
                <p>Select your Excel file containing mutual fund data</p>
                <input type="file" name="excel_file" accept=".xlsx,.xls" required>
                <br>
                <button type="submit">Upload and Convert</button>
            </div>
        </form>
    </div>
</body>
</html>
"#;
    Ok(HttpResponse::Ok().content_type("text/html").body(html))
}

async fn upload_excel(mut payload: Multipart) -> Result<HttpResponse> {
    // Save uploaded file to temporary location
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

    // Process Excel file
    match process_excel_file(temp_path).await {
        Ok(count) => {
            let response = json!({
                "status": "success",
                "message": format!("Successfully processed {} fund records", count),
                "database": "funds.sqlite"
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
    // Open Excel workbook
    let mut workbook = open_workbook_auto(file_path)?;

    // Create SQLite database
    let conn = Connection::open("funds.sqlite")?;
    create_database_schema(&conn)?;

    let skip_sheets = vec!["Main Page", "Summary", "Glossary", "Load", "Disclaimer"];
    let mut total_records = 0;

    // Process each worksheet
    for sheet_name in workbook.sheet_names().clone() {
        if skip_sheets.contains(&sheet_name.as_str()) {
            continue;
        }

        println!("Processing sheet: {}", sheet_name);

        if let Ok(range) = workbook.worksheet_range(&sheet_name) {
            let records = extract_fund_data(&sheet_name, &range)?;
            let inserted = insert_fund_data(&conn, records)?;
            total_records += inserted;
            println!("Inserted {} records from sheet: {}", inserted, sheet_name);
        }
    }

    Ok(total_records)
}

fn create_database_schema(conn: &Connection) -> SqliteResult<()> {
    conn.execute("DROP TABLE IF EXISTS funds", [])?;

    conn.execute(
        "CREATE TABLE funds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            scheme_name TEXT NOT NULL,
            launch_date TEXT,
            fund_size_apr25 REAL,
            fund_size_may25 REAL,
            latest_nav REAL,
            days_7 REAL,
            days_14 REAL,
            days_21 REAL,
            month_1 REAL,
            months_3 REAL,
            months_6 REAL,
            ytd REAL,
            year_1 REAL,
            years_2 REAL,
            years_3 REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )",
        [],
    )?;

    Ok(())
}

fn extract_fund_data(
    category: &str,
    range: &calamine::Range<calamine::Data>
) -> Result<Vec<FundData>, Box<dyn std::error::Error>> {
    let mut funds = Vec::new();

    // Find header row (usually around row 8-9)
    let header_row_idx = find_header_row(range)?;

    // Process data rows
    for row_idx in (header_row_idx + 1)..range.height() {
        if let Some(fund) = parse_fund_row(category, range, row_idx) {
            funds.push(fund);
        }
    }

    Ok(funds)
}

fn find_header_row(range: &calamine::Range<calamine::Data>) -> Result<usize, &'static str> {
    for row_idx in 0..std::cmp::min(15, range.height()) {
        if let Some(cell) = range.get((row_idx, 0)) {
            if cell.to_string().contains("Scheme Name") {
                return Ok(row_idx);
            }
        }
    }
    Err("Header row not found")
}

fn parse_fund_row(
    category: &str,
    range: &calamine::Range<calamine::Data>,
    row_idx: usize,
) -> Option<FundData> {
    // Get scheme name (first column)
    let scheme_name = range.get((row_idx, 0))?.to_string();

    // Skip empty rows or header-like rows
    if scheme_name.is_empty()
        || scheme_name.contains("Scheme Name")
        || scheme_name.contains("To view Exit Loads")
        || scheme_name == category {
        return None;
    }

    Some(FundData {
        category: category.to_string(),
        scheme_name,
        launch_date: range.get((row_idx, 1)).map(|c| c.to_string()).unwrap_or_default(),
        fund_size_apr25: parse_float(range.get((row_idx, 2))),
        fund_size_may25: parse_float(range.get((row_idx, 3))),
        latest_nav: parse_float(range.get((row_idx, 4))),
        days_7: parse_float(range.get((row_idx, 5))),
        days_14: parse_float(range.get((row_idx, 6))),
        days_21: parse_float(range.get((row_idx, 7))),
        month_1: parse_float(range.get((row_idx, 8))),
        months_3: parse_float(range.get((row_idx, 9))),
        months_6: parse_float(range.get((row_idx, 10))),
        ytd: parse_float(range.get((row_idx, 11))),
        year_1: parse_float(range.get((row_idx, 12))),
        years_2: parse_float(range.get((row_idx, 13))),
        years_3: parse_float(range.get((row_idx, 14))),
    })
}

fn parse_float(cell: Option<&calamine::Data>) -> f64 {
    match cell {
        Some(calamine::Data::Float(f)) => *f,
        Some(calamine::Data::Int(i)) => *i as f64,
        Some(val) => val.to_string().parse().unwrap_or(0.0),
        None => 0.0,
    }
}

fn insert_fund_data(conn: &Connection, funds: Vec<FundData>) -> SqliteResult<usize> {
    let mut inserted = 0;

    for fund in funds {
        conn.execute(
            "INSERT INTO funds (
                category, scheme_name, launch_date, fund_size_apr25, fund_size_may25,
                latest_nav, days_7, days_14, days_21, month_1, months_3, months_6,
                ytd, year_1, years_2, years_3
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                fund.category, fund.scheme_name, fund.launch_date, fund.fund_size_apr25,
                fund.fund_size_may25, fund.latest_nav, fund.days_7, fund.days_14,
                fund.days_21, fund.month_1, fund.months_3, fund.months_6,
                fund.ytd, fund.year_1, fund.years_2, fund.years_3
            ],
        )?;
        inserted += 1;
    }

    Ok(inserted)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    println!("0.0.0.0:8081");

    HttpServer::new(|| {
        App::new()
            .wrap(Logger::default())
            .route("/", web::get().to(upload_page))
            .route("/upload", web::post().to(upload_excel))
    })
        .bind("0.0.0.0:8081")?
        .run()
        .await
}