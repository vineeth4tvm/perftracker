use actix_multipart::Multipart;
use actix_web::{middleware::Logger, web, App, HttpResponse, HttpServer, Result};
use calamine::{open_workbook_auto, Data, Range, Reader};
use futures_util::TryStreamExt as _;
use rusqlite::{params, Connection, Result as SqliteResult};
use serde_json::json;
use std::collections::HashMap;
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
    days_7: Option<f64>,
    days_14: Option<f64>,
    days_21: Option<f64>,
    month_1: f64,
    months_3: f64,
    months_6: f64,
    ytd: f64,
    year_1: f64,
    years_2: f64,
    years_3: f64,
    years_4: f64,
    years_5: f64,
    years_7: f64,
    years_10: f64,
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
    let mut workbook = open_workbook_auto(file_path)?;
    let conn = Connection::open("funds.sqlite")?;
    create_database_schema(&conn)?;

    let skip_sheets = vec!["Main Page", "Summary", "Glossary", "Load", "Disclaimer"];
    let mut all_funds = Vec::new();

    // First pass: collect all funds from all sheets
    for sheet_name in workbook.sheet_names().clone() {
        if skip_sheets.contains(&sheet_name.as_str()) {
            continue;
        }

        println!("Processing sheet: {}", sheet_name);

        if let Ok(range) = workbook.worksheet_range(&sheet_name) {
            let mut records = extract_fund_data(&sheet_name, &range)?;
            all_funds.append(&mut records);
            println!("Collected {} records from sheet: {}", records.len(), sheet_name);
        }
    }

    // Second pass: remove ALL duplicates globally
    let unique_funds = remove_all_duplicates(all_funds);
    println!("Total unique funds after global deduplication: {}", unique_funds.len());

    // Insert all unique funds
    let total_records = insert_fund_data(&conn, unique_funds)?;

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
            years_4 REAL,
            years_5 REAL,
            years_7 REAL,
            years_10 REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )",
        [],
    )?;

    Ok(())
}

fn extract_fund_data(
    category: &str,
    range: &Range<Data>,
) -> Result<Vec<FundData>, Box<dyn std::error::Error>> {
    let mut funds = Vec::new();
    let header_row_idx = find_header_row(range)?;

    // Check if the sheet has "7 Days" column to determine layout
    let has_days_columns = range
        .get((header_row_idx, 5))
        .map(|cell| cell.to_string().to_lowercase().contains("7 days"))
        .unwrap_or(false);

    // Debug: Log detected layout
    println!(
        "Sheet '{}': Using {} layout",
        category,
        if has_days_columns { "full (19 columns)" } else { "reduced (16 columns)" }
    );

    // Debug: Log header row
    let mut headers = Vec::new();
    for col_idx in 0..range.width() {
        let header = range
            .get((header_row_idx, col_idx))
            .map(|c| c.to_string())
            .unwrap_or_default();
        headers.push(header.clone());
        println!("Header column {}: {}", col_idx, header);
    }

    // Collect all valid funds from this sheet
    for row_idx in (header_row_idx + 1)..range.height() {
        if let Some(fund) = parse_fund_row(category, range, row_idx, has_days_columns) {
            funds.push(fund);
        }
    }

    println!("Sheet '{}': Collected {} funds", category, funds.len());

    Ok(funds)
}

fn remove_all_duplicates(all_funds: Vec<FundData>) -> Vec<FundData> {
    let mut scheme_name_counts = HashMap::new();
    let mut unique_funds = Vec::new();

    // Count occurrences of each scheme_name across ALL sheets
    for fund in &all_funds {
        *scheme_name_counts.entry(fund.scheme_name.clone()).or_insert(0) += 1;
    }

    // Filter to keep only funds with unique scheme_names
    for fund in all_funds {
        let count = scheme_name_counts.get(&fund.scheme_name).unwrap_or(&0);
        if *count > 1 {
            println!(
                "Removing ALL instances of duplicate scheme_name '{}' (total count: {})",
                fund.scheme_name, count
            );
            continue;
        }
        unique_funds.push(fund);
    }

    unique_funds
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

fn parse_fund_row(
    category: &str,
    range: &Range<Data>,
    row_idx: usize,
    has_days_columns: bool,
) -> Option<FundData> {
    let scheme_name = range.get((row_idx, 0))?.to_string();

    let launch_date = range.get((row_idx, 1)).map(|c| c.to_string()).unwrap_or_default();

    if scheme_name.is_empty()
        || scheme_name.to_lowercase().contains("scheme name")
        || scheme_name.contains("To view Exit Loads")
        || scheme_name == category
        || scheme_name.starts_with("# : \"In the")
        || scheme_name.starts_with("in the next")
        || launch_date.is_empty()
        || launch_date.trim().is_empty()
    {
        return None;
    }

    // Debug: Log raw row data
    let mut row_data = Vec::new();
    for col_idx in 0..range.width() {
        let cell = range
            .get((row_idx, col_idx))
            .map(|c| format!("{:?}", c))
            .unwrap_or_default();
        row_data.push(cell);
    }
    println!(
        "Row {}: scheme_name={}, raw_data={:?}",
        row_idx, scheme_name, row_data
    );

    let fund = if has_days_columns {
        // Full layout: Scheme Name, Launch Date, Fund Size Apr25, Fund Size May25, Latest NAV,
        // 7 Days, 14 Days, 21 Days, 1 Month, 3 Months, 6 Months, YTD, 1 Year, 2 Years, 3 Years,
        // 4 Years, 5 Years, 7 Years, 10 Years
        FundData {
            category: category.to_string(),
            scheme_name,
            launch_date,
            fund_size_apr25: parse_float(range.get((row_idx, 2))),
            fund_size_may25: parse_float(range.get((row_idx, 3))),
            latest_nav: parse_float(range.get((row_idx, 4))),
            days_7: Some(parse_float(range.get((row_idx, 5)))),
            days_14: Some(parse_float(range.get((row_idx, 6)))),
            days_21: Some(parse_float(range.get((row_idx, 7)))),
            month_1: parse_float(range.get((row_idx, 8))),
            months_3: parse_float(range.get((row_idx, 9))),
            months_6: parse_float(range.get((row_idx, 10))),
            ytd: parse_float(range.get((row_idx, 11))),
            year_1: parse_float(range.get((row_idx, 12))),
            years_2: parse_float(range.get((row_idx, 13))),
            years_3: parse_float(range.get((row_idx, 14))),
            years_4: parse_float(range.get((row_idx, 15))),
            years_5: parse_float(range.get((row_idx, 16))),
            years_7: parse_float(range.get((row_idx, 17))),
            years_10: parse_float(range.get((row_idx, 18))),
        }
    } else {
        // Reduced layout: Scheme Name, Launch Date, Fund Size Apr25, Fund Size May25, Latest NAV,
        // 1 Month, 3 Months, 6 Months, YTD, 1 Year, 2 Years, 3 Years,
        // 4 Years, 5 Years, 7 Years, 10 Years
        FundData {
            category: category.to_string(),
            scheme_name,
            launch_date: launch_date.clone(),
            fund_size_apr25: parse_float(range.get((row_idx, 2))),
            fund_size_may25: parse_float(range.get((row_idx, 3))),
            latest_nav: parse_float(range.get((row_idx, 4))),
            days_7: None,
            days_14: None,
            days_21: None,
            month_1: parse_float(range.get((row_idx, 5))),
            months_3: parse_float(range.get((row_idx, 6))),
            months_6: parse_float(range.get((row_idx, 7))),
            ytd: parse_float(range.get((row_idx, 8))),
            year_1: parse_float(range.get((row_idx, 9))),
            years_2: parse_float(range.get((row_idx, 10))),
            years_3: parse_float(range.get((row_idx, 11))),
            years_4: parse_float(range.get((row_idx, 12))),
            years_5: parse_float(range.get((row_idx, 13))),
            years_7: parse_float(range.get((row_idx, 14))),
            years_10: parse_float(range.get((row_idx, 15))),
        }
    };

    // Debug: Log parsed fund data
    println!("Parsed fund for row {}: {:?}", row_idx, fund);

    Some(fund)
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

fn parse_float(cell: Option<&Data>) -> f64 {
    match cell {
        Some(Data::Float(f)) => *f,
        Some(Data::Int(i)) => *i as f64,
        Some(val) => val.to_string().parse().unwrap_or(0.0),
        None => 0.0,
    }
}

fn insert_fund_data(conn: &Connection, funds: Vec<FundData>) -> SqliteResult<usize> {
    let mut inserted = 0;

    for mut fund in funds {
        // Clean the scheme_name before insertion
        let scheme_name1 = clean_scheme_name(fund.scheme_name);
        fund.scheme_name = clean_scheme_name(scheme_name1);

        // Debug: Log fund data before insertion
        println!("Inserting fund: {:?}", fund);
        conn.execute(
            "INSERT INTO funds (
                category, scheme_name, launch_date, fund_size_apr25, fund_size_may25,
                latest_nav, days_7, days_14, days_21, month_1, months_3, months_6,
                ytd, year_1, years_2, years_3, years_4, years_5, years_7, years_10
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
            params![
                fund.category,
                fund.scheme_name,
                fund.launch_date,
                fund.fund_size_apr25,
                fund.fund_size_may25,
                fund.latest_nav,
                fund.days_7,
                fund.days_14,
                fund.days_21,
                fund.month_1,
                fund.months_3,
                fund.months_6,
                fund.ytd,
                fund.year_1,
                fund.years_2,
                fund.years_3,
                fund.years_4,
                fund.years_5,
                fund.years_7,
                fund.years_10
            ],
        )?;
        inserted += 1;
    }

    Ok(inserted)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    println!("Starting server at http://0.0.0.0:8081");

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