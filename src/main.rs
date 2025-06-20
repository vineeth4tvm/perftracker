use actix_multipart::Multipart;
use actix_web::{middleware::Logger, web, App, HttpResponse, HttpServer, Result};
use calamine::{open_workbook_auto, Data, Reader, Range};
use futures_util::TryStreamExt as _;
use rusqlite::{params, Connection, Result as SqliteResult};
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;

#[derive(Debug, Clone)]
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
    years_5: f64,
    years_7: f64,
    years_10: f64,
    since_inception: f64,
    additional_cols: Vec<f64>,
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
        .debug { margin-top: 20px; padding: 10px; background: #e9ecef; font-family: monospace; }
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
        Ok((inserted_count, duplicate_count, debug_info)) => {
            let response = json!({
                "status": "success",
                "message": format!("Successfully processed {} fund records, removed {} duplicates", inserted_count, duplicate_count),
                "database": "funds.sqlite",
                "debug": debug_info
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

async fn process_excel_file(
    file_path: &Path,
) -> Result<(usize, usize, String), Box<dyn std::error::Error>> {
    let mut workbook = open_workbook_auto(file_path)?;
    let conn = Connection::open("funds.sqlite")?;
    create_database_schema(&conn)?;

    let skip_sheets = vec![
        "Main Page",
        "Summary",
        "Glossary",
        "Load",
        "Disclaimer",
        "Intro",
        "Introduction",
        "Cover",
        "Index",
        "Contents",
        "Methodology",
        "Note",
        "Notes",
    ];

    let mut all_funds: Vec<FundData> = Vec::new();
    let mut debug_info = String::new();

    for sheet_name in workbook.sheet_names().clone() {
        if skip_sheets
            .iter()
            .any(|&skip| sheet_name.to_lowercase().contains(&skip.to_lowercase()))
        {
            debug_info.push_str(&format!("Skipping sheet: {}\n", sheet_name));
            continue;
        }

        debug_info.push_str(&format!("Processing sheet: {}\n", sheet_name));

        if let Ok(range) = workbook.worksheet_range(&sheet_name) {
            let (mut records, sheet_debug) = extract_fund_data(&sheet_name, &range)?;
            debug_info.push_str(&sheet_debug);
            all_funds.append(&mut records);
        }
    }

    let (unique_funds, duplicate_count) = remove_duplicates(all_funds);
    let inserted_count = insert_fund_data(&conn, unique_funds)?;

    debug_info.push_str(&format!(
        "Total inserted: {}, Duplicates removed: {}\n",
        inserted_count, duplicate_count
    ));

    Ok((inserted_count, duplicate_count, debug_info))
}

fn remove_duplicates(funds: Vec<FundData>) -> (Vec<FundData>, usize) {
    let mut scheme_counts: HashMap<String, usize> = HashMap::new();

    for fund in &funds {
        let count = scheme_counts.entry(fund.scheme_name.clone()).or_insert(0);
        *count += 1;
    }

    let unique_funds: Vec<FundData> = funds
        .into_iter()
        .filter(|fund| *scheme_counts.get(&fund.scheme_name).unwrap_or(&0) == 1)
        .collect();

    let total_duplicates = scheme_counts
        .values()
        .filter(|&&count| count > 1)
        .sum::<usize>();

    (unique_funds, total_duplicates)
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
            years_5 REAL,
            years_7 REAL,
            years_10 REAL,
            since_inception REAL,
            additional_data TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )",
        [],
    )?;

    Ok(())
}

fn extract_fund_data(
    category: &str,
    range: &Range<Data>,
) -> Result<(Vec<FundData>, String), Box<dyn std::error::Error>> {
    let mut funds = Vec::new();
    let mut debug_info = String::new();

    let header_row_idx = find_header_row(range)?;
    debug_info.push_str(&format!(
        "  Header row found at index: {}\n",
        header_row_idx
    ));

    let column_mapping = analyze_headers(range, header_row_idx);
    debug_info.push_str(&format!("  Column mapping: {:?}\n", column_mapping));

    for row_idx in (header_row_idx + 1)..range.height() {
        if let Some(fund) = parse_fund_row_with_mapping(category, range, row_idx, &column_mapping)
        {
            funds.push(fund);
        }
    }

    debug_info.push_str(&format!("  Extracted {} fund records\n", funds.len()));

    Ok((funds, debug_info))
}

fn find_header_row(range: &Range<Data>) -> Result<usize, &'static str> {
    for row_idx in 0..std::cmp::min(15, range.height()) {
        for col_idx in 0..std::cmp::min(5, range.width()) {
            if let Some(cell) = range.get((row_idx, col_idx)) {
                let cell_text = cell.to_string().to_lowercase();
                if cell_text.contains("scheme name")
                    || cell_text.contains("fund name")
                    || cell_text.contains("launch date")
                    || cell_text.contains("nav")
                {
                    return Ok(row_idx);
                }
            }
        }
    }
    Err("Header row not found")
}

fn analyze_headers(range: &Range<Data>, header_row_idx: usize) -> HashMap<String, usize> {
    let mut column_mapping = HashMap::new();

    if let Some(header_row) = range.rows().nth(header_row_idx) {
        for (col_idx, cell) in header_row.iter().enumerate() {
            let header_text = cell.to_string().to_lowercase().trim().to_string();

            match header_text.as_str() {
                h if h.contains("scheme name") || h.contains("fund name") => {
                    column_mapping.insert("scheme_name".to_string(), col_idx);
                }
                h if h.contains("launch date") => {
                    column_mapping.insert("launch_date".to_string(), col_idx);
                }
                h if h.contains("fund size") && h.contains("apr") => {
                    column_mapping.insert("fund_size_apr25".to_string(), col_idx);
                }
                h if h.contains("fund size") && h.contains("may") => {
                    column_mapping.insert("fund_size_may25".to_string(), col_idx);
                }
                h if h.contains("latest nav") => {
                    column_mapping.insert("latest_nav".to_string(), col_idx);
                }
                h if h.contains("nav") && !column_mapping.contains_key("latest_nav") => {
                    column_mapping.insert("latest_nav".to_string(), col_idx);
                }
                h if h.contains("since inception") => {
                    column_mapping.insert("since_inception".to_string(), col_idx);
                }
                h if h.contains("10") && (h.contains("year") || h.contains('y')) => {
                    column_mapping.insert("years_10".to_string(), col_idx);
                }
                h if h.contains("7") && (h.contains("year") || h.contains('y')) => {
                    column_mapping.insert("years_7".to_string(), col_idx);
                }
                h if h.contains("5") && (h.contains("year") || h.contains('y')) => {
                    column_mapping.insert("years_5".to_string(), col_idx);
                }
                h if h.contains("3") && (h.contains("year") || h.contains('y')) => {
                    column_mapping.insert("years_3".to_string(), col_idx);
                }
                h if h.contains("2") && (h.contains("year") || h.contains('y')) => {
                    column_mapping.insert("years_2".to_string(), col_idx);
                }
                h if h.contains("1") && (h.contains("year") || h.contains('y')) => {
                    column_mapping.insert("year_1".to_string(), col_idx);
                }
                h if h.contains("ytd") => {
                    column_mapping.insert("ytd".to_string(), col_idx);
                }
                h if h.contains("6") && (h.contains("month") || h.contains('m')) => {
                    column_mapping.insert("months_6".to_string(), col_idx);
                }
                h if h.contains("3") && (h.contains("month") || h.contains('m')) => {
                    column_mapping.insert("months_3".to_string(), col_idx);
                }
                h if h.contains("1") && (h.contains("month") || h.contains('m')) => {
                    column_mapping.insert("month_1".to_string(), col_idx);
                }
                h if h.contains("21") && (h.contains("day") || h.contains('d')) => {
                    column_mapping.insert("days_21".to_string(), col_idx);
                }
                h if h.contains("14") && (h.contains("day") || h.contains('d')) => {
                    column_mapping.insert("days_14".to_string(), col_idx);
                }
                h if h.contains("7") && (h.contains("day") || h.contains('d')) => {
                    column_mapping.insert("days_7".to_string(), col_idx);
                }
                _ => {}
            }
        }
    }
    column_mapping
}

fn parse_fund_row_with_mapping(
    category: &str,
    range: &Range<Data>,
    row_idx: usize,
    column_mapping: &HashMap<String, usize>,
) -> Option<FundData> {
    let scheme_name = column_mapping
        .get("scheme_name")
        .and_then(|&col_idx| range.get((row_idx, col_idx)))
        .map(|c| c.to_string().trim().to_string())
        .unwrap_or_default();

    if scheme_name.is_empty()
        || scheme_name.to_lowercase().contains("scheme name")
        || scheme_name.to_lowercase().contains("fund name")
        || scheme_name.to_lowercase().contains("to view exit loads")
        || scheme_name == category
        || scheme_name.chars().all(|c| c == '-' || c.is_whitespace())
    {
        return None;
    }

    let get_value = |field: &str| -> f64 {
        parse_float(
            column_mapping
                .get(field)
                .and_then(|&col_idx| range.get((row_idx, col_idx))),
        )
    };

    let get_string = |field: &str| -> String {
        column_mapping
            .get(field)
            .and_then(|&col_idx| range.get((row_idx, col_idx)))
            .map(|c| c.to_string())
            .unwrap_or_default()
    };

    let max_cols = range.width();
    let mut additional_cols = Vec::new();
    let last_standard_col = column_mapping.values().max().cloned().unwrap_or(0);

    for col_idx in (last_standard_col + 1)..max_cols {
        additional_cols.push(parse_float(range.get((row_idx, col_idx))));
    }

    Some(FundData {
        category: category.to_string(),
        scheme_name,
        launch_date: get_string("launch_date"),
        fund_size_apr25: get_value("fund_size_apr25"),
        fund_size_may25: get_value("fund_size_may25"),
        latest_nav: get_value("latest_nav"),
        days_7: get_value("days_7"),
        days_14: get_value("days_14"),
        days_21: get_value("days_21"),
        month_1: get_value("month_1"),
        months_3: get_value("months_3"),
        months_6: get_value("months_6"),
        ytd: get_value("ytd"),
        year_1: get_value("year_1"),
        years_2: get_value("years_2"),
        years_3: get_value("years_3"),
        years_5: get_value("years_5"),
        years_7: get_value("years_7"),
        years_10: get_value("years_10"),
        since_inception: get_value("since_inception"),
        additional_cols,
    })
}

// CORRECTED: This function is now more robust. Instead of a catch-all case,
// it explicitly handles known non-numeric types to avoid incorrect parsing.
fn parse_float(cell: Option<&Data>) -> f64 {
    match cell {
        Some(Data::Float(f)) => *f,
        Some(Data::Int(i)) => *i as f64,
        Some(Data::String(s)) => {
            let cleaned = s
                .trim()
                .replace('%', "")
                .replace(',', "")
                .replace('₹', "")
                .replace("Rs", "");
            cleaned.parse().unwrap_or(0.0)
        }
        // Explicitly handle non-numeric types as 0.0. The previous catch-all
        // could convert these to strings and cause parsing errors.
        Some(Data::Bool(_)) | Some(Data::Error(_)) | Some(Data::Empty) => 0.0,
        // Any other data type from Calamine (like DateTime) will be treated as 0.0,
        // which is safer than attempting to parse it.
        _ => 0.0,
    }
}

fn insert_fund_data(conn: &Connection, funds: Vec<FundData>) -> SqliteResult<usize> {
    let mut inserted = 0;

    for fund in funds {
        let additional_data = serde_json::to_string(&fund.additional_cols).unwrap_or_default();

        conn.execute(
            "INSERT INTO funds (
                category, scheme_name, launch_date, fund_size_apr25, fund_size_may25,
                latest_nav, days_7, days_14, days_21, month_1, months_3, months_6,
                ytd, year_1, years_2, years_3, years_5, years_7, years_10,
                since_inception, additional_data
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
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
                fund.years_5,
                fund.years_7,
                fund.years_10,
                fund.since_inception,
                additional_data
            ],
        )?;
        inserted += 1;
    }

    Ok(inserted)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    println!("Server starting on http://127.0.0.1:8081");

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