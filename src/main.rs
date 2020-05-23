use structopt::StructOpt;
use reqwest::{get};
use scraper::{Selector, Html};
use regex::Regex;
use std::fmt;

const WIKI_TABLE_ELEMENT: &'static str = "table.wikitable";
const WIKI_DATABASE_FILE: &'static str = "wikiDatabase.db";

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct Command {
    #[structopt(
        about = "The url to pull information from",
        help = "USAGE: wtd https://example.com",
    )]
    url: String,
    #[structopt(
        about = "optional param for specifying the database to use. Defaults to wikiDatabase.db",
        help = "USAGE: wtd https://example.com myDataBase.db",
    )]
    file_name: Option<String>,
}

#[derive(PartialEq)]
#[derive(Debug)]
enum SqlTypes {
    INTEGER,
    REAL,
    NUMERIC,
    TEXT,
}

#[derive(Debug)]
enum WtdError {
    TableNotFound,
    TableBodyNotFound,
    HeaderAndTypesAmountMismatch,
    TableHeaderNotFound,
    UnableToReachPage,
    UnsuccessFulRequest,
    ResponseBodyError,
    Sqlite3Connection,
    Sqlite3InsertError,
    CreateTableError,
}

impl fmt::Display for WtdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            WtdError::TableBodyNotFound => f.write_str("Table Body not found"),
            WtdError::TableNotFound => f.write_str("Table element not found"),
            WtdError::HeaderAndTypesAmountMismatch => f.write_str("Headers and types must be the same length"),
            WtdError::Sqlite3Connection => f.write_str("Failed to insert into sqlite3 database"),
            WtdError::Sqlite3InsertError => f.write_str("Failed to insert data into database"),
            WtdError::CreateTableError => f.write_str("Failed to create table"),
            WtdError::UnableToReachPage => f.write_str("Unable to reach page"),
            WtdError::ResponseBodyError => f.write_str("Failed to get body from response"),
            WtdError::UnsuccessFulRequest => f.write_str("Request did not respond with a 200"),
            WtdError::TableHeaderNotFound => f.write_str("Table header was not found"),
        }
    }
}

impl std::error::Error for WtdError {
    fn description(&self) -> &str {
        match *self {
            WtdError::TableNotFound => "Table not found error",
            WtdError::TableBodyNotFound => "Table body not found error",
            WtdError::HeaderAndTypesAmountMismatch => "Header and Types Amount Mismatch error",
            WtdError::Sqlite3Connection => "Sqlite3 Connection Error",
            WtdError::Sqlite3InsertError => "Sqlite3 Insert Error",
            WtdError::ResponseBodyError => "Response Body Error",
            WtdError::UnableToReachPage => "Unable to reach page Error",
            WtdError::UnsuccessFulRequest => "Non 200 response",
            WtdError::TableHeaderNotFound => "Table header not found error",
            WtdError::CreateTableError => "Create Table error",
        }
    }
}

// So that .to_string() works on this particular Enum
impl fmt::Display for SqlTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[tokio::main]
async fn main() -> Result<(), WtdError> {
    let args = Command::from_args();
    let database_name = args.file_name.unwrap_or(String::from(WIKI_DATABASE_FILE));
    match get_wiki_page(args.url, database_name).await {
        Ok(()) => {println!("Success!"); Ok(())},
        Err(err) => { eprintln!("Error: {}", err); std::process::exit(1)},
    }
}

async fn get_wiki_page(url: String, database_name: String) -> Result<(), WtdError> {
    match get(&url).await {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.text().await {
                    Ok(body) => extract_data(&body, &database_name),
                    Err(_) => Err(WtdError::ResponseBodyError)
                }
            } else {
                Err(WtdError::UnsuccessFulRequest)
            }
        },
        Err(_) => Err(WtdError::UnableToReachPage)
    }
}

fn extract_data(body: &str, database_name: &str) -> Result<(), WtdError> {
    match get_table_headers_and_types_from_html(body) {
        Ok(headers) => {
            match get_page_title_from_html(body).get(0) {
                Some(table_name) => {
                    create_table(table_name, headers, database_name).unwrap();
                    insert_rows(table_name, body, database_name)
                },
                None => Err(WtdError::TableHeaderNotFound),
            }
        },
        Err(err) => Err(err)
    }
}

/// Returns a vector containing the title from a given html string
/// Returns a vec of strings because it's possible that the selector finds more than one h1 tag
fn get_page_title_from_html(body: &str) -> Vec<String> {
    let fragment = Html::parse_fragment(body);
    let selector = Selector::parse("h1").unwrap();
    fragment.select(&selector).map(|e| {e.inner_html()}).collect()
}

/// Returns a Result with a vector containing table headers from a given html string
fn get_table_headers_and_types_from_html(body: &str) -> Result<Vec<(String, String)>, WtdError> {
    match get_table_header_names(&body) {
        Ok(table_headers) => {
            let mut table_header_types: Vec<String> = get_table_header_types(body, table_headers.len());
            table_header_types.reverse(); // TODO: I'm doing this because I'm using pop
            if table_headers.len() == table_header_types.len() {
                Ok(table_headers.iter()
                    .map(|column| (String::from(column), table_header_types.pop().unwrap()))
                    .collect())
            } else {
                Err(WtdError::HeaderAndTypesAmountMismatch)
            }
        },
        Err(err) => Err(err)
    }
}

#[test]
fn test_get_table_headers_and_types_from_html() {
    let html = std::fs::read_to_string("fixtures/samplepage.html").unwrap();
    let headers_and_types = get_table_headers_and_types_from_html(&html).unwrap();

    let expected = vec!(
        (String::from("Flag"), String::from("TEXT")),
        (String::from("Member state"), String::from("TEXT")),
        (String::from("Date of admission"), String::from("TEXT")),
        (String::from("See also"), String::from("TEXT"))
    );
    assert_eq!(expected, headers_and_types);
}

/// Gets the types for each column in a table
fn get_table_header_types(body: &str, num: usize) -> Vec<String> {
    let all_data = get_table_cells(body);
    let first_n_vec: Vec<String> = all_data[0..num].to_vec();
    first_n_vec.iter().map(|d| derive_type(d).to_string()).collect()
}


// TODO: Depricate this method, get the headers only
fn get_table_cells(body: &str) -> Vec<String> {
    let fragment = Html::parse_fragment(body);
    let table_selector = Selector::parse(WIKI_TABLE_ELEMENT).unwrap();
    let table = fragment.select(&table_selector).next().unwrap();

    let table_data_selector = Selector::parse("td").unwrap();
    table.select(&table_data_selector)
        .map(|e| e.inner_html())
        .collect()
}

fn get_raw_table_rows(body: &str) -> Result<Vec<Vec<String>>, WtdError> {
    let fragment = Html::parse_fragment(body);
    let table_selector = Selector::parse(WIKI_TABLE_ELEMENT).unwrap();
    let table_body_selector = Selector::parse("tbody").unwrap();
    let table_row_selector = Selector::parse("tr").unwrap();
    let table_data_selector = Selector::parse("td,th").unwrap(); // Sometimes the cells are headers

    match fragment.select(&table_selector).next() {
        Some(table) => {
            match table.select(&table_body_selector).next() {
                Some(tbody) => {
                    Ok(tbody.select(&table_row_selector).skip(1).map(|r| {
                        r.select(&table_data_selector).map(|td| td.inner_html()).collect::<Vec<String>>()
                    }).collect())
                },
                None => Err(WtdError::TableBodyNotFound)
            }
        },
        None => Err(WtdError::TableNotFound)
    }
}

// TODO: If the row is empty, insert raw if possible
fn clean_row(row: Vec<String>) -> Vec<String> {
    row.iter()
        .map(|e| {
            let removed_tags = remove_html_tags(e);
            let removed_apostrophe = remove_apostrophe(&removed_tags);
            let removed_citations = remove_wiki_citation_links(&removed_apostrophe);
            let int_or_double = clean_integer_or_double_string(&removed_citations);
            let trimmed = int_or_double.trim();
            match int_or_double.parse::<i64>() {
                Ok(_) => return String::from(int_or_double),
                Err(_) => {},
            };
            match int_or_double.parse::<f64>() {
                Ok(_) => return String::from(int_or_double),
                Err(_) => {},
            };
            format!("'{}'", trimmed)
        }).collect()
}

#[test]
fn test_clean_row() {
    let row: Vec<String> = vec!(
        String::from("187"),
        String::from(r###"<span class="flagicon"><img alt="" src="//upload.wikimedia.org/wikipedia/commons/thumb/2/2e/Flag_of_the_Marshall_Islands.svg/23px-Flag_of_the_Marshall_Islands.svg.png" decoding="async" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/2/2e/Flag_of_the_Marshall_Islands.svg/35px-Flag_of_the_Marshall_Islands.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/2/2e/Flag_of_the_Marshall_Islands.svg/46px-Flag_of_the_Marshall_Islands.svg.png 2x" data-file-width="570" data-file-height="300" width="23" height="12"></span>&nbsp;<a href="/wiki/Demographics_of_Marshall_Islands" class="mw-redirect" title="Demographics of Marshall Islands">Marshall Islands</a>"###),
        String::from("55,500"),
        String::from(r###"<span data-sort-value="6996712478476410351♠" style="display:none"></span>0.000712%"###),
        String::from(r###"<span data-sort-value="000000002018-07-01-0000" style="white-space:nowrap">1 Jul 2018</span>"###),
        String::from(r###"National annual estimate<sup id="cite_ref-auto1_104-6" class="reference"><a href="#cite_note-auto1-104">[90]</a></sup>"###),
    );
    let expected = vec!(
        String::from("187"),
        String::from("'Marshall Islands'"),
        String::from("55500"),
        String::from("0.000712"),
        String::from("'1 Jul 2018'"),
        String::from("'National annual estimate'"),
    );
    assert_eq!(clean_row(row), expected);
}

// Helper method to remove apostrophes because we use them for quoting the inserts
fn remove_apostrophe(s: &str) -> String {
    String::from(str::replace(s, "'", "''"))
}

/// Derives the type of the string
fn derive_type(sample_datum: &str) -> SqlTypes {
    // TODO: This needs to parse out dates
    let html_cleaned_data = remove_html_tags(sample_datum);
    let removed_citations = remove_wiki_citation_links(&html_cleaned_data);
    let cleaned = clean_integer_or_double_string(&removed_citations);
    // Ignoring the result since we only care about the type
    // Ignoring the error since we expect all but one of the cases to fail
    match cleaned.parse::<i64>() {
        Ok(_) => return SqlTypes::INTEGER,
        Err(_) => {},
    };
    match cleaned.parse::<f64>() {
        Ok(_) => return SqlTypes::REAL,
        Err(_) => {},
    };
    match removed_citations.parse::<bool>() {
        Ok(_) => return SqlTypes::NUMERIC,
        Err(_) => {},
    };
    return SqlTypes::TEXT;
}

#[test]
fn test_derive_type() {
    // Simple test cases
    let int = "1";
    assert_eq!(derive_type(int), SqlTypes::INTEGER);

    let double = "10.1";
    assert_eq!(derive_type(double), SqlTypes::REAL);

    let boolean = "true";
    assert_eq!(derive_type(boolean), SqlTypes::NUMERIC);

    let text = "some text";
    assert_eq!(derive_type(text), SqlTypes::TEXT);

    // With extra html and other characters
    let flag_with_tags = r###"<span class="flagicon"><img alt="" src="//upload.wikimedia.org/wikipedia/commons/thumb/f/fa/Flag_of_the_People%27s_Republic_of_China.svg/23px-Flag_of_the_People%27s_Republic_of_China.svg.png" decoding="async" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/f/fa/Flag_of_the_People%27s_Republic_of_China.svg/35px-Flag_of_the_People%27s_Republic_of_China.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/f/fa/Flag_of_the_People%27s_Republic_of_China.svg/45px-Flag_of_the_People%27s_Republic_of_China.svg.png 2x" data-file-width="900" data-file-height="600" width="23" height="15"></span>&nbsp;<a href="/wiki/Demographics_of_China" title="Demographics of China">China</a><sup id="cite_ref-4" class="reference"><a href="#cite_note-4">[b]</a></sup>"###;
    assert_eq!(derive_type(flag_with_tags), SqlTypes::TEXT);

    let large_number = "1,402,843,280";
    assert_eq!(derive_type(large_number), SqlTypes::INTEGER);

    let percentage_with_span = r###"<span data-sort-value="7001180118809521761♠" style="display:none"></span>18.0%"###;
    assert_eq!(derive_type(percentage_with_span), SqlTypes::REAL);

    // TODO: Until we decide on a uniform date format dates are strings
    let date_string_with_span = r###"<span data-sort-value="000000002020-05-28-0000" style="white-space:nowrap">28 May 2020</span>"###;
    assert_eq!(derive_type(date_string_with_span), SqlTypes::TEXT);

    let text_with_citations = r###"National population clock<sup id="cite_ref-7" class="reference"><a href="#cite_note-7">[4]</a></sup>"###;
    assert_eq!(derive_type(text_with_citations), SqlTypes::TEXT);
}

/// Method for removing html tags
fn remove_html_tags(s: &str) -> String {
    let cleaned = str::replace(s, "&nbsp;", " ");
    let replace_br = str::replace(&cleaned, "<br>", " ");
    let re_html_tags = Regex::new(r"(<.*?>)").unwrap();
    String::from(re_html_tags.replace_all(&replace_br, "").trim())
}

#[test]
fn test_remove_html_tags() {
    let flag_and_country_html = r##"<span class="flagicon"><img alt="" src="//upload.wikimedia.org/wikipedia/commons/thumb/f/fa/Flag_of_the_People%27s_Republic_of_China.svg/23px-Flag_of_the_People%27s_Republic_of_China.svg.png" decoding="async" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/f/fa/Flag_of_the_People%27s_Republic_of_China.svg/35px-Flag_of_the_People%27s_Republic_of_China.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/f/fa/Flag_of_the_People%27s_Republic_of_China.svg/45px-Flag_of_the_People%27s_Republic_of_China.svg.png 2x" data-file-width="900" data-file-height="600" width="23" height="15"></span>&nbsp;<a href="/wiki/Demographics_of_China" title="Demographics of China">China</a><sup id="cite_ref-4" class="reference"><a href="#cite_note-4">[b]</a></sup>"##;
    assert_eq!(remove_html_tags(&flag_and_country_html), "China[b]");
}

/// Method for removing wiki citations
fn remove_wiki_citation_links(s: &str) -> String {
    let re_citation = Regex::new(r"(\[[a-zA-Z0-9]+\])").unwrap();
    re_citation.replace_all(s, "").into_owned()
}

#[test]
fn test_remove_wiki_citation_links() {
    let lowercase_citation = "China[b]";
    assert_eq!(remove_wiki_citation_links(&lowercase_citation), "China");

    let uppercase_citation = "China[B]";
    assert_eq!(remove_wiki_citation_links(&uppercase_citation), "China");

    let numbered_citation = "China[1]";
    assert_eq!(remove_wiki_citation_links(&numbered_citation), "China");

    let larger_number = "China[1000]";
    assert_eq!(remove_wiki_citation_links(&larger_number), "China");
}

fn clean_integer_or_double_string(i: &str) -> String {
    let clean_int = str::replace(i, ",", "");
    str::replace(&clean_int, "%", "")
}

#[test]
fn test_clean_integer_or_double_string() {
    // Removing spans
    let percent_with_span = "18.0%";
    assert_eq!(clean_integer_or_double_string(percent_with_span), "18.0");

    // Removing commas
    let number_with_commas = "1,402,843,280";
    assert_eq!(clean_integer_or_double_string(&number_with_commas), "1402843280");
}

fn get_table_header_names(body: &str) -> Result<Vec<String>, WtdError> {
    let fragment = Html::parse_fragment(body);
    let table_selector = Selector::parse(WIKI_TABLE_ELEMENT).unwrap();
    let table_body_selector = Selector::parse("tbody").unwrap();
    let table_row_selector = Selector::parse("tr").unwrap();
    let table_header_selector = Selector::parse("th").unwrap();

    match fragment.select(&table_selector).next() {
        Some(table) => {
            match table.select(&table_body_selector).next() {
                Some(tbody) => {
                    let rows: Vec<Vec<String>> = tbody.select(&table_row_selector).map(|r| {
                        r.select(&table_header_selector).map(|td| td.inner_html()).collect::<Vec<String>>()
                    }).collect();
                    Ok(rows.get(0).unwrap().iter().map(|s| clean_header_string(String::from(s))).collect())
                },
                None => Err(WtdError::TableBodyNotFound)
            }
        },
        None => Err(WtdError::TableNotFound),
    }
}

#[test]
fn test_get_table_header_names() {
    let plain_table = std::fs::read_to_string("fixtures/tableHeaders.html").unwrap();
    let expected: Vec<String> = vec!(
        String::from("Rank"),
        String::from("Country (or dependent territory)"),
        String::from("Population"),
        String::from("% of world population"),
        String::from("Date"),
        String::from("Source"),
    );
    assert_eq!(get_table_header_names(&plain_table).unwrap(), expected);
    
    let ths_inside_non_header_rows = std::fs::read_to_string("fixtures/memberStatesTable.html").unwrap();
    let expected: Vec<String> = vec!(
        String::from("Flag"),
        String::from("Member state"),
        String::from("Date of admission"),
        String::from("See also"),
    );
    assert_eq!(get_table_header_names(&ths_inside_non_header_rows).unwrap(), expected);
}

/// Removes unwanted chars and whitespace from strings
fn clean_header_string(header: String) -> String {
    let without_tags = remove_html_tags(&header);
    let clean_header = remove_wiki_citation_links(&without_tags);
    String::from(clean_header.trim())
}

/// Creating the table from the headers and header type tuples
fn create_table(table_name: &str, headers_and_types: Vec<(String, String)>, database_name: &str) -> Result<(), WtdError> {
    match sqlite::open(database_name) {
        Ok(connection) => {
            let table_columns_vec: Vec<String> = headers_and_types.iter().map(|vec| format!("'{}' {},", vec.0, vec.1)).collect();
            let mut table_columns = table_columns_vec.join(" ");
            table_columns.pop(); // Removing the last commacode: i32
            let create_table_string = format!("CREATE TABLE '{}' ({});", str::replace(table_name, " ", "_"), table_columns);
            match connection.execute(&create_table_string) {
                Ok(()) => { println!("Successfully Created table"); Ok(()) },
                Err(err) => {
                    eprintln!("Error: Failed to create table: {}, Statement: {}", err, &create_table_string);
                    Err(WtdError::CreateTableError)
                },
            }
        },
        Err(_) => Err(WtdError::Sqlite3Connection),
    }
}

/// Inserts rows into the database
fn insert_rows(table_name: &str, body: &str, database_name: &str) -> Result<(), WtdError> {
    match sqlite::open(database_name) {
        Ok(connection) => {
            match create_insert_statement(table_name, body) {
                Ok(insert_statement) => {
                    println!("Inserting rows");
                    match connection.execute(&insert_statement) {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            eprintln!("Error: Failed to insert into table: {}\nSQL Statement: {}", err, &insert_statement);
                            Err(WtdError::Sqlite3InsertError)
                        },
                    }
                },
                Err(err) => Err(err)
            }
        },
        Err(err) => {
            eprintln!("Error: Could not connect to sqlite3 databse, {}", err);
            Err(WtdError::Sqlite3Connection)
        },
    }
}

/// Creates the insert statement
fn create_insert_statement(table_name: &str, body: &str) -> Result<String, WtdError> {
    match get_raw_table_rows(body) {
        Ok(rows) => {
            let mut insert_statement = String::new();
            for r in rows {
                let cleaned_row = clean_row(r);
                if !cleaned_row.is_empty() {
                    if insert_statement.is_empty() {
                        insert_statement = format!("INSERT into {} VALUES ({})", str::replace(table_name, " ", "_"), cleaned_row.join(", "));
                    } else {
                        insert_statement = format!("{}, ({})", insert_statement, cleaned_row.join(", "));
                    }
                }
            }
            Ok(format!("{};", insert_statement))
        },
        Err(err) => Err(err)
    }
}
