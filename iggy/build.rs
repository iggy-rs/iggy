use std::error::Error;
use std::fs::File;
use std::io::Write;

use sqlx::{Pool, Sqlite, SqlitePool};

#[derive(sqlx::FromRow, Debug)]
pub struct ErrorCode {
    pub id: u32,
    pub name: String,
    pub code: u32,
    pub template: String,
    pub signature: String,
    pub converts_from: String,
}

pub struct PreprocessedErrorCode {
    pub id: u32,
    pub name: String,
    pub code: u32,
    pub template: String,
    pub signature: String,
    pub converts_from: String,
    pub pascal_case_name: String,
    pub signature_wildcard_pattern: String,
}

impl From<ErrorCode> for PreprocessedErrorCode {
    fn from(error_code: ErrorCode) -> Self {
        PreprocessedErrorCode {
            id: error_code.id,
            name: error_code.name.clone(),
            code: error_code.code,
            template: error_code.template.clone(),
            signature: error_code.signature.clone(),
            converts_from: error_code.converts_from.clone(),
            pascal_case_name: snake_to_pascal_case(&error_code.name),
            signature_wildcard_pattern: {
                to_wildcard_pattern(&error_code.build_data_value())
            },
        }
    }
}

fn snake_to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first_char) => first_char.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect::<Vec<String>>()
        .join("")
}



fn get_spaces(num_spaces: usize) -> String {
    if num_spaces == 0 {
        "".to_string()
    } else {
        " ".repeat(num_spaces).to_string()
    }
}



fn create_error_enum_variant_item(preprocessed_error_code: &PreprocessedErrorCode) -> String {
    let mut result = String::new();

    result.push_str(&format!(
        "    #[error(\"{}\")]\n",
        preprocessed_error_code.template
    ));
    result.push_str(&format!(
        "    {}",
        &snake_to_pascal_case(&preprocessed_error_code.name)
    ));
    let sig = &preprocessed_error_code.signature;
    let converts_from = &preprocessed_error_code.converts_from;

    match (converts_from.is_empty(), sig.is_empty()) {
        (true, true) => (),
        (true, false) => result.push_str(&format!("({})", sig)),
        (false, true) => result.push_str(&format!("(#[from] {})", converts_from)),
        (false, false) => result.push_str(&format!("({}, {})", converts_from, sig)),
    };

    result
}


enum ArmType {
    AsString,
    AsCode,
    FromCodeAsString,
}


fn create_error_enum_body(preprocessed_error_codes: &Vec<PreprocessedErrorCode>) -> String {
    let mut result = String::new();
    let n = preprocessed_error_codes.len();

    for (idx, preprocessed_error_code) in preprocessed_error_codes.iter().enumerate() {
        result.push_str(&format!(
            "{},",
            &create_error_enum_variant_item(&preprocessed_error_code)
        ));

        if idx < n - 1 {
            result.push_str("\n");
        }
    }

    result
}

// Create the `String` representing the arms for the given `ArmType`
// based on the given `PreprocessedErrorCode`.
fn create_arms(
    preprocessed_error_codes: &Vec<PreprocessedErrorCode>,
    arm_type: ArmType,
    default_value: Option<String>,
) -> String {
    let mut result = String::new();
    let n = preprocessed_error_codes.len();

    for (idx, preprocessed_error_code) in preprocessed_error_codes.iter().enumerate() {
        let num_spaces = if idx == 0 { 0 } else { 12 };
        let next = match arm_type {
            ArmType::AsString => preprocessed_error_code.create_as_string_arm(num_spaces),
            ArmType::AsCode => preprocessed_error_code.create_code_arm(num_spaces),
            ArmType::FromCodeAsString => preprocessed_error_code.create_from_code_as_string_arm(num_spaces),
        };

        result.push_str(&next);

        if idx < n - 1 {
            result.push_str("\n");
        }
    }

    if let Some(dv) = default_value {
        let indent = get_spaces(12);
        result.push_str(&format!("\n{}_ => \"{}\"", indent, dv));
    }

    result
}

impl ErrorCode {
    fn build_data_value(&self) -> String {
        match (self.converts_from.is_empty(), self.signature.is_empty()) {
            (true, true) => String::new(),
            (true, false) => format!("({})", self.signature),
            (false, true) => format!("(#[from] {})", self.converts_from),
            (false, false) => format!("({}, {})", self.converts_from, self.signature),
        }
    }
}

impl PreprocessedErrorCode {
    fn create_code_arm(&self, num_spaces: usize) -> String {
        format!(
            "{indent}Error::{}{} => {},",
            snake_to_pascal_case(&self.name),
            self.signature_wildcard_pattern,
            self.code,
            indent = get_spaces(num_spaces),
        )
    }

    fn create_from_code_as_string_arm(&self, num_spaces: usize) -> String {
        format!(
            "{indent}{} => \"{}\",",
            self.code,
            self.name,
            indent = get_spaces(num_spaces),
        )
    }

    fn create_as_string_arm(&self, num_spaces: usize) -> String {
        format!(
            "{indent}Error::{}{} => \"{}\",",
            snake_to_pascal_case(&self.name),
            self.signature_wildcard_pattern,
            self.name,
            indent = get_spaces(num_spaces),
        )
    }
}

fn to_wildcard_pattern(s: &str) -> String {
    if s.is_empty() {
        String::new()
    } else {
        let n = s.chars().filter(|&c| c == ',').count();
        format!("({})", vec!["_"; n + 1].join(", "))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let conn: Pool<Sqlite> =
        SqlitePool::connect("sqlite:./src/errors/error_table_db/errors.db").await?;

    let error_codes: Vec<PreprocessedErrorCode> =
        sqlx::query_as::<_, ErrorCode>("SELECT * FROM error_codes")
            .fetch_all(&conn)
            .await?
            .into_iter()
            .map(PreprocessedErrorCode::from)
            .collect();

    let mut file =
        File::create("./src/errors/generated_code/errors.rs")?;

    let as_code_arms = create_arms(
        &error_codes,
        ArmType::AsCode,
        None);
    let from_code_as_string_arms = create_arms(
        &error_codes,
        ArmType::FromCodeAsString,
        Some("error".to_string()),
    );
    let as_string_arms = create_arms(
        &error_codes,
        ArmType::AsString,
        None);

    let mut result = String::new();
    // @formatter:off
    result.push_str("// This file was generated by build.rs. Do not edit by hand.\n");
    result.push_str("// If you need to add a new Error variant follow the instructions\n");
    result.push_str("// in the README.md in the errors package to add a new Error variant\n");
    result.push_str("// to the provided database and then rebuild the project using `cargo build`.\n");
    result.push_str("// For further details see the mentioned README.\n");
    result.push_str("use quinn::{ConnectionError, ReadError, ReadToEndError, WriteError};\n");
    result.push_str("use std::array::TryFromSliceError;\n");
    result.push_str("use std::net::AddrParseError;\n");
    result.push_str("use std::num::ParseIntError;\n");
    result.push_str("use std::str::Utf8Error;\n");
    result.push_str("use thiserror::Error;\n");
    result.push_str("use tokio::io;\n\n");
    result.push_str("#[derive(Debug, Error)]\n");
    result.push_str("pub enum Error {\n");
    result.push_str(&create_error_enum_body(&error_codes));
    result.push_str("}\n\n");
    result.push_str("impl Error {\n");
    result.push_str(&format!("{indent}pub fn as_code(&self) -> u32 {{\n", indent=get_spaces(4)));
    result.push_str(&format!("{indent}match self {{\n", indent=get_spaces(8)));
    result.push_str(&format!("{indent}{as_code_arms}\n", as_code_arms = as_code_arms, indent=get_spaces(12)));
    result.push_str(&format!("{indent}}}\n", indent=get_spaces(8)));
    result.push_str(&format!("{indent}}}\n\n", indent=get_spaces(4)));
    result.push_str(&format!("{indent}pub fn from_code_as_string(code: u32) -> &'static str {{\n", indent=get_spaces(4)));
    result.push_str(&format!("{indent}match code {{\n", indent=get_spaces(8)));
    result.push_str(&format!("{indent}{from_code_as_string_arms}\n", indent=get_spaces(8), from_code_as_string_arms = from_code_as_string_arms));
    result.push_str(&format!("{indent}}}\n", indent=get_spaces(8)));
    result.push_str(&format!("{indent}}}\n\n", indent=get_spaces(4)));
    result.push_str(&format!("{indent}pub fn as_string(&self) -> &'static str {{\n", indent=get_spaces(4)));
    result.push_str(&format!("{indent}match self {{\n", indent=get_spaces(8)));
    result.push_str(&format!("{indent}{as_string_arms}\n", as_string_arms = as_string_arms, indent=get_spaces(12)));
    result.push_str(&format!("{indent}}}\n", indent=get_spaces(8))); // 8
    result.push_str(&format!("{indent}}}\n", indent=get_spaces(4))); // 4
    result.push_str("}");
    // @formatter:on
    writeln!(file, "{}", result)?;

    Ok(())
}
