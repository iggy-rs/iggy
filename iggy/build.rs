extern crate rmp_serde as rmps;
extern crate serde;
extern crate serde_derive;

use std::error::Error;
use std::fs::File;
use std::io::Write;

use errors_repository::PreprocessedErrorRepositoryEntry;

use crate::data_repository::{DataRepository, SledDb};
use crate::errors_repository::get_or_create;

mod data_repository;
mod errors_repository;

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
    " ".repeat(num_spaces).to_string()
}

struct ErrorEnumVariant {
    snake_case_name: String,
    template: String,
    signature: String,
    converts_from: String,
}

struct ErrorEnum {
    pascal_case_name: String,
    variants: Vec<ErrorEnumVariant>,
}

impl From<PreprocessedErrorRepositoryEntry> for ErrorEnumVariant {
    fn from(value: PreprocessedErrorRepositoryEntry) -> Self {
        Self {
            snake_case_name: value.snake_case_name,
            template: value.template,
            signature: value.signature,
            converts_from: value.converts_from,
        }
    }
}

impl ErrorEnumVariant {
    fn to_code_string(&self) -> String {
        let mut result = String::new();

        result.push_str(&format!(
            "{indent}#[error(\"{}\")]\n",
            self.template,
            indent = get_spaces(4)
        ));
        result.push_str(&format!(
            "{indent}{}",
            &snake_to_pascal_case(&self.snake_case_name),
            indent = get_spaces(4),
        ));
        let signature = &self.signature;
        let converts_from = &self.converts_from;

        match (converts_from.is_empty(), signature.is_empty()) {
            (true, true) => (),
            (true, false) => result.push_str(&format!("({})", signature)),
            (false, true) => result.push_str(&format!("(#[from] {})", converts_from)),
            (false, false) => result.push_str(&format!("({}, {})", converts_from, signature)),
        };

        result
    }
}

impl ErrorEnum {
    fn to_code_string(&self) -> String {
        vec![
            format!("pub enum {name} {{", name = self.pascal_case_name),
            self.variants
                .iter()
                .map(|enum_variant| format!("{},", &enum_variant.to_code_string()))
                .collect::<Vec<String>>()
                .join("\n"),
            "}".to_string(),
        ]
        .join("\n")
    }
}

enum ConversionType {
    AsString,
    AsCode,
    FromCodeAsString,
}

impl ConversionType {
    fn to_match_arm(&self, entry: &PreprocessedErrorRepositoryEntry, num_spaces: usize) -> String {
        match self {
            ConversionType::AsString => {
                format!(
                    "{indent}Error::{}{} => \"{}\",",
                    crate::snake_to_pascal_case(&entry.snake_case_name),
                    entry.signature_wildcard_pattern,
                    entry.snake_case_name,
                    indent = crate::get_spaces(num_spaces),
                )
            }
            ConversionType::AsCode => {
                format!(
                    "{indent}Error::{}{} => {},",
                    crate::snake_to_pascal_case(&entry.snake_case_name),
                    entry.signature_wildcard_pattern,
                    entry.code,
                    indent = crate::get_spaces(num_spaces),
                )
            }
            ConversionType::FromCodeAsString => {
                format!(
                    "{indent}{} => \"{}\",",
                    entry.code,
                    entry.snake_case_name,
                    indent = crate::get_spaces(num_spaces),
                )
            }
        }
    }

    fn create_arms(
        &self,
        preprocessed_error_codes: &Vec<PreprocessedErrorRepositoryEntry>,
        default_value: Option<String>,
    ) -> Vec<String> {
        let mut result: Vec<String> = vec![];

        for (idx, preprocessed_error_code) in preprocessed_error_codes.iter().enumerate() {
            let num_spaces = if idx == 0 { 0 } else { 12 };

            let next = self.to_match_arm(&preprocessed_error_code, num_spaces);
            result.push(next);
        }

        if let Some(dv) = default_value {
            let indent = get_spaces(12);
            result.push(format!("{}_ => \"{}\"", indent, dv));
        }

        result
    }
}

struct MatchConversionFunction {
    name: String,
    parameter_list: String,
    return_type: String,
    match_arms: Vec<String>,
    match_on: String,
}

impl MatchConversionFunction {
    fn to_code_string(&self) -> String {
        let lines = vec![
            format!(
                "{indent}{signature} {{",
                indent = get_spaces(4),
                signature = format!(
                    "pub fn {name}({parameter_list}) -> {return_type}",
                    name = self.name,
                    parameter_list = self.parameter_list,
                    return_type = self.return_type
                )
            ),
            format!(
                "{indent}match {match_on} {{",
                indent = get_spaces(8),
                match_on = self.match_on
            ),
            format!(
                "{indent}{as_code_arms}",
                as_code_arms = self.match_arms.join("\n"),
                indent = get_spaces(12)
            ),
            format!("{indent}}}", indent = get_spaces(8)),
            format!("{indent}}}\n", indent = get_spaces(4)),
        ];

        lines.join("\n")
    }
}

const GENERATED_ERRORS_PATH: &'static str = "./src/errors/generated_code/errors.rs";

fn main() -> Result<(), Box<dyn Error>> {
    let errors_db: SledDb = get_or_create().expect("Could not load errors database");

    let entries: Vec<PreprocessedErrorRepositoryEntry> = errors_db
        .fetch_all()?
        .into_iter()
        .map(PreprocessedErrorRepositoryEntry::from)
        .collect();

    let as_string_conversion = MatchConversionFunction {
        name: "as_string".to_string(),
        parameter_list: "&self".to_string(),
        return_type: "&'static str".to_string(),
        match_on: "self".to_string(),
        match_arms: ConversionType::AsString.create_arms(&entries, None),
    };

    let as_code_conversion = MatchConversionFunction {
        name: "as_code".to_string(),
        parameter_list: "&self".to_string(),
        return_type: "u32".to_string(),
        match_on: "self".to_string(),
        match_arms: ConversionType::AsCode.create_arms(&entries, None),
    };

    let from_code_as_string_conversion = MatchConversionFunction {
        name: "from_code_as_string".to_string(),
        parameter_list: "code: u32".to_string(),
        return_type: "&'static str".to_string(),
        match_on: "code".to_string(),
        match_arms: ConversionType::FromCodeAsString
            .create_arms(&entries, Some("error".to_string())),
    };

    let lines = vec![
        "// This file was generated by build.rs. Do not edit by hand.".to_string(),
        "// If you need to add a new Error variant follow the instructions".to_string(),
        "// in the README.md in the errors package to add a new Error variant".to_string(),
        "// to the provided database and then rebuild the project using `cargo build`.".to_string(),
        "// For further details see the mentioned README.".to_string(),
        "use quinn::{ConnectionError, ReadError, ReadToEndError, WriteError};".to_string(),
        "use std::array::TryFromSliceError;".to_string(),
        "use std::net::AddrParseError;".to_string(),
        "use std::num::ParseIntError;".to_string(),
        "use std::str::Utf8Error;".to_string(),
        "use thiserror::Error;".to_string(),
        "use tokio::io;\n".to_string(),
        "#[derive(Debug, Error)]".to_string(),
        ErrorEnum {
            pascal_case_name: "Error".to_string(),
            variants: entries.into_iter().map(ErrorEnumVariant::from).collect(),
        }
        .to_code_string(),
        "impl Error {".to_string(),
        as_code_conversion.to_code_string(),
        from_code_as_string_conversion.to_code_string(),
        as_string_conversion.to_code_string(),
        "}".to_string(),
    ];

    let output = lines.join("\n");
    let mut generated_code_file = File::create(GENERATED_ERRORS_PATH)?;
    writeln!(generated_code_file, "{}", output)?;

    Ok(())
}
