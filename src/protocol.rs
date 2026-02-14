#[derive(Debug, PartialEq, Clone)]
pub enum RespValue {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RespValue>),
    Null, // Represents $-1\r\n
    Integer(i64),
}

pub fn parse_resp(input: &str) -> Result<RespValue, String> {
    // We convert our string into an iterator of lines.
    // .peekable() lets us look at the next item without consuming it.
    let mut lines = input.split("\r\n").peekable();
    parse_recursive(&mut lines)
}

// We create a helper function to handle the recursion
fn parse_recursive(
    lines: &mut std::iter::Peekable<std::str::Split<&str>>,
) -> Result<RespValue, String> {
    let mut line = lines.next().ok_or("Empty input")?;
    while line.is_empty() {
        line = lines.next().ok_or("Empty input")?;
    }
    let prefix = line.chars().next().ok_or("Missing prefix")?;

    match prefix {
        '+' => Ok(RespValue::SimpleString(line[1..].to_string())),
        '$' => {
            let _len: i64 = line[1..].parse().map_err(|_| "Invalid length")?;
            if _len == -1 {
                return Ok(RespValue::Null);
            }
            if _len < 0 {
                return Err("Invalid negative length for bulk string".to_string());
            }

            let data = lines.next().ok_or("Missing bulk data")?;
            if data.len() != _len as usize {
                return Err("Bulk string length does not match with provided length".to_string());
            }
            Ok(RespValue::BulkString(data.to_string()))
        }
        '*' => {
            // 1. Parse number of elements
            let count: usize = line[1..].parse().map_err(|_| "Invalid array length")?;
            let mut items = Vec::with_capacity(count);

            // 2. Recursively parse each element
            for _ in 0..count {
                items.push(parse_recursive(lines)?);
            }

            Ok(RespValue::Array(items))
        }
        _ => Err(format!("Unknown prefix: {}", prefix)),
    }
}

impl RespValue {
    pub fn encode(&self) -> String {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s),
            RespValue::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            RespValue::Array(elements) => {
                let mut out = format!("*{}\r\n", elements.len());
                for el in elements {
                    out.push_str(&el.encode());
                }
                out
            }
            RespValue::Null => "$-1\r\n".to_string(),
            RespValue::Integer(x) => format!(":{}\r\n", x),
        }
    }
}
