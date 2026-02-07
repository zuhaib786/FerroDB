use FerroDB::protocol::*;
#[test]
fn test_parse_simple_string() {
    let input = "+OK\r\n";
    let result = parse_resp(input).unwrap();

    assert_eq!(result, RespValue::SimpleString("OK".to_string()));
}
#[test]
fn test_parse_bulk_string() {
    let input = "$5\r\nhello\r\n";
    let result = parse_resp(input).unwrap();
    assert_eq!(result, RespValue::BulkString("hello".to_string()));
}
#[test]
fn test_parse_array() {
    let input = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let result = parse_resp(input).unwrap();
    let expected = RespValue::Array(vec![
        RespValue::BulkString("GET".to_string()),
        RespValue::BulkString("key".to_string()),
    ]);
    assert_eq!(result, expected);
}
#[test]
fn test_encode_simple_string() {
    let value = RespValue::SimpleString("OK".to_string());
    assert_eq!(value.encode(), "+OK\r\n");
}

#[test]
fn test_encode_bulk_string() {
    let value = RespValue::BulkString("hello".to_string());
    assert_eq!(value.encode(), "$5\r\nhello\r\n");
}

#[test]
fn test_encode_null() {
    assert_eq!(RespValue::Null.encode(), "$-1\r\n");
}

#[test]
fn test_encode_array() {
    let value = RespValue::Array(vec![
        RespValue::BulkString("GET".to_string()),
        RespValue::BulkString("key".to_string()),
    ]);
    assert_eq!(value.encode(), "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
}

#[test]
fn test_roundtrip() {
    // Parse then encode should give us back the original
    let original = "*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n";
    let parsed = parse_resp(original).unwrap();
    let encoded = parsed.encode();
    assert_eq!(encoded, original);
}
#[test]
fn test_encode_integer() {
    let value = RespValue::Integer(42);
    assert_eq!(value.encode(), ":42\r\n");

    let zero = RespValue::Integer(0);
    assert_eq!(zero.encode(), ":0\r\n");

    let negative = RespValue::Integer(-10);
    assert_eq!(negative.encode(), ":-10\r\n");
}
