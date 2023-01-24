use std::str::FromStr;

use uuid::Uuid;

pub fn get_uuid(value: &serde_json::Value, key: &str) -> Option<Uuid> {
    value
        .as_object()
        .and_then(|x| x.get(key))
        .and_then(|x| x.as_str())
        .and_then(|x| Uuid::from_str(x).ok())
}
