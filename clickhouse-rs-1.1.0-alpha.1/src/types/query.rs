use std::collections::HashMap;

use crate::types::{SettingType, SettingValue};

#[derive(Clone, Debug)]
pub struct Query {
    sql: String,
    id: String,
    settings: HashMap<String, SettingValue>,
}

impl Query {
    pub fn new(sql: impl AsRef<str>) -> Self {
        Self {
            sql: sql.as_ref().to_string(),
            id: "".to_string(),
            settings: HashMap::new(),
        }
    }

    pub fn id(self, id: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().to_string(),
            ..self
        }
    }

    /// Per-query setting, overrides the connection-level setting with the same
    /// name for this query only.
    pub fn with_setting<V>(mut self, name: &str, value: V, is_important: bool) -> Self
    where
        V: Into<SettingType>,
    {
        self.settings.insert(
            name.into(),
            SettingValue {
                value: value.into(),
                is_important,
            },
        );
        self
    }

    pub(crate) fn get_sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn get_id(&self) -> &str {
        &self.id
    }

    pub(crate) fn get_settings(&self) -> &HashMap<String, SettingValue> {
        &self.settings
    }

    pub(crate) fn map_sql<F>(self, f: F) -> Self
    where
        F: Fn(&str) -> String,
    {
        Self {
            sql: f(&self.sql),
            ..self
        }
    }
}

impl<T> From<T> for Query
where
    T: AsRef<str>,
{
    fn from(source: T) -> Self {
        Self::new(source)
    }
}
