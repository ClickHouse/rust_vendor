use std::{collections::HashMap, fmt, sync::Arc};

use crate::types::{Progress, SettingType, SettingValue};

pub type ProgressCallback = Arc<dyn Fn(&Progress) + Send + Sync>;

#[derive(Clone)]
pub struct Query {
    sql: String,
    id: String,
    settings: HashMap<String, SettingValue>,
    progress_callback: Option<ProgressCallback>,
}

impl fmt::Debug for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Query")
            .field("sql", &self.sql)
            .field("id", &self.id)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl Query {
    pub fn new(sql: impl AsRef<str>) -> Self {
        Self {
            sql: sql.as_ref().to_string(),
            id: "".to_string(),
            settings: HashMap::new(),
            progress_callback: None,
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

    /// Invoked on every server Progress packet with the accumulated totals for
    /// this query (the wire protocol sends deltas, the driver sums them).
    pub fn with_progress<F>(mut self, f: F) -> Self
    where
        F: Fn(&Progress) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Arc::new(f));
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

    pub(crate) fn get_progress_callback(&self) -> Option<&ProgressCallback> {
        self.progress_callback.as_ref()
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
