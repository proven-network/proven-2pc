use proven_common::ChangeData;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SqlChangeData;
impl ChangeData for SqlChangeData {
    fn merge(self, _other: Self) -> Self {
        self
    }
}
