use proven_common::ChangeData;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct KvChangeData;
impl ChangeData for KvChangeData {
    fn merge(self, _other: Self) -> Self {
        self
    }
}
