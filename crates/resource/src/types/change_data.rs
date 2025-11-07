use proven_common::ChangeData;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceChangeData;
impl ChangeData for ResourceChangeData {
    fn merge(self, _other: Self) -> Self {
        self
    }
}
