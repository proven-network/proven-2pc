use proven_common::ChangeData;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueChangeData;
impl ChangeData for QueueChangeData {
    fn merge(self, _other: Self) -> Self {
        self
    }
}
