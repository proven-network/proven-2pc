mod common;

#[path = "aggregate"]
mod tests {
    pub mod avg;
    pub mod count;
    pub mod expr;
    pub mod group_by;
    pub mod max;
    pub mod min;
    pub mod stdev;
    pub mod sum;
    pub mod variance;
}
