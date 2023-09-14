pub(crate) fn is_power_of_two(n: u32) -> bool {
    n != 0 && (n & (n - 1)) == 0
}
