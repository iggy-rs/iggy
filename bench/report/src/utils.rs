use serde::Serializer;

pub(crate) fn round_float<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_f64((value * 1000.0).round() / 1000.0)
}
