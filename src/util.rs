// pub fn serialize_with_display<T: std::fmt::Display>(x: &T) -> String {
//     format!("{x}")
// }

use std::fmt::Display;

use serde::Serializer;

pub fn serialize_with_display<T: Display, S>(x: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("{x}"))
}
