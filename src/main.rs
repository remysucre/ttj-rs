use lfj::*;

fn main() -> Result<(), polars::prelude::PolarsError> {
    let db = data::ImdbData::new();
    // o1a::q1a(&db)?;
    // o6f::q6f(&db)?;
    // o7c::q7c(&db)?;
    // o8c::q8c(&db)?;
    // o10c::q10c(&db)?;
    // o11a::q11a(&db)?;
    // o13a::q13a(&db)?;
    // o15a::q15a(&db)?;
    // o16b::q16b(&db)?;
    // o17a::q17a(&db)?;
    // o17f::q17f(&db)?;
    // o18c::q18c(&db)?;
    // o19d::q19d(&db)?;
    o21a::q21a(&db)?;
    // o25a::q25a(&db)?;
    // o25c::q25c(&db)?;
    // o30c::q30c(&db)?;
    // o31a::q31a(&db)?;
    // o33a::q33a(&db)?;
    Ok(())
}
