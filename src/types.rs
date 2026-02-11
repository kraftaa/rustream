use arrow::datatypes::DataType;

/// Map a Postgres type name (from information_schema or pg_type) to an Arrow DataType.
pub fn pg_type_to_arrow(pg_type: &str) -> DataType {
    match pg_type.to_lowercase().as_str() {
        // Booleans
        "boolean" | "bool" => DataType::Boolean,

        // Integers
        "smallint" | "int2" => DataType::Int16,
        "integer" | "int" | "int4" => DataType::Int32,
        "bigint" | "int8" => DataType::Int64,
        "serial" => DataType::Int32,
        "bigserial" => DataType::Int64,

        // Floats
        "real" | "float4" => DataType::Float32,
        "double precision" | "float8" => DataType::Float64,
        "numeric" | "decimal" => DataType::Utf8, // preserve precision as string

        // Strings
        "text" | "varchar" | "character varying" | "char" | "character" | "name" | "citext" => {
            DataType::Utf8
        }

        // Binary
        "bytea" => DataType::Binary,

        // Date / Time
        "date" => DataType::Date32,
        "timestamp" | "timestamp without time zone" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        "timestamp with time zone" | "timestamptz" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        }
        "time" | "time without time zone" => {
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
        }
        "interval" => DataType::Utf8, // intervals as string

        // UUID
        "uuid" => DataType::Utf8,

        // JSON
        "json" | "jsonb" => DataType::Utf8,

        // Arrays → store as JSON string
        t if t.starts_with('_') || t.ends_with("[]") => DataType::Utf8,

        // Fallback
        _ => DataType::Utf8,
    }
}
