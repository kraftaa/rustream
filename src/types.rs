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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn boolean_types() {
        assert_eq!(pg_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(pg_type_to_arrow("bool"), DataType::Boolean);
    }

    #[test]
    fn integer_types() {
        assert_eq!(pg_type_to_arrow("smallint"), DataType::Int16);
        assert_eq!(pg_type_to_arrow("int2"), DataType::Int16);
        assert_eq!(pg_type_to_arrow("integer"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("int"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("int4"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("serial"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("bigint"), DataType::Int64);
        assert_eq!(pg_type_to_arrow("int8"), DataType::Int64);
        assert_eq!(pg_type_to_arrow("bigserial"), DataType::Int64);
    }

    #[test]
    fn float_types() {
        assert_eq!(pg_type_to_arrow("real"), DataType::Float32);
        assert_eq!(pg_type_to_arrow("float4"), DataType::Float32);
        assert_eq!(pg_type_to_arrow("double precision"), DataType::Float64);
        assert_eq!(pg_type_to_arrow("float8"), DataType::Float64);
    }

    #[test]
    fn numeric_as_utf8() {
        assert_eq!(pg_type_to_arrow("numeric"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("decimal"), DataType::Utf8);
    }

    #[test]
    fn string_types() {
        for t in &[
            "text",
            "varchar",
            "character varying",
            "char",
            "character",
            "name",
            "citext",
        ] {
            assert_eq!(pg_type_to_arrow(t), DataType::Utf8, "failed for {t}");
        }
    }

    #[test]
    fn binary_type() {
        assert_eq!(pg_type_to_arrow("bytea"), DataType::Binary);
    }

    #[test]
    fn date_type() {
        assert_eq!(pg_type_to_arrow("date"), DataType::Date32);
    }

    #[test]
    fn timestamp_types() {
        assert_eq!(
            pg_type_to_arrow("timestamp"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            pg_type_to_arrow("timestamp without time zone"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            pg_type_to_arrow("timestamp with time zone"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            pg_type_to_arrow("timestamptz"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn time_type() {
        assert_eq!(
            pg_type_to_arrow("time"),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            pg_type_to_arrow("time without time zone"),
            DataType::Time64(TimeUnit::Microsecond)
        );
    }

    #[test]
    fn uuid_json_types() {
        assert_eq!(pg_type_to_arrow("uuid"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("json"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("jsonb"), DataType::Utf8);
    }

    #[test]
    fn array_types() {
        assert_eq!(pg_type_to_arrow("_int4"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("_text"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("integer[]"), DataType::Utf8);
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(pg_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(pg_type_to_arrow("Integer"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("TEXT"), DataType::Utf8);
    }

    #[test]
    fn unknown_type_falls_back_to_utf8() {
        assert_eq!(pg_type_to_arrow("some_custom_type"), DataType::Utf8);
    }
}
