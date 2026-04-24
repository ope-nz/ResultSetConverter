ResultSetConverter is a small Java utility library for B4J that converts JDBC ResultSet objects into B4X-friendly List and Map structures.

It removes repetitive JDBC iteration and type-handling code and is particularly useful for APIs, UI data binding, exports, and reporting.

## Methods

### Configuration
| Method | Description |
|--------|-------------|
| `setLowerCaseFieldNames(boolean)` | Convert all column name keys/headers to lowercase. Default: false. |

### Metadata
| Method | Returns | Description |
|--------|---------|-------------|
| `GetFieldTypes(rs)` | Map | Column names mapped to their JDBC type names. Does not close the ResultSet. |
| `GetFieldList(rs)` | List | Column names as a List. Does not close the ResultSet. |

### List Output
| Method | Returns | Description |
|--------|---------|-------------|
| `ToListOfMaps(rs, includeRowNumber, rowNumberColumnName)` | List | Each row as a Map. Optional 1-based row number field. |
| `ToListOfLists(rs, includeHeader)` | List | Each row as a List, with an optional header row of column names. |
| `ToList(rs)` | List | All values from the first column as a flat List. |

### Scalar
| Method | Returns | Description |
|--------|---------|-------------|
| `ToScalar(rs)` | Object | First value of the first row, typed. Returns null if empty. |
| `ToScalarString(rs)` | String | First value of the first row converted to String. Returns null if empty. |
| `ToScalarInt(rs)` | int | First value of the first row as int. Returns 0 if null or empty. |
| `ToScalarLong(rs)` | long | First value of the first row as long. Returns 0 if null or empty. |
| `ToScalarDouble(rs)` | double | First value of the first row as double. Returns 0.0 if null or empty. |

### Map Output
| Method | Returns | Description |
|--------|---------|-------------|
| `ToMap(rs)` | Map | Column names as keys, first row values as values. |
| `ToMapOfMaps(rs)` | Map | First column as outer key, remaining columns as inner Map per row. |
| `ToMapOfLists(rs)` | Map | Column names as keys, all values in each column as a List. |
| `ToMapOfRowNumberValuePair(rs)` | Map | 1-based row number as key, first column value as value. |
| `ToMapOfKeyValuePair(rs)` | Map | First column as key, second column as value. |

### JSON Output
| Method | Returns | Description |
|--------|---------|-------------|
| `ToJSON(rs, includeRowNumber)` | String | Array of row objects. Optional row number field. |
| `ToJSONObject(rs)` | String | Single JSON object from the first row. |
| `ToJSONArray(rs)` | String | Flat JSON array of values from the first column. |
| `ToTimeSeriesJSON(rs, timeColumn, valueColumns)` | String | Time series JSON object with named series (compatible with charting libraries). |
| `ToPagedJSON(rs, page, pageSize)` | String | Paginated JSON with page metadata and a data array. |

### Text Output
| Method | Returns | Description |
|--------|---------|-------------|
| `ToCSV(rs, includeHeader)` | String | RFC 4180 CSV with optional header row. |
| `ToHTMLTable(rs, cssClass)` | String | HTML table string with optional CSS class. |

## Notes

- All methods except `GetFieldTypes` and `GetFieldList` automatically close the ResultSet.
- Column types are preserved where possible: `int`, `long`, `float`, `double`, `boolean`, `BigDecimal`, and date/time types are mapped automatically. Unrecognized types fall back to String.
- `setLowerCaseFieldNames` applies globally to all subsequent calls.
