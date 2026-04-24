package nz.ope.rsconv;

import anywheresoftware.b4a.BA.Author;
import anywheresoftware.b4a.BA.ShortName;
import anywheresoftware.b4a.BA.Version;
import anywheresoftware.b4a.keywords.Common;
import anywheresoftware.b4a.objects.collections.Map;
import anywheresoftware.b4a.objects.collections.List;

import java.util.ArrayList;

import java.sql.*;
import java.text.SimpleDateFormat;

@Version(1.1f)
@Author("Ope Ltd")
@ShortName("ResultSetConverter")

public class ResultSetConverter {

    private static boolean pLowerCaseFieldNames = false;

    /**
     * Sets whether field names returned by all methods are converted to lowercase.
     * Applies globally to all subsequent calls. Default is false.
     *
     * @param value If true, all column names used as keys or headers are lowercased.
     */
    public static void setLowerCaseFieldNames(boolean value) {
        pLowerCaseFieldNames = value;
    }

    /**
     * Returns a Map containing the column names from the ResultSet as the keys, and
     * the corresponding JDBC type name as the value.
     * 
     * Note, this does not close the ResultSet.
     * 
     * Example output:
     * {"Name": "VARCHAR", "Age": "INTEGER"}
     * 
     * @param rs The ResultSet to convert.
     * @return A Map containing the column names and their corresponding JDBC type
     *         names.
     */
    public static Map GetFieldTypes(ResultSet rs) {
        Map Result = new Map();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                String columnName = fieldName(metaData.getColumnLabel(i));
                int columnType = metaData.getColumnType(i);
                Result.Put(columnName, convertJdbcTypeToString(columnType));
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        return Result;
    }

    /**
     * Returns a List containing the column names from the ResultSet.
     * 
     * Note, this does not close the ResultSet.
     * 
     * Example output:
     * ["Name", "Age"]
     * 
     * @param rs The ResultSet to convert.
     * @return A List containing the column names.
     */
    public static List GetFieldList(ResultSet rs) {
        List Result = new List();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                String columnName = fieldName(metaData.getColumnLabel(i));
                Result.Add(columnName);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        return Result;
    }

    /**
     * Converts a JDBC ResultSet to a List of Maps, with the column names used
     * as the keys in each Map.
     *
     * The column type is used to determine the type of the value to be added to the
     * Map (e.g. Integer for Types.INTEGER, etc.).
     *
     * If the column type is not recognized, the value is added as a String.
     *
     * If includeRowNumber is true, each Map will include a 1-based row number as
     * the first entry. The key for this field is set by rowNumberColumnName.
     * If rowNumberColumnName is null, "Row" is used as the default.
     *
     * Example output (includeRowNumber = false):
     * [{"Name": "John", "Age": 30}, {"Name": "Jane", "Age": 25}]
     *
     * Example output (includeRowNumber = true, rowNumberColumnName = null):
     * [{"Row": 1, "Name": "John", "Age": 30}, {"Row": 2, "Name": "Jane", "Age": 25}]
     *
     * Example output (includeRowNumber = true, rowNumberColumnName = "#"):
     * [{"#": 1, "Name": "John", "Age": 30}, {"#": 2, "Name": "Jane", "Age": 25}]
     *
     * @param rs                  The ResultSet to convert.
     * @param includeRowNumber    If true, each Map includes a row number field.
     * @param rowNumberColumnName The key name for the row number field. Defaults to "Row" if null.
     * @return A List containing the values from the ResultSet.
     */
    public static List ToListOfMaps(ResultSet rs, boolean includeRowNumber, String rowNumberColumnName) {
        List Result = new List();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        if (includeRowNumber && (rowNumberColumnName == null || rowNumberColumnName == ""))
            rowNumberColumnName = "Row";

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            String[] columnNames = new String[columnCount];
            int[] columnTypes = new int[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnNames[i] = fieldName(metaData.getColumnLabel(i + 1));
                columnTypes[i] = metaData.getColumnType(i + 1);
            }

            int rowNumber = 1;

            while (rs.next()) {
                Map Row = new Map();
                Row.Initialize();

                if (includeRowNumber) Row.Put(rowNumberColumnName, rowNumber++);

                for (int i = 0; i < columnCount; i++) {
                    Object value = getResultSetValue(rs, i + 1, columnTypes[i]);
                    Row.Put(columnNames[i], value);
                }
                Result.Add(Row.getObject());
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }

        CloseResultSet(rs);
        return Result;
    }

    /**
     * Converts a JDBC ResultSet to a List of Lists, with the first inner List
     * containing the column names (if includeHeader is true), and each subsequent
     * List containing the values from one row.
     *
     * The column type is used to determine the type of each value
     * (e.g. Integer for Types.INTEGER, etc.).
     * If the column type is not recognized, the value is added as a String.
     *
     * Example output:
     * [["Name", "Age"], ["John", 30], ["Jane", 25]]
     *
     * @param rs            The ResultSet to convert.
     * @param includeHeader If true, the first List contains the column names.
     * @return A List of Lists containing the values from the ResultSet.
     */
    public static List ToListOfLists(ResultSet rs, boolean includeHeader) {
        List Result = new List();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            String[] columnNames = new String[columnCount];
            int[] columnTypes = new int[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnNames[i] = fieldName(metaData.getColumnLabel(i + 1));
                columnTypes[i] = metaData.getColumnType(i + 1);
            }

            if (includeHeader) {
                ArrayList Row = new ArrayList<>();
                for (int i = 0; i < columnCount; i++) {
                    Row.add(columnNames[i]);
                }
                Result.Add(Row);
            }

            while (rs.next()) {
                ArrayList Row = new ArrayList<>();
                for (int i = 0; i < columnCount; i++) {
                    Object value = getResultSetValue(rs, i + 1, columnTypes[i]);
                    Row.add(value);
                }
                Result.Add(Row);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);
        return Result;
    }

    /**
     * Converts a JDBC ResultSet to a List, using the values from the first column.
     *
     * The column type of the first column is used to determine the type of each
     * value (e.g. Integer for Types.INTEGER, etc.).
     * If the column type is not recognized, the value is added as a String.
     *
     * Expects a ResultSet with a single column.
     *
     * Example output:
     * ["John", "Jane"]
     *
     * @param rs The ResultSet to convert.
     * @return A List containing the values from the first column.
     */
    public static List ToList(ResultSet rs) {
        List Result = new List();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnType = metaData.getColumnType(1);
            while (rs.next()) {
                Object value = getResultSetValue(rs, 1, columnType);
                Result.Add(value);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);
        return Result;
    }

    /**
     * Returns a single value from the first column of the first row of the
     * ResultSet.
     *
     * Useful for aggregate queries such as COUNT(*), MAX(), SUM(), etc.
     * The column type is used to determine the type of the returned value.
     * If the ResultSet is empty, null is returned.
     *
     * Example output:
     * 3
     *
     * @param rs The ResultSet to read.
     * @return The value of the first column of the first row, or null if empty.
     */
    public static Object ToScalar(ResultSet rs) {
        if (VerifyResultSet(rs) == false)
            return null;

        Object value = null;
        try {
            if (rs.next()) {
                int columnType = rs.getMetaData().getColumnType(1);
                value = getResultSetValue(rs, 1, columnType);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);
        return value;
    }

    /**
     * Returns the first column of the first row as a String.
     * Any column type is accepted: numeric, boolean, date, and string types are
     * all converted via their toString representation. Dates are formatted as
     * "yyyy-MM-dd", times as "HH:mm:ss", and timestamps as "yyyy-MM-dd'T'HH:mm:ss.SSSXXX".
     * Returns null if the ResultSet is empty, the value is SQL NULL, or an error occurs.
     *
     * @param rs The ResultSet to read.
     * @return The value as a String, or null.
     */
    public static String ToScalarString(ResultSet rs) {
        if (VerifyResultSet(rs) == false)
            return null;

        try {
            if (rs.next()) {
                int columnType = rs.getMetaData().getColumnType(1);
                Object value = getResultSetValue(rs, 1, columnType);
                return value != null ? value.toString() : null;
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        } finally {
            CloseResultSet(rs);
        }
        return null;
    }

    /**
     * Returns the first column of the first row as an int.
     * Returns 0 if the ResultSet is empty, the value is SQL NULL, the column
     * is not an integer type, or an error occurs.
     *
     * Accepted column types: INTEGER, SMALLINT, TINYINT.
     *
     * @param rs The ResultSet to read.
     * @return The value as an int, or 0.
     */
    public static int ToScalarInt(ResultSet rs) {
        if (VerifyResultSet(rs) == false)
            return 0;

        try {
            if (rs.next()) {
                int columnType = rs.getMetaData().getColumnType(1);
                switch (columnType) {
                    case Types.INTEGER:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        int val = rs.getInt(1);
                        return rs.wasNull() ? 0 : val;
                    default:
                        return 0;
                }
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        } finally {
            CloseResultSet(rs);
        }
        return 0;
    }

    /**
     * Returns the first column of the first row as a long.
     * Returns 0 if the ResultSet is empty, the value is SQL NULL, the column
     * is not a bigint type, or an error occurs.
     *
     * Accepted column types: BIGINT.
     *
     * @param rs The ResultSet to read.
     * @return The value as a long, or 0.
     */
    public static long ToScalarLong(ResultSet rs) {
        if (VerifyResultSet(rs) == false)
            return 0;

        try {
            if (rs.next()) {
                int columnType = rs.getMetaData().getColumnType(1);
                switch (columnType) {
                    case Types.BIGINT:
                        long val = rs.getLong(1);
                        return rs.wasNull() ? 0 : val;
                    default:
                        return 0;
                }
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        } finally {
            CloseResultSet(rs);
        }
        return 0;
    }

    /**
     * Returns the first column of the first row as a double.
     * Returns 0.0 if the ResultSet is empty, the value is SQL NULL, the column
     * is not a floating-point or decimal type, or an error occurs.
     *
     * Accepted column types: DOUBLE, FLOAT, REAL, DECIMAL, NUMERIC.
     *
     * @param rs The ResultSet to read.
     * @return The value as a double, or 0.0.
     */
    public static double ToScalarDouble(ResultSet rs) {
        if (VerifyResultSet(rs) == false)
            return 0.0;

        try {
            if (rs.next()) {
                int columnType = rs.getMetaData().getColumnType(1);
                switch (columnType) {
                    case Types.DOUBLE:
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        double val = rs.getDouble(1);
                        return rs.wasNull() ? 0.0 : val;
                    default:
                        return 0.0;
                }
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        } finally {
            CloseResultSet(rs);
        }
        return 0.0;
    }

    /**
     * Converts a JDBC ResultSet to a Map, with the column names used as the
     * keys, and the values of the columns used as the values.
     * 
     * The column types of the columns are used to determine the types of the
     * keys and values to be added to the Map (e.g. Integer for Types.INTEGER,
     * etc.). If the column type is not recognized, the value is added as a String.
     * 
     * Expects a ResultSet with a single row. If more than one row is returned,
     * only the first row is used.
     * 
     * Example output:
     * {"Name": "John", "Age": 30}
     * 
     * @param rs The ResultSet to convert.
     * @return A Map containing the values from the ResultSet.
     */
    public static Map ToMap(ResultSet rs) {
        Map Result = new Map();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            if (!rs.next()) {
                CloseResultSet(rs);
                return Result;
            }

            for (int i = 1; i <= columnCount; i++) {
                String columnName = fieldName(metaData.getColumnLabel(i));
                int columnType = metaData.getColumnType(i);
                Object value = getResultSetValue(rs, i, columnType);
                Result.Put(columnName, value);
            }

        } catch (Exception e) {
            Common.Log(e.getMessage());
        }

        CloseResultSet(rs);
        return Result;
    }

    /**
     * Converts a JDBC ResultSet to a Map, with the value of the first column
     * used as the key, and a Map of the columns used as the value.
     * 
     * The column types of the columns are used to determine the types of the
     * keys and values to be added to the Map (e.g. Integer for Types.INTEGER,
     * etc.).
     * 
     * If the column types are not recognized, the values are added as Strings.
     * 
     * Column one should be unique in the ResultSet.
     * 
     * Example output:
     * {"John": {"Name": "John", "Age": 30}, "Jane": {"Name": "Jane", "Age": 25}}
     * 
     * @param rs The ResultSet to convert.
     * @return A Map containing the values from the ResultSet.
     */
    public static Map ToMapOfMaps(ResultSet rs) {
        Map Result = new Map();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            String[] columnNames = new String[columnCount];
            int[] columnTypes = new int[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnNames[i] = fieldName(metaData.getColumnLabel(i + 1));
                columnTypes[i] = metaData.getColumnType(i + 1);
            }

            while (rs.next()) {
                Object key = getResultSetValue(rs, 1, columnTypes[0]);

                Map Row = new Map();
                Row.Initialize();

                for (int i = 0; i < columnCount; i++) {
                    Object value = getResultSetValue(rs, i + 1, columnTypes[i]);
                    Row.Put(columnNames[i], value);
                }
                Result.Put(key, Row.getObject());
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);
        return Result;
    }

    /**
     * Converts a JDBC ResultSet to a Map of Lists, with each key
     * being a column label from the ResultSet, and each value being an
     * ArrayList containing all the values from that column in the
     * ResultSet.
     * 
     * Example output:
     * {
     * "Name": ["John", "Jane"],
     * "Age": [30, 25]
     * }
     * 
     * @param rs The ResultSet to convert.
     * @return A Map containing the values from the ResultSet.
     */
    public static Map ToMapOfLists(ResultSet rs) {
        Map result = new Map();
        result.Initialize();

        if (VerifyResultSet(rs) == false)
            return result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // One ArrayList per column
            ArrayList[] columns = new ArrayList[columnCount];
            int[] columnTypes = new int[columnCount];

            for (int i = 0; i < columnCount; i++) {
                columns[i] = new ArrayList();
                columnTypes[i] = metaData.getColumnType(i + 1);
            }

            // Read rows
            while (rs.next()) {
                for (int col = 0; col < columnCount; col++) {
                    Object value = getResultSetValue(rs, col + 1, columnTypes[col]);
                    columns[col].add(value);
                }
            }

            // Add to map (using column labels as keys)
            for (int col = 0; col < columnCount; col++) {
                String columnName = fieldName(metaData.getColumnLabel(col + 1));
                result.Put(columnName, columns[col]);
            }

        } catch (Exception e) {
            Common.Log(e.getMessage());
        }

        CloseResultSet(rs);
        return result;
    }

    /**
     * Converts a JDBC ResultSet to a Map, with the row number as the key
     * and the value of the first column as the value.
     * 
     * The column type of the first column is used to determine the type of the
     * value to be added to the Map (e.g. Integer for Types.INTEGER, etc.).
     * If the column type is not recognized, the value is added as a String.
     * 
     * Expects a ResultSet with a single column.
     * 
     * Example output:
     * {1: "John", 2: "Jane"}
     * 
     * @param rs The ResultSet to convert.
     * @return A Map containing the values from the ResultSet.
     */
    public static Map ToMapOfRowNumberValuePair(ResultSet rs) {
        Map Result = new Map();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnType = metaData.getColumnType(1);
            Long rowNumber = 0L;

            while (rs.next()) {
                rowNumber++;
                Object value = getResultSetValue(rs, 1, columnType);
                Result.Put(rowNumber, value);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);
        return Result;
    }

    /**
     * Converts a JDBC ResultSet to a Map, with the first column's values used
     * as the keys, and the second column's values used as the values.
     * 
     * The column types of the columns are used to determine the types of the
     * keys and values to be added to the Map (e.g. Integer for Types.INTEGER,
     * etc.).
     * If the column types are not recognized, the values are added as Strings.
     * 
     * Expects a ResultSet with two columns.
     * 
     * Example output:
     * {"John": 30, "Jane": 25}
     * 
     * @param rs The ResultSet to convert.
     * @return A Map containing the values from the ResultSet.
     */
    public static Map ToMapOfKeyValuePair(ResultSet rs) {
        Map Result = new Map();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int keyType = metaData.getColumnType(1);
            int valueType = metaData.getColumnType(2);

            while (rs.next()) {
                Object key = getResultSetValue(rs, 1, keyType);
                Object value = getResultSetValue(rs, 2, valueType);
                Result.Put(key, value);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);
        return Result;
    }

    /**
     * Converts a JDBC ResultSet to a JSON array of objects, with the column
     * names used as the keys in each object.
     *
     * If includeRowNumber is true, each object will include a "Row" key with
     * a 1-based row number as the first field.
     *
     * Example output:
     * [{"Name":"John","Age":30},{"Name":"Jane","Age":25}]
     *
     * @param rs              The ResultSet to convert.
     * @param includeRowNumber If true, each object includes a "Row" field.
     * @return A JSON string representing the ResultSet.
     */
    public static String ToJSON(ResultSet rs, boolean includeRowNumber) {
        if (VerifyResultSet(rs) == false)
            return "[]";

        StringBuilder sb = new StringBuilder("[");
        boolean firstRow = true;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            String[] columnNames = new String[columnCount];
            int[] columnTypes = new int[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnNames[i] = fieldName(metaData.getColumnLabel(i + 1));
                columnTypes[i] = metaData.getColumnType(i + 1);
            }

            int rowNumber = 1;
            while (rs.next()) {
                if (!firstRow) sb.append(",");
                firstRow = false;
                sb.append("{");
                boolean firstField = true;
                if (includeRowNumber) {
                    sb.append("\"Row\":").append(rowNumber++);
                    firstField = false;
                }
                for (int i = 0; i < columnCount; i++) {
                    if (!firstField) sb.append(",");
                    firstField = false;
                    sb.append(toJsonValue(columnNames[i])).append(":");
                    sb.append(toJsonValue(getResultSetValue(rs, i + 1, columnTypes[i])));
                }
                sb.append("}");
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }

        CloseResultSet(rs);
        sb.append("]");
        return sb.toString();
    }

    /**
     * Converts the first row of a JDBC ResultSet to a single JSON object,
     * with the column names used as the keys.
     *
     * If the ResultSet is empty, an empty object is returned.
     * If more than one row is returned, only the first row is used.
     *
     * Example output:
     * {"Name":"John","Age":30}
     *
     * @param rs The ResultSet to convert.
     * @return A JSON string representing the first row.
     */
    public static String ToJSONObject(ResultSet rs) {
        if (VerifyResultSet(rs) == false)
            return "{}";

        StringBuilder sb = new StringBuilder("{");

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            if (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) sb.append(",");
                    String columnName = fieldName(metaData.getColumnLabel(i));
                    int columnType = metaData.getColumnType(i);
                    Object value = getResultSetValue(rs, i, columnType);
                    sb.append(toJsonValue(columnName)).append(":").append(toJsonValue(value));
                }
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }

        CloseResultSet(rs);
        sb.append("}");
        return sb.toString();
    }

    /**
     * Converts the first column of a JDBC ResultSet to a flat JSON array.
     *
     * Expects a ResultSet with a single column.
     *
     * Example output:
     * ["John","Jane","Bob"]
     *
     * @param rs The ResultSet to convert.
     * @return A JSON string representing the column values as an array.
     */
    public static String ToJSONArray(ResultSet rs) {
        if (VerifyResultSet(rs) == false)
            return "[]";

        StringBuilder sb = new StringBuilder("[");
        boolean first = true;

        try {
            int columnType = rs.getMetaData().getColumnType(1);
            while (rs.next()) {
                if (!first) sb.append(",");
                first = false;
                sb.append(toJsonValue(getResultSetValue(rs, 1, columnType)));
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }

        CloseResultSet(rs);
        sb.append("]");
        return sb.toString();
    }

    /**
     * Converts a JDBC ResultSet to an ECharts-compatible time-series JSON object.
     *
     * The time column must contain millisecond epoch values (Long). Each entry in
     * valueColumns produces one series. Column lookup is case-insensitive.
     *
     * Output:
     * {"series":[{"name":"cpu","data":[[1711450000000,23.4],...]},...]}
     *
     * @param rs           The ResultSet to convert.
     * @param timeColumn   Column name containing epoch-ms timestamps.
     * @param valueColumns Column names to expose as series.
     * @return A JSON string in ECharts time-series format.
     */
    public static String ToTimeSeriesJSON(ResultSet rs, String timeColumn, String[] valueColumns) {
        if (VerifyResultSet(rs) == false)
            return "{\"series\":[]}";

        java.util.ArrayList<Long> times = new java.util.ArrayList<>();
        @SuppressWarnings("unchecked")
        java.util.ArrayList<Object>[] values = new java.util.ArrayList[valueColumns.length];
        for (int i = 0; i < valueColumns.length; i++)
            values[i] = new java.util.ArrayList<>();

        try {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            java.util.HashMap<String, Integer> colIdx = new java.util.HashMap<>();
            int[] colTypes = new int[colCount + 1];
            for (int i = 1; i <= colCount; i++) {
                colIdx.put(meta.getColumnLabel(i).toLowerCase(), i);
                colTypes[i] = meta.getColumnType(i);
            }

            Integer tIdx = colIdx.get(timeColumn.toLowerCase());
            if (tIdx == null) {
                Common.Log("ToTimeSeriesJSON: column '" + timeColumn + "' not found");
                CloseResultSet(rs);
                return "{\"series\":[]}";
            }

            Integer[] vIdx = new Integer[valueColumns.length];
            int[] vTypes = new int[valueColumns.length];
            for (int i = 0; i < valueColumns.length; i++) {
                vIdx[i] = colIdx.get(valueColumns[i].toLowerCase());
                if (vIdx[i] != null) vTypes[i] = colTypes[vIdx[i]];
            }

            while (rs.next()) {
                times.add(rs.getLong(tIdx));
                for (int i = 0; i < valueColumns.length; i++)
                    values[i].add(vIdx[i] != null ? getResultSetValue(rs, vIdx[i], vTypes[i]) : null);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);

        StringBuilder sb = new StringBuilder("{\"series\":[");
        for (int s = 0; s < valueColumns.length; s++) {
            if (s > 0) sb.append(",");
            sb.append("{\"name\":").append(toJsonValue(valueColumns[s])).append(",\"data\":[");
            for (int r = 0; r < times.size(); r++) {
                if (r > 0) sb.append(",");
                sb.append("[").append(times.get(r)).append(",")
                  .append(toJsonValue(values[s].get(r))).append("]");
            }
            sb.append("]}");
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Converts a JDBC ResultSet to a paginated JSON object.
     *
     * Reads all rows into memory then returns the requested page. Pages are 1-based.
     *
     * Output:
     * {"page":1,"pageSize":100,"totalRows":4820,"data":[...]}
     *
     * @param rs       The ResultSet to convert.
     * @param page     1-based page number.
     * @param pageSize Rows per page.
     * @return A JSON string representing the paginated result.
     */
    public static String ToPagedJSON(ResultSet rs, int page, int pageSize) {
        if (VerifyResultSet(rs) == false)
            return "{\"page\":" + page + ",\"pageSize\":" + pageSize + ",\"totalRows\":0,\"data\":[]}";
        if (page < 1) page = 1;
        if (pageSize < 1) pageSize = 100;

        java.util.ArrayList<String> allRows = new java.util.ArrayList<>();
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            String[] names = new String[colCount];
            int[] types = new int[colCount];
            for (int i = 0; i < colCount; i++) {
                names[i] = fieldName(meta.getColumnLabel(i + 1));
                types[i] = meta.getColumnType(i + 1);
            }
            while (rs.next()) {
                StringBuilder row = new StringBuilder("{");
                for (int i = 0; i < colCount; i++) {
                    if (i > 0) row.append(",");
                    row.append(toJsonValue(names[i])).append(":")
                       .append(toJsonValue(getResultSetValue(rs, i + 1, types[i])));
                }
                row.append("}");
                allRows.add(row.toString());
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);

        int total = allRows.size();
        int from = (page - 1) * pageSize;
        int to = Math.min(from + pageSize, total);

        StringBuilder sb = new StringBuilder();
        sb.append("{\"page\":").append(page)
          .append(",\"pageSize\":").append(pageSize)
          .append(",\"totalRows\":").append(total)
          .append(",\"data\":[");
        for (int i = from; i < to; i++) {
            if (i > from) sb.append(",");
            sb.append(allRows.get(i));
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Converts a JDBC ResultSet to a CSV string.
     *
     * If includeHeader is true, the first line will contain the column names.
     * Values containing commas, double-quotes, or newlines are quoted and
     * internal double-quotes are escaped by doubling them.
     * Lines are separated by CRLF as per the CSV standard (RFC 4180).
     *
     * Example output:
     * Name,Age,Salary
     * John,30,55000.5
     * Jane,25,48000.0
     *
     * @param rs            The ResultSet to convert.
     * @param includeHeader If true, the first line contains column names.
     * @return A CSV string representing the ResultSet.
     */
    public static String ToCSV(ResultSet rs, boolean includeHeader) {
        if (VerifyResultSet(rs) == false)
            return "";

        StringBuilder sb = new StringBuilder();

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            String[] columnNames = new String[columnCount];
            int[] columnTypes = new int[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnNames[i] = fieldName(metaData.getColumnLabel(i + 1));
                columnTypes[i] = metaData.getColumnType(i + 1);
            }

            if (includeHeader) {
                for (int i = 0; i < columnCount; i++) {
                    if (i > 0) sb.append(",");
                    sb.append(toCsvValue(columnNames[i]));
                }
                sb.append("\r\n");
            }

            while (rs.next()) {
                for (int i = 0; i < columnCount; i++) {
                    if (i > 0) sb.append(",");
                    sb.append(toCsvValue(getResultSetValue(rs, i + 1, columnTypes[i])));
                }
                sb.append("\r\n");
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }

        CloseResultSet(rs);
        return sb.toString();
    }

    /**
     * Converts a JDBC ResultSet to an HTML table string.
     *
     * Column names are used as header cells. If cssClass is non-null and
     * non-empty, it is applied to the table element. Null values are rendered
     * as empty cells. All text content is HTML-escaped.
     *
     * Example output:
     * &lt;table class="my-table"&gt;&lt;thead&gt;&lt;tr&gt;&lt;th&gt;Name&lt;/th&gt;...
     *
     * @param rs       The ResultSet to convert.
     * @param cssClass Optional CSS class for the table element. Pass null or ""
     *                 for no class.
     * @return An HTML string representing the ResultSet as a table.
     */
    public static String ToHTMLTable(ResultSet rs, String cssClass) {
        if (VerifyResultSet(rs) == false)
            return "";

        StringBuilder sb = new StringBuilder();

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            String[] columnNames = new String[columnCount];
            int[] columnTypes = new int[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnNames[i] = fieldName(metaData.getColumnLabel(i + 1));
                columnTypes[i] = metaData.getColumnType(i + 1);
            }

            if (cssClass != null && !cssClass.isEmpty()) {
                sb.append("<table class=\"").append(escapeHtml(cssClass)).append("\">");
            } else {
                sb.append("<table>");
            }

            sb.append("<thead><tr>");
            for (String name : columnNames) {
                sb.append("<th>").append(escapeHtml(name)).append("</th>");
            }
            sb.append("</tr></thead><tbody>");

            while (rs.next()) {
                sb.append("<tr>");
                for (int i = 0; i < columnCount; i++) {
                    Object value = getResultSetValue(rs, i + 1, columnTypes[i]);
                    sb.append("<td>").append(value != null ? escapeHtml(value.toString()) : "").append("</td>");
                }
                sb.append("</tr>");
            }

            sb.append("</tbody></table>");

        } catch (Exception e) {
            Common.Log(e.getMessage());
        }

        CloseResultSet(rs);
        return sb.toString();
    }

    /**
     * Escapes special HTML characters in a string to prevent XSS.
     * Replaces &amp;, &lt;, &gt;, and &quot;.
     *
     * @param s The string to escape.
     * @return The HTML-escaped string.
     */
    private static String escapeHtml(String s) {
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    /**
     * Returns the field name, lowercased if pLowerCaseFieldNames is true.
     *
     * @param name The raw column name from the ResultSet metadata.
     * @return The field name to use as a key or header.
     */
    private static String fieldName(String name) {
        return pLowerCaseFieldNames ? name.toLowerCase() : name;
    }

    /**
     * Converts a Java value to its JSON representation.
     * Strings are quoted and special characters are escaped.
     * Numbers and booleans are returned without quotes.
     * Null is returned as "null".
     *
     * @param value The value to convert.
     * @return A JSON string representation of the value.
     */
    private static String toJsonValue(Object value) {
        if (value == null)
            return "null";
        if (value instanceof Boolean || value instanceof Integer ||
                value instanceof Long || value instanceof Float ||
                value instanceof Double || value instanceof java.math.BigDecimal)
            return value.toString();
        String s = value.toString();
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append("\"");
        return sb.toString();
    }

    /**
     * Converts a value to a CSV-safe string.
     * If the value contains commas, double-quotes, or newlines, it is wrapped
     * in double-quotes and any internal double-quotes are escaped by doubling.
     * Null values are returned as an empty string.
     *
     * @param value The value to convert.
     * @return A CSV-safe string representation of the value.
     */
    private static String toCsvValue(Object value) {
        if (value == null)
            return "";
        String s = value.toString();
        if (s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r"))
            return "\"" + s.replace("\"", "\"\"") + "\"";
        return s;
    }

    /**
     * Formats a given Date object to a string in the format "yyyy-MM-dd".
     * If the given Date object is null, the method returns null.
     * 
     * @param date the Date object to format
     * @return a string representation of the given Date object, or null if the
     *         object is null
     */
    private static String formatDate(Date date) {
        if (date == null)
            return null;
        return new SimpleDateFormat("yyyy-MM-dd").format(date);
    }

    /**
     * Formats a given Time object to a string in the format "HH:mm:ss".
     * If the given Time object is null, the method returns null.
     * 
     * @param time the Time object to format
     * @return a string representation of the given Time object, or null if the
     *         object is null
     */
    private static String formatTime(Time time) {
        if (time == null)
            return null;
        return new SimpleDateFormat("HH:mm:ss").format(time);
    }

    /**
     * Formats a given Timestamp object to a string in the format
     * "yyyy-MM-dd'T'HH:mm:ss.SSSXXX".
     * The string is formatted according to the UTC timezone.
     * If the given Timestamp object is null, the method returns null.
     * 
     * @param timestamp the Timestamp object to format
     * @return a string representation of the given Timestamp object, or null if the
     *         object is null
     */
    private static String formatTimestamp(Timestamp timestamp) {
        if (timestamp == null)
            return null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
        return sdf.format(timestamp);
    }

    /**
     * Retrieves the value of a column from a ResultSet, converting it to the
     * appropriate Java type based on the JDBC column type.
     *
     * If the column contains a SQL NULL, null is returned.
     * If the column type is not recognized, the value is returned as a String
     * via getString().
     *
     * @param rs          The ResultSet to read from.
     * @param columnIndex The 1-based index of the column to retrieve.
     * @param columnType  The JDBC type constant (from java.sql.Types) for the column.
     * @return The column value as a typed Java object, or null if the value is SQL NULL.
     */
    private static Object getResultSetValue(ResultSet rs, int columnIndex, int columnType) {
        Object value = null;

        try {
            if (rs.getObject(columnIndex) == null) {
                value = null;
            } else {
                switch (columnType) {
                    case Types.INTEGER:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        value = rs.getInt(columnIndex);
                        break;
                    case Types.BIGINT:
                        value = rs.getLong(columnIndex);
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                        value = rs.getFloat(columnIndex);
                        break;
                    case Types.DOUBLE:
                        value = rs.getDouble(columnIndex);
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        value = rs.getBigDecimal(columnIndex);
                        break;
                    case Types.BOOLEAN:
                    case Types.BIT:
                        value = rs.getBoolean(columnIndex);
                        break;
                    case Types.DATE:
                        value = formatDate(rs.getDate(columnIndex));
                        break;
                    case Types.TIME:
                        value = formatTime(rs.getTime(columnIndex));
                        break;
                    case Types.TIMESTAMP:
                        value = formatTimestamp(rs.getTimestamp(columnIndex));
                        break;
                    default:
                        value = rs.getString(columnIndex);
                }
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }

        return value;
    }

    /**
     * Converts a given JDBC type code to its corresponding string representation.
     *
     * This method maps the integer type codes defined in the java.sql.Types class
     * to their respective string names. It handles both negative and positive type
     * codes.
     * If the provided type code does not match any known type, "UNKNOWN" is
     * returned.
     *
     * @param jdbcType The integer type code of the JDBC type.
     * @return The string representation of the JDBC type.
     */
    private static String convertJdbcTypeToString(int jdbcType) {
        switch (jdbcType) {
            // Negative type codes
            case Types.BIGINT:
                return "BIGINT"; // -5
            case Types.BINARY:
                return "BINARY"; // -2
            case Types.BIT:
                return "BIT"; // -7
            case Types.LONGNVARCHAR:
                return "LONGNVARCHAR"; // -16
            case Types.LONGVARBINARY:
                return "LONGVARBINARY"; // -4
            case Types.LONGVARCHAR:
                return "LONGVARCHAR"; // -1
            case Types.NCHAR:
                return "NCHAR"; // -15
            case Types.NVARCHAR:
                return "NVARCHAR"; // -9
            case Types.ROWID:
                return "ROWID"; // -8
            case Types.TINYINT:
                return "TINYINT"; // -6
            case Types.VARBINARY:
                return "VARBINARY"; // -3

            // Positive type codes
            case Types.ARRAY:
                return "ARRAY"; // 2003
            case Types.BLOB:
                return "BLOB"; // 2004
            case Types.BOOLEAN:
                return "BOOLEAN"; // 16
            case Types.CHAR:
                return "CHAR"; // 1
            case Types.CLOB:
                return "CLOB"; // 2005
            case Types.DATALINK:
                return "DATALINK"; // 70
            case Types.DATE:
                return "DATE"; // 91
            case Types.DECIMAL:
                return "DECIMAL"; // 3
            case Types.DISTINCT:
                return "DISTINCT"; // 2001
            case Types.DOUBLE:
                return "DOUBLE"; // 8
            case Types.FLOAT:
                return "FLOAT"; // 6
            case Types.INTEGER:
                return "INTEGER"; // 4
            case Types.JAVA_OBJECT:
                return "JAVA_OBJECT"; // 2000
            case Types.NCLOB:
                return "NCLOB"; // 2011
            case Types.NULL:
                return "NULL"; // 0
            case Types.NUMERIC:
                return "NUMERIC"; // 2
            case Types.OTHER:
                return "OTHER"; // 1111
            case Types.REAL:
                return "REAL"; // 7
            case Types.REF:
                return "REF"; // 2006
            case Types.REF_CURSOR:
                return "REF_CURSOR"; // 2012
            case Types.SMALLINT:
                return "SMALLINT"; // 5
            case Types.SQLXML:
                return "SQLXML"; // 2009
            case Types.STRUCT:
                return "STRUCT"; // 2002
            case Types.TIME:
                return "TIME"; // 92
            case Types.TIME_WITH_TIMEZONE:
                return "TIME_WITH_TIMEZONE"; // 2013
            case Types.TIMESTAMP:
                return "TIMESTAMP"; // 93
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return "TIMESTAMP_WITH_TIMEZONE"; // 2014
            case Types.VARCHAR:
                return "VARCHAR"; // 12

            default:
                return "UNKNOWN";
        }
    }

    /**
     * Closes the given ResultSet.
     * Returns true if closed successfully, or false if an exception is thrown.
     *
     * @param rs The ResultSet to close.
     * @return True if the ResultSet was closed successfully, false otherwise.
     */
    private static boolean CloseResultSet(ResultSet rs) {
        try {
            rs.close();
            return true;
        } catch (Exception e) {
            Common.Log(e.getMessage());
            return false;
        }
    }

    /**
     * Verifies that a ResultSet is non-null, open, and has not already been consumed.
     *
     * For scrollable ResultSets, the cursor must still be before the first row.
     * For forward-only ResultSets, cursor position cannot be reliably checked,
     * so this method returns true as long as the ResultSet is non-null and open.
     *
     * @param rs The ResultSet to verify.
     * @return True if the ResultSet appears ready to use, false otherwise.
     */
    private static boolean VerifyResultSet(ResultSet rs) {
    try {
        if (rs == null) {
            Common.Log("Warning: ResultSet is null.");
            return false;
        }

        if (rs.isClosed()) {
            Common.Log("Warning: ResultSet is closed.");
            return false;
        }

        int type = rs.getType();

        if (type == ResultSet.TYPE_FORWARD_ONLY) {
            // Cannot reliably determine cursor position
            //Common.Log("Warning: Forward-only ResultSet - cannot verify cursor state.");
            return true; // cursor position is not checkable for forward-only ResultSets
        }

        // For scrollable result sets, this is valid
        if (!rs.isBeforeFirst()) {
            Common.Log("Warning: ResultSet has already been read or is empty.");
            return false;
        }

        return true;

    } catch (SQLException e) {
        Common.Log("VerifyResultSet error: " + e.getMessage());
        return false;
    }
}
}