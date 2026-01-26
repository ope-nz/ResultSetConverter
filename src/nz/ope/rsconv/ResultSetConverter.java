package nz.ope.resultsetconverter;

import anywheresoftware.b4a.BA;
import anywheresoftware.b4a.BA.Author;
import anywheresoftware.b4a.BA.DependsOn;
import anywheresoftware.b4a.BA.ShortName;
import anywheresoftware.b4a.BA.Version;
import anywheresoftware.b4a.keywords.Common;
import anywheresoftware.b4a.objects.collections.Map;
import anywheresoftware.b4a.objects.collections.List;

import java.util.ArrayList;

import java.sql.*;
import java.text.SimpleDateFormat;

@Version(1.0f)
@ShortName("ResultSetConverter")

public class ResultSetConverter {

    /**
     * Returns a Map containing the column names from the ResultSet as the keys, and
     * the corresponding JDBC type name as the value.
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
                String columnName = metaData.getColumnLabel(i);
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
                String columnName = metaData.getColumnLabel(i);
                Result.Add(columnName);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        return Result;
    }

    /**
     * Converts a JDBC ResultSet to an ArrayList of Maps, with the column names used
     * as the keys in the Map.
     * 
     * The column type is used to determine the type of the value to be added to the
     * Map (e.g. Integer for Types.INTEGER, etc.).
     * 
     * If the column type is not recognized, the value is added as a String.
     * 
     * Example output:
     * [{"Name": "John", "Age": 30}, {"Name": "Jane", "Age": 25}]
     * 
     * @param rs The ResultSet to convert.
     * @return An ArrayList containing the values from the ResultSet.
     */
    public static List ToListOfMaps(ResultSet rs) {
        List Result = new List();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map Row = new Map();
                Row.Initialize();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    int columnType = metaData.getColumnType(i);
                    Object value = getResultSetValue(rs, i, columnType);
                    Row.Put(columnName, value);
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
     * Converts a JDBC ResultSet to an ArrayList of ArrayLists, with the first
     * ArrayList containing the column names (if includeHeader is true), and
     * subsequent ArrayLists containing the values from each row.
     * 
     * The column type is used to determine the type of the value to be added to the
     * ArrayList (e.g. Integer for Types.INTEGER, etc.).
     * 
     * If the column type is not recognized, the value is added as a String.
     * 
     * Example output:
     * [["Name", "Age"], ["John", 30], ["Jane", 25]]
     * 
     * @param rs            The ResultSet to convert.
     * @param includeHeader If true, the first ArrayList will contain the
     *                      column names.
     * @return An ArrayList containing the values from the ResultSet.
     */
    public static List ToListOfLists(ResultSet rs, boolean includeHeader) {
        List Result = new List();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            if (includeHeader) {
                ArrayList Row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Row.add(columnName);
                }
                Result.Add(Row);
            }

            while (rs.next()) {
                ArrayList Row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    int columnType = metaData.getColumnType(i);
                    Object value = getResultSetValue(rs, i, columnType);
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
     * Converts a JDBC ResultSet to an ArrayList, with the first column's
     * values used as the elements in the ArrayList.
     * 
     * The column type of the first column is used to determine the type of the
     * value to be added to the ArrayList (e.g. Integer for Types.INTEGER, etc.).
     * If the column type is not recognized, the value is added as a String.
     * 
     * Expects a ResultSet with a single column.
     * 
     * Example output:
     * ["John", "Jane"]
     * 
     * @param rs The ResultSet to convert.
     * @return An ArrayList containing the values from the ResultSet.
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
    public static Map ToMap(ResultSet rs) {
        Map Result = new Map();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Object key = getResultSetValue(rs, 1, metaData.getColumnType(1));

                Map Row = new Map();
                Row.Initialize();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    int columnType = metaData.getColumnType(i);
                    Object value = getResultSetValue(rs, i, columnType);
                    Row.Put(columnName, value);
                }
                Result.Put(key, Row);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);
        return Result;
    }

    /**
     * Converts a JDBC ResultSet into a Map containing two ArrayLists:
     * 
     * One for series values and another for data values.
     * 
     * The first column's values are added to the series ArrayList.
     * 
     * The second column's values are added to the data ArrayList.
     * 
     * Expects a ResultSet with two columns.
     * 
     * Example output:
     * {
     * "series": [series_value1, series_value2, ...],
     * "data": [data_value1, data_value2, ...]
     * }
     * 
     * @param rs The ResultSet to convert.
     * @return A Map with keys "series" and "data", containing ArrayLists of the
     *         respective values.
     */

    public static Map ToDataSeries(ResultSet rs) {
        ArrayList Data = new ArrayList<>();
        ArrayList Series = new ArrayList<>();

        Map Result = new Map();
        Result.Initialize();

        if (VerifyResultSet(rs) == false)
            return Result;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnType1 = metaData.getColumnType(1);
            int columnType2 = metaData.getColumnType(2);
            while (rs.next()) {
                Object series_value = getResultSetValue(rs, 1, columnType1);
                Series.add(series_value);

                Object data_value = getResultSetValue(rs, 2, columnType2);
                Data.add(data_value);
            }

            Result.Put("series", Series);
            Result.Put("data", Data);

        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);
        return Result;
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
            int columnType = metaData.getColumnType(2);

            while (rs.next()) {
                String key = rs.getString(1);
                Object value = getResultSetValue(rs, 2, columnType);
                Result.Put(key, value);
            }
        } catch (Exception e) {
            Common.Log(e.getMessage());
        }
        CloseResultSet(rs);
        return Result;
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
     * Retrieves the value of a column in a ResultSet, given the column index and
     * JDBC type.
     * If the column type is not recognized, the value is returned as a String.
     * If the value is null, null is returned.
     * 
     * @param rs          The ResultSet to retrieve the value from.
     * @param columnIndex The index of the column to retrieve.
     * @param columnType  The JDBC type of the column.
     * @return The value of the column, or null if the value is null or the column
     *         type is not recognized.
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
     * Returns true if the ResultSet was closed successfully, false otherwise.
     * If the ResultSet is null, or if it has already been read or is empty, a
     * warning
     * message is logged and false is returned.
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
     * Verifies that the given ResultSet is valid and ready to be used.
     * It checks if the ResultSet is null, and if it has already been read or is
     * empty.
     * If any of these conditions are true, it logs a warning message and returns
     * false.
     * Otherwise, it returns true.
     *
     * @param rs the ResultSet to verify
     * @return true if the ResultSet is valid and ready to be used, false otherwise
     */
    private static boolean VerifyResultSet(ResultSet rs) {
        try {
            if (rs == null) {
                Common.Log("Warning: ResultSet is null.");
                return false;
            }
            if (!rs.isBeforeFirst()) {
                Common.Log("Warning: ResultSet has already been read or is empty.");
                return false;
            }
            return true;
        } catch (Exception e) {
            Common.Log(e.getMessage());
            return false;
        }
    }
}