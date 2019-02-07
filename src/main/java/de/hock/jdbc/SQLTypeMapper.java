/*
 *
 * Copyright (c) 2017 Bundesagentur fuer Arbeit. All Rights Reserved
 *
 */
package de.hock.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Ein Java Type zu SQL Datentyp mapper
 *
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 */
public class SQLTypeMapper {

  private static final String BYTE_ARRAY = "byte[]";
  private static final String LOCALDATETIME = "java.time.LocalDateTime";
  private static final String LOCALTIME = "java.time.LocalTime";
  private static final String LOCALDATE = "java.time.LocalDate";
  private static final String DOUBLE = "java.lang.Double";
  private static final String FLOAT = "java.lang.Float";
  private static final String LONG = "java.lang.Long";
  private static final String INTEGER = "java.lang.Integer";
  private static final String SHORT = "java.lang.Short";
  private static final String BYTE = "java.lang.Byte";
  private static final String BOOLEAN = "java.lang.Boolean";
  private static final String BIGDECIMAL = "java.math.BigDecimal";
  private static final String STRING = "java.lang.String";
  private IllegalArgumentException exception = null;

  private static EntityReflection reflection = new EntityReflection();

  private static ParameterValidator validator = new ParameterValidator();

  /**
   * Dieses Hilfemethode setzt die angegebe Wert in der
   * {@link PreparedStatement} an entsprechende index.
   *
   * @param preparedStatement
   *          ein {@link PreparedStatement}-Objekt
   * @param placeholderIndex
   *          placeholder Index in der {@link PreparedStatement}-Objekt
   * @param spaltewert
   *          ein Wert, womit platzhalter wird durch gesetzt.
   *
   * @throws SQLException
   *           falls ein Datenbank fehler tritt auf.
   * @throws EntitityWrapperException
   *           falls ein Fehler tritt auf.
   */
  public void setObjectValue(PreparedStatement preparedStatement, Integer placeholderIndex, Object spaltewert) throws SQLException {
    validator.verifyArguments(preparedStatement, "PreparedStatement darf nicht null sein!");
    validator.verifyArguments(placeholderIndex, "Platzhalter index darf nicht null sein!");

    if (Objects.isNull(spaltewert)) {
      preparedStatement.setNull(placeholderIndex, JDBCType.NULL.getVendorTypeNumber());
      return;
    }

    String typeName = spaltewert.getClass().getTypeName();
    switch (typeName) {
    case STRING:
      preparedStatement.setString(placeholderIndex, (String) spaltewert);
      break;
    case BIGDECIMAL:
      preparedStatement.setBigDecimal(placeholderIndex, (BigDecimal) spaltewert);
      break;
    case BOOLEAN:
      preparedStatement.setBoolean(placeholderIndex, (Boolean) spaltewert);
      break;
    case BYTE:
      preparedStatement.setByte(placeholderIndex, (Byte) spaltewert);
      break;
    case SHORT:
      preparedStatement.setShort(placeholderIndex, (Short) spaltewert);
      break;
    case INTEGER:
      preparedStatement.setInt(placeholderIndex, (Integer) spaltewert);
      break;
    case LONG:
      preparedStatement.setLong(placeholderIndex, (Long) spaltewert);
      break;
    case FLOAT:
      preparedStatement.setFloat(placeholderIndex, (Float) spaltewert);
      break;
    case DOUBLE:
      preparedStatement.setDouble(placeholderIndex, (Double) spaltewert);
      break;
    case LOCALDATE:
      LocalDate localdate = (LocalDate) spaltewert;
      preparedStatement.setDate(placeholderIndex, Date.valueOf(localdate));
      break;
    case LOCALTIME:
      LocalTime localtime = (LocalTime) spaltewert;
      preparedStatement.setTime(placeholderIndex, Time.valueOf(localtime));
      break;
    case LOCALDATETIME:
      LocalDateTime localdatetime = (LocalDateTime) spaltewert;
      preparedStatement.setTimestamp(placeholderIndex, Timestamp.valueOf(localdatetime));
      break;
    case BYTE_ARRAY:
      byte[] byteWert = (byte[]) spaltewert;
      preparedStatement.setBytes(placeholderIndex, byteWert);
      break;
    default:
      if (reflection.isEnum(spaltewert.getClass())) {
        preparedStatement.setString(placeholderIndex, spaltewert.toString());
      } else {
        throw new IllegalArgumentException(MessageFormat.format("Es gibt keine mapper für Java type {0}", typeName));
      }
    }
  }

  /**
   * Dieses Hilfemethode liest ein <code>classType</code> aus der
   * {@link ResultSet} mit angegebenen <code>columnName</code>.
   *
   * @param resultSet
   *          ein {@link ResultSet}-Objekt
   * @param columnName
   *          ein Wert aus der {@link ResultSet} mit dieser Name wird gelesen.
   * @param classType
   *          Objekt-Type, dass Spaltenwert zu richtige Typ zu mappen.
   *
   * @return ein <code>classType</code> aus der {@link ResultSet}.
   *
   * @throws SQLException
   *           falls ein Datenbank fehler tritt auf.
   * @throws InvocationTargetException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws EntitityWrapperException
   *           falls ein Fehler tritt auf.
   *
   */
  public <T> Object getObject(ResultSet resultSet, String columnName, Class<T> classType)
      throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    validator.verifyArguments(resultSet, columnName, classType);

    switch (classType.getTypeName()) {
    case STRING:
      return resultSet.getString(columnName);
    case BIGDECIMAL:
      return resultSet.getBigDecimal(columnName);
    case BOOLEAN:
      return resultSet.getBoolean(columnName);
    case BYTE:
      return resultSet.getByte(columnName);
    case SHORT:
      return resultSet.getShort(columnName);
    case INTEGER:
      return resultSet.getInt(columnName);
    case LONG:
      return resultSet.getLong(columnName);
    case FLOAT:
      return resultSet.getFloat(columnName);
    case DOUBLE:
      return resultSet.getDouble(columnName);
    case LOCALDATE:
      return resultSet.getDate(columnName).toLocalDate();
    case LOCALTIME:
      return resultSet.getTime(columnName).toLocalTime();
    case LOCALDATETIME:
      return resultSet.getTimestamp(columnName).toLocalDateTime();
    case BYTE_ARRAY:
      return resultSet.getBytes(columnName);
    default:
      if (reflection.isEnum(classType)) {
        return getEnumValue(resultSet, columnName, classType);
      }

      return null;
    }
  }

  /**
   * 
   * @param resultSet
   * @param columnName
   * @param sqlType
   * @return
   * @throws SQLException
   */
  public Object getObject(ResultSet resultSet, String columnName, Integer sqlType) throws SQLException {
    switch (sqlType) {
    case java.sql.Types.ARRAY:
      return resultSet.getArray(columnName);
    case java.sql.Types.BIT:
    case java.sql.Types.SMALLINT:
    case java.sql.Types.INTEGER:
      return resultSet.getInt(columnName);
    case java.sql.Types.BIGINT:
    case java.sql.Types.DECIMAL:
    case java.sql.Types.NUMERIC:
      return resultSet.getLong(columnName);
    case java.sql.Types.DOUBLE:
    case java.sql.Types.FLOAT:
      return resultSet.getDouble(columnName);
    case java.sql.Types.BOOLEAN:
      return resultSet.getBoolean(columnName);
    case java.sql.Types.CHAR:
      return resultSet.getCharacterStream(columnName);
    case java.sql.Types.CLOB:
    case java.sql.Types.LONGNVARCHAR:
    case java.sql.Types.LONGVARCHAR:
      return resultSet.getString(columnName);
    case java.sql.Types.BLOB:
      Blob blob = resultSet.getBlob(columnName);
      byte[] bytes = blob.getBytes(1, Long.valueOf(blob.length()).intValue());
      blob.free();
      return Arrays.toString(bytes);
    default:
      return resultSet.getObject(columnName);
    }
  }

  /**
   * Dieses Hilfemethode liest ein <code>classType</code>-Enum aus der
   * {@link ResultSet} mit angegebenen <code>columnName</code>.
   *
   * @param resultSet
   *          ein {@link ResultSet}-Objekt
   * @param columnName
   *          ein Wert aus der {@link ResultSet} mit dieser Name wird gelesen.
   * @param classType
   *          Objekt-Type, dass Spaltenwert zu richtige Typ zu mappen.
   *
   * @return ein <code>classType</code>-Enum aus der {@link ResultSet}.
   *
   * @throws SQLException
   *           falls ein Datenbank fehler tritt auf.
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws InvocationTargetException
   * @throws IllegalArgumentException
   * @throws EntitityWrapperException
   *           falls ein Fehler tritt auf.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T> Object getEnumValue(ResultSet resultSet, String columnName, Class<T> classType)
      throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    validator.verifyArguments(resultSet, columnName, classType);

    String enumValue = resultSet.getString(columnName);
    Object enumObject = null;

    try {
      Class type = Class.forName(classType.getTypeName());
      enumObject = Enum.valueOf(type, enumValue);
    } catch (IllegalArgumentException iae) {
      exception = iae;
    }

    if (Objects.isNull(enumObject)) {
      Optional<Method> firstMethod = Stream.of(classType.getDeclaredMethods()).filter(m -> Objects.equals("fromValue", m.getName())).findFirst();
      Method method = firstMethod.orElseThrow(() -> new IllegalArgumentException("Neither Enum#valueof() nor T#fromValue() is present.", exception));
      enumObject = method.invoke(null, enumValue);
    }

    return enumObject;
  }
}
