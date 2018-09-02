package de.hock.jdbc;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Eine Hilfe-Klasse, die bitet die M&ouml;glichkeit die Platzhalter in der
 * {@link PreparedStatement} durch setzen.
 *
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 *
 */
public class PlaceholderSetter {

  private static SQLTypeMapper typeMapper = new SQLTypeMapper();
  private static ParameterValidator validator = new ParameterValidator();

  /**
   *
   * @param preparedStatement
   * @param columns
   * @param type
   * @throws EntitityWrapperException
   * @throws SQLException
   */
  public <T> void setPlaceholder(PreparedStatement preparedStatement, Collection<String> columns, Map<String, Field> fields, T type)
      throws IllegalAccessException, SQLException {
    validator.verifyArguments(preparedStatement);
    validator.verifyArguments(columns);
    validator.verifyArguments(fields);
    validator.verifyArguments(type);

    int placeholderIndex = 0;

    for (String column : columns) {
      Field field = fields.get(column);
      field.setAccessible(true);
      Object spaltewert = field.get(type);
      typeMapper.setObjectValue(preparedStatement, ++placeholderIndex, spaltewert);
    }
  }

  /**
   * Ersetze Platzhalter mit Platzhalterwerte in der PreparedStatement.
   *
   * @param zeile
   * @param statement
   * @throws SQLException
   */
  public void setPlaceholder(PreparedStatement preparedStatement, List<Object> columnValues) throws SQLException {
    validator.verifyArguments(columnValues);
    int placeholderIndex = 0;

    for (Object value : columnValues) {
      typeMapper.setObjectValue(preparedStatement, ++placeholderIndex, value);
    }
  }
}