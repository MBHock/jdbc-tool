/*
*
* Copyright (c) 2017 Bundesagentur fuer Arbeit. All Rights Reserved
*
*/

package de.hock.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * An interface which converted a Resultset object to the given java object.
 *
 * @param <T>
 *          Generic Object, A resultset will be modeled to this object.
 *
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.com">Mojammal Hock</a>
 */
public interface RowMapper<T> {

  /**
   * @param resultSet
   * @return Typed Object
   * @throws SQLException
   */
  public T mapRow(ResultSet resultSet) throws SQLException;

  public Map<String, Integer> getColumnNames();

  public String getTableName();

  default Map<String, Integer> getColumnNames(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    int columnCount = metadata.getColumnCount();
    Map<String, Integer> columnInfo = new HashMap<>(columnCount);

    for (int index = 1; index <= columnCount; index++) {
      columnInfo.put(metadata.getColumnLabel(index), metadata.getColumnType(index));
    }

    return columnInfo;
  }

  default String getTableName(ResultSet resultSet) throws SQLException {
    return resultSet.getMetaData().getTableName(1);
  }
}
