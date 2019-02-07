/**
 * 
 */
package de.hock.database.select;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;

import de.hock.jdbc.SQLTypeMapper;
import de.hock.jdbc.RowMapper;

/**
 * 
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.com">Mojammal Hock</a>
 *
 */
public class StringRowMapper implements RowMapper<String> {

  private Map<String, Integer> columns;
  private String tableName;
  private SQLTypeMapper typeMapper = new SQLTypeMapper();

  /*
   * (non-Javadoc)
   * 
   * @see
   * de.hock.jdbc.DatenbankZeilenMapper#baueZeilenZuObjektUm(java.sql.ResultSet)
   */
  @Override
  public String mapRow(ResultSet resultSet) throws SQLException {
    if (Objects.isNull(columns) || Objects.isNull(tableName)) {
      columns = getColumnNames(resultSet);
      tableName = getTableName(resultSet);
    }

    StringJoiner joiner = new StringJoiner(", ");
    for (Entry<String, Integer> columninfo : getColumnNames().entrySet()) {
      joiner.add(String.join("=", columninfo.getKey(), String.valueOf(typeMapper.getObject(resultSet, columninfo.getKey(), columninfo.getValue()))));
    }

    return joiner.toString();
  }

  @Override
  public Map<String, Integer> getColumnNames() {
    return columns;
  }

  @Override
  public String getTableName() {
    return tableName;
  }
}
