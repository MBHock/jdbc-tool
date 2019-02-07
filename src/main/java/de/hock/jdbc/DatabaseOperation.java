/*
 *
 * Copyright (c) 2017 Bundesagentur fuer Arbeit. All Rights Reserved
 *
 */
package de.hock.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Eine hilfe Klasse f&uuml;r Plain Datenbank Operation.
 *
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 */
public interface DatabaseOperation extends Transactional {

  public <T> void batchInsert(List<T> typeObjects) throws SQLException, IllegalAccessException;

  public void batchInsert(String query, List<List<Object>> rowsOfColumnValues) throws SQLException;

  public int batchUpdate(String query, List<List<Object>> rowsOfColumnValues) throws SQLException;

  public int delete(String query, List<Object> columnValues) throws SQLException;

  public void executeDDL(List<String> queries) throws SQLException;

  public void executeDDL(String query, List<Object> placeholder) throws SQLException;

  public void insert(String query, List<Object> columnValues) throws SQLException;

  public <T> void insert(T typedObject) throws SQLException, IllegalAccessException;

  public <T> T selectOneRow(String query, List<Object> columnValues, Class<T> clazz) throws SQLException;

  public <T> ReadItem<T> selectRow(String query, List<Object> columnValues, Class<T> clazz) throws SQLException;

  public <T> ReadItem<T> selectRow(String query, List<Object> columnValues, RowMapper<T> mapper) throws SQLException;

  public <T> List<T> selectRows(String query, List<Object> columnValues, Class<T> clazz) throws SQLException;

  public <T> List<T> selectRows(String query, List<Object> columnValues, RowMapper<T> mapper) throws SQLException;

  public int update(String query, List<Object> columnValues) throws SQLException;

  public void batchInsert(String tableName, List<String> columns, List<Map<String, String>> rows) throws SQLException;

  // public <T> List<T> selectRows(String selectSql, Map<String, Object>
  // placeholder, RowMapper<T> mapper) throws SQLException;

}
