/**
 *
 * Copyright (c) 2018 Bundesagentur fuer Arbeit. All Rights Reserved
 *
 */
package de.hock.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Ein Implementation Klasse zum ReadItem Interface.
 *
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 */
public class RowWrapper<T> implements ReadItem<T> {

  private static final Logger logger = Logger.getLogger(RowWrapper.class.getSimpleName());;

  private Connection connection;
  private RowMapper<T> mapper;
  private ResultSet resultSet;
  private Statement statement;

  @Override
  public T nextItem() throws SQLException {
    if (resultSet.next()) {
      return mapper.mapRow(resultSet);
    }

    return null;
  }

  @Override
  public void close() {
    try {
      if (Objects.nonNull(resultSet)) {
        resultSet.close();
      }
    } catch (SQLException sqlEx) {
      logger.log(Level.SEVERE, "Fehler beim ResultSet zu schlißen.", sqlEx);
    }

    try {
      if (Objects.nonNull(statement)) {
        statement.close();
      }
    } catch (SQLException sqlEx) {
      logger.log(Level.SEVERE, "Fehler beim PreparedStatement zu schlißen.", sqlEx);
    }

    try {
      if (Objects.nonNull(connection)) {
        connection.close();
      }
    } catch (SQLException sqlEx) {
      logger.log(Level.SEVERE, "Fehler beim Connection zu schlißen.", sqlEx);
    }
  }

  public void setMapper(RowMapper<T> mapper) {
    this.mapper = mapper;
  }

  public void setResultSet(ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {
    this.resultSet = resultSet;
    this.connection = connection;
    this.statement = preparedStatement;
  }

}
