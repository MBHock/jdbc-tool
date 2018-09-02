/**
 *
 * Copyright (c) 2017 Bundesagentur fuer Arbeit. All Rights Reserved
 *
 */
package de.hock.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.PooledConnection;

import oracle.ucp.jdbc.PoolDataSource;

/**
 * Dies bitet eine Interface Connection zu handeln, was man in
 * {@link DatabaseOperation} braucht.
 *
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 * 
 */
public interface ConnectionManager {

  /**
   * Diese Methode erzeugt / zieht eine neue {@link Connection} aus der
   * {@link PooledConnection}
   *
   * @return connection {@link Connection}
   *
   * @throws SQLException,
   *           wenn keine Datenbank verbindung gibt.
   */
  public Connection getConnection() throws SQLException;

  /**
   * Diese Method schlisst die geöffnete Connection und Statement.
   *
   * @param connection
   * @param statement
   */
  public void close(Connection connection, Statement statement);

  /**
   * Schreibt alle ins Datenbank, die gerade in Connection offen sind.
   *
   * @param connection
   * @throws SQLException
   */
  public void commit(Connection connection) throws SQLException;

  /**
   * Falls eine {@link SQLException} w&auml;hrend DatenbankOperation wird
   * gefeuert, dann wird alle Operation in dieser <code>connection</code>
   * zur&uuml;ckgerollt
   *
   * @param connection
   *          {@link Connection}
   */
  public void rollback(Connection connection);

  /**
   * Ein {@link PoolDataSource} in der ConnectionManager zu setzen. Ein
   * PoolDataSource handelt sich Datenbankverbindung zum Datenbank.
   *
   * @param poolDatasource
   *          PoolDataSource zum Datenbank.
   */
  public void setDatasource(PoolDataSource poolDatasource);

  /**
   * Pruft ob der ConnectionManager ist ein Intanz von Transactional oder
   * NonTransactional .
   *
   * @return true, wenn der ConnectionManager ein Instanz von Transactional ist.
   *         false sonst
   */
  public boolean istTransactionalConnectionManager();
}
