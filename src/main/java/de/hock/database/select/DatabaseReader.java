/**
 * 
 */
package de.hock.database.select;

import java.sql.SQLException;

import de.hock.run.ServiceExecutorProperties;

/**
 * @author Mojammal Hock
 * @version 1.0
 * @since 1.0
 *
 */
public interface DatabaseReader extends Runnable, ServiceExecutorProperties {

  /**
   * 
   * @param properties
   * @throws SQLException
   */
  public void executeSelectStatement() throws SQLException;

}
