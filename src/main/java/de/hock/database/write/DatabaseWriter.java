/**
 * 
 */
package de.hock.database.write;

import java.sql.SQLException;

import de.hock.run.ServiceExecutorProperties;

/**
 * @author hockm002
 *
 */
public interface DatabaseWriter extends Runnable, ServiceExecutorProperties {
  /**
   * 
   * @param properties
   * @return
   * @throws SQLException
   */
  public void executeStatement() throws SQLException;

}
