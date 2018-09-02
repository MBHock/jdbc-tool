/**
 * 
 */
package de.hock.dataset.importer;

import java.io.IOException;
import java.util.Properties;

/**
 * @author Mojammal Hock
 * @version 1.0
 * @since 1.0
 *
 */
public interface FileImporter {


  /**
   * 
   * @param properties
   * @throws IOException
   */
  public void importFile(Properties properties) throws IOException;

}
