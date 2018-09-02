/**
 * 
 */
package de.hock.dataset.importer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import de.hock.filehandler.FileSystemHandler;
import de.hock.jdbc.DatabaseOperation;
import de.hock.jdbc.OracleDatabaseOperation;
import de.hock.jdbc.ProduceOracleDataSource;
import de.hock.run.ConfigProperty;

/**
 * @author Mojammal Hock
 * @version 1.0
 * @since 1.0
 *
 */
public class DatasetImporter implements FileImporter {

  private static final Logger logger = Logger.getLogger(DatasetImporter.class.getSimpleName());

  /*
   * (non-Javadoc)
   * 
   * @see de.hock.run.DatasetProcessor#process(java.util.Properties)
   */
  @Override
  public void importFile(Properties properties) throws IOException {

    FileSystemHandler fileHandler = new FileSystemHandler();

    List<File> files = fileHandler.readFilenamesFromPathWithEndPattern(properties.getProperty(ConfigProperty.IMPORT_DIR.propKey()),
        properties.getProperty(ConfigProperty.FILE_PATTERN.propKey()));
    DatabaseOperation db = new OracleDatabaseOperation(ProduceOracleDataSource.getOraclePool(properties));

    for (File file : files) {
      try {
        logger.log(Level.FINE, "Read dataset from {0}", file);
        XmlDatasetReader dataseReader = new XmlDatasetReader(file);

        dataseReader.readAllNodes();

        for (String tableName : dataseReader.getTableNames()) {
          db.batchInsert(tableName, dataseReader.getColumnsOfTable(tableName), dataseReader.getRowValues(tableName));
        }

      } catch (ParserConfigurationException | SAXException | SQLException ex) {

        logger.log(Level.SEVERE, String.format("Got exception while parse content of %s", file), ex);

      }
    }
  }
}
