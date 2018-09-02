/**
 * 
 */
package de.hock.database.select;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import de.hock.jdbc.DatabaseOperation;
import de.hock.jdbc.OracleDatabaseOperation;
import de.hock.jdbc.ProduceOracleDataSource;
import de.hock.jdbc.RowMapper;
import de.hock.run.ConfigProperty;

/**
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.com">Mojammal Hock</a>
 * 
 * @version 1.0
 * @since 1.0
 *
 */
public class OracleDatabaseReader implements DatabaseReader {

  private static AtomicInteger threadCounter = new AtomicInteger(0);
  private static final Logger logger = Logger.getLogger(OracleDatabaseReader.class.getSimpleName());
  private final String sqlStatement;
  private final Properties properties;
  private final List<Object> placeholder;
  private final Boolean isValidStatement;

  public OracleDatabaseReader(Properties properties) {
    this.properties = properties;

    sqlStatement = properties.getProperty(ConfigProperty.SELECT_STATEMENT.propKey());
    isValidStatement = Objects.nonNull(sqlStatement) && !sqlStatement.isEmpty();

    String filter = properties.getProperty(ConfigProperty.SELECT_FILTER.propKey());
    placeholder = Objects.nonNull(filter) && !filter.isEmpty() ? Arrays.asList(filter.split(",")).stream().map(arg -> (Object) arg).collect(Collectors.toList())
        : Collections.emptyList();

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * de.hock.database.select.DatabaseReader#executeSelectStatement(java.util.
   * Properties)
   */
  @Override
  public void executeSelectStatement() {
    RowMapper<String> mapper = new StringRowMapper();
    DatabaseOperation db = new OracleDatabaseOperation(ProduceOracleDataSource.getOraclePool(properties));
    try {
      List<String> results = db.selectRows(sqlStatement, placeholder, mapper);
      String result = results.parallelStream().collect(Collectors.joining("\n"));
      System.out.println(String.format("=== Repeat coutn %d, Tablename %s === \n%s", threadCounter.getAndIncrement(), mapper.getTableName(), result));
    } catch (SQLException sqlEx) {
      logger.log(Level.SEVERE, String.format("Exception: while executing %s, filter %s,", sqlStatement, placeholder), sqlEx);
    }
  }

  @Override
  public void run() {
    Instant start = Instant.now();
    executeSelectStatement();
    Duration duration = Duration.between(start, Instant.now());
    logger.log(Level.INFO, "Select statement executed in {0}", duration);
  }

  @Override
  public Integer numberOfWorkerThread() {
    if (isValidStatement) {
      String threadCount = (String) properties.get(ConfigProperty.THREAD_COUNT.propKey());
      return Objects.isNull(threadCount) || "".equals(threadCount) ? 1 : Integer.parseInt(threadCount);
    }

    return 0;
  }

  @Override
  public Integer numberOfTaskExecution() {
    if (isValidStatement) {
      String repeatCount = (String) properties.get(ConfigProperty.REPEAT_COUNT.propKey());
      return Objects.isNull(repeatCount) || "".equals(repeatCount) ? 0 : Integer.parseInt(repeatCount);
    }

    return 0;
  }

  // Integer repeatCount =
  // Integer.valueOf(properties.getProperty(ConfigProperty.REPEAT_COUNT.getKey(),
  // "0"));
  // for (int loopCount = 0; loopCount < repeatCount; loopCount++) {
  // }
  // Map<String, Object> placeholder = Objects.nonNull(placeholderValue) &&
  // !placeholderValue.isEmpty() ?
  // Arrays.asList(placeholderValue.split(",")).stream()
  // .map(element ->
  // element.split("=")).map(Arrays::asList).collect(Collectors.toMap(values ->
  // values.get(0), values -> values.get(1)))
  // : Collections.emptyMap();

}
