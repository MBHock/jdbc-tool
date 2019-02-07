/**
 * 
 */
package de.hock.database.write;

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
import de.hock.run.ConfigProperty;

/**
 * @author hockm002
 *
 */
public class OracleDatabaseWriter implements DatabaseWriter {

  private static final int BATCH_SIZE = 5000;

  private static AtomicInteger threadCounter = new AtomicInteger(0);

  private static final Logger logger = Logger.getLogger(OracleDatabaseWriter.class.getSimpleName());
  private final String sqlStatement;
  private final Properties properties;
  private final Integer threadCount;
  private final Integer repeatCount;
  private final List<Object> placeholder;
  private final Boolean isValidStatement;

  public OracleDatabaseWriter(Properties properties) {
    this.properties = properties;
    sqlStatement = properties.getProperty(ConfigProperty.WRITE_STATEMENT.propKey());
    isValidStatement = Objects.nonNull(sqlStatement) && !sqlStatement.isEmpty();

    String filter = properties.getProperty(ConfigProperty.WRITE_FILTER.propKey());
    placeholder = Objects.nonNull(filter) && !filter.isEmpty() ? Arrays.asList(filter.split(",")).stream().map(arg -> (Object) arg).collect(Collectors.toList())
        : Collections.emptyList();

    String count = (String) properties.get(ConfigProperty.THREAD_COUNT.propKey());
    threadCount = Objects.isNull(count) || "".equals(count) ? 1 : Integer.parseInt(count);

    count = (String) properties.get(ConfigProperty.REPEAT_COUNT.propKey());
    repeatCount = Objects.isNull(count) || "".equals(count) ? 0 : Integer.parseInt(count);

  }

  /*
   * (non-Javadoc)
   * 
   * @see de.hock.database.write.DatabaseWriter#executeStatement(java.util.
   * Properties)
   */
  @Override
  public void executeStatement() {

    if (isValidStatement) {
      DatabaseOperation db = new OracleDatabaseOperation(ProduceOracleDataSource.getOraclePool(properties));

      logger.log(Level.FINER, "Executing statement {0}", sqlStatement);
      try {
        if (isSelectStatement()) {

          Integer currentExecution = threadCounter.incrementAndGet();
          List<List<Object>> values = createPlaceholderValues(currentExecution);
          db.batchInsert(sqlStatement, values);
          logger.log(Level.INFO, "ExecutionId: {0}, {1} data has been inserted", new Object[] { currentExecution, values.size() });

        } else {
          db.executeDDL(sqlStatement, placeholder);
        }
      } catch (SQLException sqlEx) {
        logger.log(Level.SEVERE, String.format("Exception: while executing %s,filter %s,", sqlStatement, placeholder), sqlEx);
      }
    }
  }

  @Override
  public void run() {
    Instant start = Instant.now();
    executeStatement();
    Duration duration = Duration.between(start, Instant.now());
    logger.log(Level.INFO, "All write statement executed in {0}", duration);
  }

  @Override
  public Integer numberOfWorkerThread() {
    return threadCount;
  }

  @Override
  public Integer numberOfTaskExecution() {

    int numberoftaskCount = 0;

    if (!isSelectStatement()) {
      numberoftaskCount = repeatCount;
    } else {
      if (repeatCount / BATCH_SIZE == 0) {
        numberoftaskCount = 1;
      } else {
        int mod = repeatCount / BATCH_SIZE;
        numberoftaskCount = repeatCount % BATCH_SIZE == 0 ? mod : mod + 1;
      }
    }

    System.out.println("numberoftaskCount: " + numberoftaskCount);
    return numberoftaskCount;
  }

  private boolean isSelectStatement() {
    String writeStatement = properties.getProperty(ConfigProperty.WRITE_STATEMENT.propKey());

    return Objects.nonNull(writeStatement) && !writeStatement.isEmpty()
        && writeStatement.matches("^[iInNsSeErRtT]+\\s+[iInNtToO]+\\s+[a-zA-Z0-9_]+\\s+\\([a-zA-Z0-9_,]+\\)\\s[vVaAlLuUeEsS]+\\((.*)");

  }

  private synchronized List<List<Object>> createPlaceholderValues(Integer count) {
    int packateSize = BATCH_SIZE;

    if (repeatCount < BATCH_SIZE) {
      packateSize = repeatCount;
    } else if (repeatCount > (count * BATCH_SIZE)) {
      packateSize = BATCH_SIZE;
    } else if (repeatCount < (count * BATCH_SIZE)) {
      packateSize = (repeatCount - BATCH_SIZE * (count - 1));
    }

    return Collections.nCopies(packateSize, placeholder);
  }
}
