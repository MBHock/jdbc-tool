/**
 * 
 */
package de.hock.run;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import de.hock.argument.ArgumentParser;
import de.hock.argument.ArgumentReader;
import de.hock.database.select.DatabaseReader;
import de.hock.database.select.OracleDatabaseReader;
import de.hock.database.write.DatabaseWriter;
import de.hock.database.write.OracleDatabaseWriter;
import de.hock.dataset.importer.DatasetImporter;
import de.hock.dataset.importer.FileImporter;

/**
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 */
public class MainClass {

  private static final Logger logger = Logger.getLogger(MainClass.class.getSimpleName());
  private static final Properties properties = new Properties();

  private static final ArgumentReader argReader = new ArgumentReader();
  private static final ArgumentParser argParser = new ArgumentParser();
  private static final FileImporter importDataset = new DatasetImporter();

  public static void main(String[] args) throws Exception {
    Instant start = Instant.now();
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    logger.log(Level.INFO, "{0}, {1}{2}: Started: {3}", new Object[] { runtime.getVmVendor(), runtime.getVmName(), runtime.getVmVersion(),
        LocalDateTime.ofEpochSecond(runtime.getStartTime() / 1000, 0, ZoneOffset.UTC) });

    argParser.validateArgument(args);

    argReader.loadDefaultConfig(properties);

    argReader.readArguments(args, properties);

    setGlobalLogLevel();

    DatabaseReader readDatabase = new OracleDatabaseReader(properties);
    if (readDatabase.numberOfTaskExecution() > 0) {
      TaskExecutor taskExecutor = new TaskExecutor(readDatabase.numberOfWorkerThread());
      taskExecutor.executeTask(readDatabase, readDatabase.numberOfTaskExecution());
    }

    DatabaseWriter writeDatabase = new OracleDatabaseWriter(properties);
    if (writeDatabase.numberOfTaskExecution() > 0) {
      TaskExecutor writerExecutor = new TaskExecutor(writeDatabase.numberOfWorkerThread());
      writerExecutor.executeTask(writeDatabase, writeDatabase.numberOfTaskExecution());
    }

    importDataset.importFile(properties);

    Duration duration = Duration.between(start, Instant.now());
    logger.log(Level.INFO, "Elapsed duration since start: {0}H {1}M {2}S",
        new Object[] { duration.toHours(), duration.toMinutes(), duration.toMillis() / 1000 });
  }

  private static void setGlobalLogLevel() {
    Logger rootLogger = LogManager.getLogManager().getLogger("");
    Level level = Level.parse(properties.getProperty(ConfigProperty.LOG_LEVEL.propKey(), ConfigProperty.LOG_LEVEL.defaultValue()));
    rootLogger.setLevel(level);
    for (Handler handler : rootLogger.getHandlers()) {
      handler.setLevel(level);
    }
  }
}
