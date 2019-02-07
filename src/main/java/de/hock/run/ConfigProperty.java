package de.hock.run;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 
 * @author Mojammal Hock
 * @version 1.0
 * @since 1.0
 *
 */
public enum ConfigProperty {

  //@formatter:off
  DATA_SOURCE_CLASS("oracle.jdbc.pool.OracleDataSource", "oracle.jdbc.pool.OracleDataSource"),
  DATABASE_URL("db.url", ""),
  DATABASE_USERNAME("db.username", ""),
  DATABASE_BASIS_USERNAME("db.basis.username", ""),
  DATABASE_PASSWORD("db.password", ""),
  CONFIG_FILE("ConfigFile", ""),
  IMPORT_DIR("ImportPath", ""),
  FILE_PATTERN("FilePattern", ""),
  REPEAT_COUNT("RepeatCount", "0"),
  THREAD_COUNT("ThreadCount", "5"),
  PAUSE_TIME_FOR_REPEAT("PauseTime", "0s"),
  INIT_POOL("InitialPoolSize", "5"),
  MIN_POOL("MinPoolSize", "5"),
  MAX_POOL("MaxPoolSize", "10"),
  LOG_LEVEL("LogLevel", "INFO"),
  WRITE_STATEMENT("WriteStatement", ""),
  WRITE_FILTER("WriteFilter",""),
  SELECT_STATEMENT("SelectStatement", ""),
  SELECT_FILTER("SelectFilter","");
//@formatter:on

  private static final Map<String, ConfigProperty> propertyHolder = new HashMap<>();

  static {
    Stream.of(ConfigProperty.values()).forEach(prop -> propertyHolder.put(prop.propKey(), prop));
  }

  private String propertyKey;
  private String defaultpropValue;
  
  private ConfigProperty(String propertyName, String defaultValue) {
    this.propertyKey= propertyName;
    this.defaultpropValue = defaultValue;
  }

  /**
   * @return the propertyName
   */
  public String propKey() {
    return propertyKey;
  }

  public String defaultValue() {
    return defaultpropValue;
  }

  public static ConfigProperty getConfig(String propKey) {
    return propertyHolder.get(propKey);
  }
}