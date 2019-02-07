/**
 * 
 */
package de.hock.argument;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import de.hock.run.ConfigProperty;

/**
 * @author Mojammal Hock
 * @version 1.0
 * @since 1.0
 */
public class ArgumentReader {

  private static final int VALUE = 1;
  private static final int KEY = 0;
  private static final Logger logger = Logger.getLogger(ArgumentReader.class.getSimpleName());

  public final void readArguments(String[] args, Properties properties) {

    Stream.of(args).forEach(arg -> {
      List<String> nameValue = Arrays.asList(arg.split("="));
      if (nameValue.size() < 2) {
        throw new IllegalArgumentException(
            String.format("Argument must have [name1]=[value1] [name2]=[value2] ... [nameN]=[valueN] format, but saw:(%s)", nameValue));
      }
      properties.put(nameValue.get(KEY), nameValue.get(VALUE));
    });

    if (Objects.nonNull(properties.get(ConfigProperty.CONFIG_FILE.propKey()))) {
      loadProperty(new File((String) properties.get(ConfigProperty.CONFIG_FILE.propKey())), properties);
    }

    logger.log(Level.INFO, "All read config {0}", properties);
  }

  public final void loadProperty(File file, Properties properties) {
    try (InputStream inputStrem = new FileInputStream(file);) {
      properties.load(inputStrem);
    } catch (IOException ex) {
      throw new RuntimeException(String.format("Could not read configuration from the given file (%s)", file), ex);
    }
  }

  public void loadDefaultConfig(Properties properties) {
    Stream.of(ConfigProperty.values()).forEach(config -> properties.setProperty(config.propKey(), config.defaultValue()));
  }
}
