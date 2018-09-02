/**
 * 
 */
package de.hock.argument;

import java.util.logging.Logger;
import java.util.stream.Stream;

import de.hock.run.ConfigProperty;

/**
 * @author Mojammal Hock
 * @version 1.0
 * @since 1.0
 */
public class ArgumentParser {

  private static final Logger logger = Logger.getLogger(ArgumentParser.class.getSimpleName());

  public void validateArgument(String[] args) {
    if (args.length == 0 || Stream.of(args).anyMatch(arg -> "-h".equals(arg) || "--help".equals(arg))) {
      showUsage();
      printAllKnownConfig();
      System.exit(0);
    }
  }

  private void showUsage() {
    StringBuilder sb = new StringBuilder();
    sb.append("DatenbankOpeation ConfigFile=[config-file] ...").append("\n");
    sb.append("  -h/--help show this usage.").append("\n");
    sb.append("  ConfigFile: from the given config file config will be read.").append("\n");
    logger.info(sb.toString());
  }

  private void printAllKnownConfig() {
    System.out.println("*** All possible configuration ***");
    Stream.of(ConfigProperty.values())
        .forEach(config -> System.out.println(String.format("%s=%s", config.propKey(), config.defaultValue())));
  }
}
