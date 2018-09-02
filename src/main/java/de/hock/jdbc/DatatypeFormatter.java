/**
 * 
 */
package de.hock.jdbc;

import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import de.hock.run.MainClass;

/**
 * @author hockm002
 *
 */
public class DatatypeFormatter {

  private static final Logger logger = Logger.getLogger(MainClass.class.getSimpleName());

  public Object getFormatedValue(String valuetype, String nodeValue) {
    Object formatedValue = null;
    switch (valuetype) {
    case "CHAR":
    case "CHARACTER":
    case "STRING":
    case "VARCHAR2":
    case "VARCHAR":
    case "CLOB":
      formatedValue = nodeValue;
      break;
    case "NCHAR":
    case "NVARCHAR2":
    case "NCLOB":
      formatedValue = nodeValue.getBytes(StandardCharsets.UTF_8);
      break;
    case "NUMBER":
    case "LONG":
      formatedValue = Long.valueOf(nodeValue);
      break;
    case "BLOB":
      formatedValue = nodeValue.getBytes();
      break;
    default:
      logger.warning(String.format("Unknow datatpye: %s, convertiere as string", valuetype));
      formatedValue = nodeValue;
    }

    return formatedValue;
  }

}
