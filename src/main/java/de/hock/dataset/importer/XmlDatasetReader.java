/**
 * 
 */
package de.hock.dataset.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author hockm002
 *
 */
public class XmlDatasetReader {

  private static final Logger logger = Logger.getLogger(XmlDatasetReader.class.getSimpleName());

  private static final String TEXT_NODE = "#text";
  private static final String ROOT_ELEMENT = "dataset";
  private static final int ROOT_INDEX = 0;

  private Map<String, List<String>> columnNames = new HashMap<>();
  private Map<String, List<Map<String, String>>> dataRows = new HashMap<>();
  private Node rootNode;

  public XmlDatasetReader(File file) throws ParserConfigurationException, SAXException, IOException {

    try (InputStream inputStream = new FileInputStream(file)) {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document document = db.parse(inputStream);

      NodeList firstLevelNodeList = document.getElementsByTagName(ROOT_ELEMENT);
      // There is only one dataset which is the root node. All the child nodes
      // of the root node is the datarow.
      rootNode = firstLevelNodeList.item(ROOT_INDEX);

    } catch (IOException ioException) {
      throw ioException;
    }
  }

  public void readAllNodes() {
    NodeList children = rootNode.getChildNodes();

    for (int childIndex = 0; childIndex < children.getLength(); childIndex++) {
      Node childNode = children.item(childIndex);
      String nodeName = childNode.getNodeName();
      logger.fine("Process node name " + nodeName);

      if (!Objects.equals(TEXT_NODE, nodeName)) {

        List<Map<String, String>> currentRows = dataRows.getOrDefault(nodeName, new ArrayList<>());
        List<String> columns = columnNames.getOrDefault(nodeName, new ArrayList<>());

        NamedNodeMap namedNode = childNode.getAttributes();
        Map<String, String> row = new HashMap<>();

        for (int attributeIndex = 0; attributeIndex < namedNode.getLength(); attributeIndex++) {
          Node attributeNode = namedNode.item(attributeIndex);

          String attNodeName = attributeNode.getNodeName();
          if (!columns.contains(attNodeName)) {
            columns.add(attNodeName);
          }
          row.put(attNodeName, attributeNode.getNodeValue());
        }

        currentRows.add(row);
        columnNames.put(nodeName, columns);
        logger.fine(String.format("Tablename: %s, column count: %d,", nodeName, columns.size()));
        logger.finer(String.format("Column names: %s", columns));
        dataRows.put(nodeName, currentRows);
        logger.fine(String.format("Tablename: %s, row count: %d,", nodeName, currentRows.size()));
        logger.finer(String.format("Row: %s", row));
      }
    }
  }

  public Set<String> getTableNames() {
    return Collections.unmodifiableSet(columnNames.keySet());
  }

  public List<String> getColumnsOfTable(String tableName) {
    List<String> columns = columnNames.get(tableName);
    Collections.sort(columns);
    return Collections.unmodifiableList(columns);
  }

  public List<Map<String, String>> getRowValues(String tableName) {
    return dataRows.get(tableName);
  }
}
