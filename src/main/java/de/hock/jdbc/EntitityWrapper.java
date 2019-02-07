/**
 *
 */
package de.hock.jdbc;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Diese Klasse bildet POJO aus gelesene Datens&auml;tze.
 *
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 * @param <T>
 */
public class EntitityWrapper<T> implements ReadItem<T> {

  private static final Logger logger = Logger.getLogger(EntitityWrapper.class.getSimpleName());;

  private final SQLTypeMapper datatypeMapper = new SQLTypeMapper();
  private final EntityReflection reflection = new EntityReflection();
  private final ParameterValidator validator = new ParameterValidator();
  private final Map<String, Column> annotatedFields = new HashMap<>();
  private final List<String> columnNames = new ArrayList<>();
  private final Map<String, Field> fields = new HashMap<>();
  private final Map<String, Method> methods = new HashMap<>();

  private Boolean isEnum;
  private Boolean isPrimitive;
  private Connection connection;
  private Class<T> entity;
  private ResultSet resultSet;
  private Statement statement;

  /**
   * Erzeugt ein EntityWrapper-Objekt f&uuml;r die angegebene Entity.
   *
   * @param entitiy
   *          ein Klass-Typ
   *
   * @param resultSet
   *          ein {@link ResultSet}-Objekt, die Daten werden aus dieser
   *          ResultSet gelesen.
   *
   * @param statement
   *          ein {@link Statement}-Objekt, das liefert ResultSet-Objekt.
   *
   * @param connection
   *          ein {@link Connection}-Objekt, die Bestehendesverbindung zur
   *          Datenbank.
   *
   * @throws SQLException
   *           falls ein Datenbank fehler tritt auf.
   */
  public void parseEntity(Class<T> entitiy) {
    this.entity = entitiy;
    isEnum = reflection.isEnum(entitiy);
    isPrimitive = reflection.isPremitiveType(entitiy);

    if (!(isEnum || isPrimitive)) {
      methods.putAll(reflection.getSetterMethods(entitiy));
      fields.putAll(reflection.getDeclaredFields(entitiy));
      annotatedFields.putAll(reflection.getAnnotatedFields(entitiy, Column.class));
      mapFieldsAndMethodsByColumnname(methods, fields, annotatedFields);
    }
  }

  /**
   * Setzt die ResultSet, Statement und Connection in dieser Klasse f&uuml;r
   * weiterverarbeitung.
   *
   * @param resultSet
   * @param statement
   * @param connection
   * @throws SQLException
   * @throws EntitityWrapperException
   */
  public void setResultSet(ResultSet resultSet, Statement statement, Connection connection) throws SQLException {
    this.resultSet = resultSet;
    this.connection = connection;
    this.statement = statement;
    columnNames.addAll(getColumnsFromResultSet());
    if (!(isEnum || isPrimitive)) {
      validator.verifyMethodsname(columnNames, methods, entity.getName());
    }
  }

  /**
   * Alles Spaltenamen werden aus der ResultSet gelesen und in einer interne
   * List wird gespeichert.
   *
   * @throws SQLException
   *           falls ein Datenbank fehler tritt auf.
   */
  private List<String> getColumnsFromResultSet() throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    int count = metadata.getColumnCount();

    List<String> columns = new ArrayList<>();
    for (int index = 1; index <= count; index++) {
      columns.add(metadata.getColumnName(index));
    }

    return columns;
  }

  /**
   * Dieses Method liest alle Werte aus der ResultSet und baut ein Typed-Objekt
   * darauf.
   *
   * @return neue type-Objekt.
   * @throws SQLException
   *           falls ein Datenbank fehler tritt auf.
   * @throws EntitityWrapperException
   *           falls ein Fehler tritt auf.
   */
  private T mapToEntity() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException {
    T object = entity.newInstance();

    for (String columnName : columnNames) {
      Class<?> clazz = fields.get(columnName).getType();
      Method method = methods.get(columnName);
      method.invoke(object, datatypeMapper.getObject(resultSet, columnName, clazz));
    }

    return object;
  }

  @Override
  public void close() {
    try {
      if (Objects.nonNull(resultSet)) {
        resultSet.close();
      }
    } catch (SQLException sqlEx) {
      logger.log(Level.SEVERE, "Fehler beim ResultSet zu schlißen.", sqlEx);
    }

    try {
      if (Objects.nonNull(statement)) {
        statement.close();
      }
    } catch (SQLException sqlEx) {
      logger.log(Level.SEVERE, "Fehler beim PreparedStatement zu schlißen.", sqlEx);
    }

    try {
      if (Objects.nonNull(connection)) {
        connection.close();
      }
    } catch (SQLException sqlEx) {
      logger.log(Level.SEVERE, "Fehler beim Connection zu schlißen.", sqlEx);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public T nextItem() throws SQLException {
    Object object = null;

    try {
      if (resultSet.next()) {
        if (isEnum) {
          object = datatypeMapper.getEnumValue(resultSet, columnNames.get(0), entity);
        } else if (isPrimitive) {
          object = datatypeMapper.getObject(resultSet, columnNames.get(0), entity);
        } else {
          object = mapToEntity();
        }
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
      throw new RuntimeException(ex);
    }

    return (T) object;
  }

  /**
   * Falls Entity Feldernamen mit {@link Column} annotatiert werden, dann wird
   * Methode und Feldern mit {@link Column#name()} in interne {@link Map}
   * speichert.
   *
   * @param methods
   * @param fields
   * @param columnAnnotations
   */
  private void mapFieldsAndMethodsByColumnname(Map<String, Method> methods, Map<String, Field> fields, Map<String, Column> columnAnnotations) {
    columnAnnotations.entrySet().forEach(entry -> {
      String keyName = entry.getKey();

      Method method = methods.get(keyName);
      Field field = fields.get(keyName);
      methods.put(entry.getValue().name(), method);
      fields.put(entry.getValue().name(), field);
    });
  }
}
