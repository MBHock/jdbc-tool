/**
 * 
 */
package de.hock.jdbc;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.ucp.jdbc.PoolDataSource;

/**
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 * 
 */
public class OracleDatabaseOperation implements DatabaseOperation {

  // private static final String FETCH_COLUMNS = "SELECT COLUMN_NAME FROM
  // sys.all_tab_cols WHERE HIDDEN_COLUMN = 'NO' AND TABLE_NAME = ? ORDER BY
  // COLUMN_ID";
  private static final String SELECT_META_INFORMATION = "select column_name, data_type from user_tab_columns where table_name = ?";

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final int DEFAULT_FETCH_SIZE = 200;
  private static final String QUERY_LOG = "Select query {0}.";

  private static final Logger logger = Logger.getLogger(OracleDatabaseOperation.class.getSimpleName());
  private static final DatatypeFormatter formatter = new DatatypeFormatter();

  private EntityReflection reflection = new EntityReflection();
  private PlaceholderSetter platzhalterSetzer = new PlaceholderSetter();
  private ParameterValidator validator = new ParameterValidator();
  private Map<String, Map<String, String>> chacheTableInfo = new HashMap<>();

  private PoolDataSource oraclePoolConnection;

  public OracleDatabaseOperation(PoolDataSource oraclePool) {
    oraclePoolConnection = oraclePool;
  }

  /**
   * Diese Methode kapselt alle Datenbank Schreib-Operation. Diese Low-Level
   * Implementation f&uuml;hrt ein Schreibquery im Datenbank.
   *
   * @param query
   *          ein SQL Query zu durchführen
   * @param rowsOfColumnValues
   *          platzhalter Werte, die wird verwendet, Query Parameter zu
   *          austauchen.
   * @return leifert Anzahl des aktulisiertes/geschriebenes Datensatz
   * @throws SQLException
   *           falls ein {@link SQLException} trat auf.
   */
  private int batchExecute(String query, List<List<Object>> rowsOfColumnValues) throws SQLException {
    validator.verifyArguments(query);
    validator.verifyArguments(rowsOfColumnValues);
    int insertOrupdateCount = 0;

    Connection connection = null;
    PreparedStatement preparedStatement = null;

    try {
      connection = oraclePoolConnection.getConnection();
      connection.setAutoCommit(false);

      preparedStatement = connection.prepareStatement(query);

      int rowCounter = 0;
      for (List<Object> columnValues : rowsOfColumnValues) {
        platzhalterSetzer.setPlaceholder(preparedStatement, columnValues);
        preparedStatement.addBatch();
        rowCounter++;
        if ((rowCounter > 0) && ((rowCounter % DEFAULT_BATCH_SIZE) == 0)) {
          preparedStatement.executeBatch();
        }
      }
      preparedStatement.executeBatch();
      insertOrupdateCount = preparedStatement.getUpdateCount();
      connection.commit();

    } finally {

      connection.close();

    }

    return insertOrupdateCount;
  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#
   * batchInsert(java.util.List)
   */
  @Override
  public <T> void batchInsert(List<T> typedObjects) throws SQLException, IllegalAccessException {
    validator.verifyListArguments(typedObjects);
    T typedObject = typedObjects.get(0);
    validator.verifyArguments(typedObject);

    List<String> columnNames = reflection.getColumnNameAlias(typedObject);
    Map<String, Field> fields = reflection.getDeclaredFields(typedObject.getClass(), Column.class);
    validator.verifyColumnsname(columnNames, fields, typedObject.getClass().getName());

    String query = erzeugeInsertQuery(typedObject, columnNames);
    logger.log(Level.FINEST, "Batch insert query {0}", query);
    Connection connection = null;
    PreparedStatement preparedStatement = null;

    try {
      connection = oraclePoolConnection.getConnection();
      preparedStatement = connection.prepareStatement(query);

      int rowCounter = 0;
      for (T type : typedObjects) {
        platzhalterSetzer.setPlaceholder(preparedStatement, columnNames, fields, type);
        preparedStatement.addBatch();
        rowCounter++;
        if ((rowCounter > 0) && ((rowCounter % DEFAULT_BATCH_SIZE) == 0)) {
          preparedStatement.executeBatch();
        }
      }

      preparedStatement.executeBatch();
      logger.log(Level.INFO, "{0} Datensatz wurde geschrieben.", preparedStatement.getUpdateCount());
      connection.commit();

    } finally {
      connection.close();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#
   * batchInsert(java.lang.String, java.util.List)
   */
  @Override
  public void batchInsert(String query, List<List<Object>> rowsOfColumnValues) throws SQLException {
    logger.log(Level.FINEST, "Batch insert query {0}", query);

    int anzahl = batchExecute(query, rowsOfColumnValues);

    logger.log(Level.FINE, "{0} Datensätze wurden geschrieben,", anzahl);
  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#
   * batchUpdate(java.lang.String, java.util.List)
   */
  @Override
  public int batchUpdate(String query, List<List<Object>> rowsOfColumnValues) throws SQLException {
    logger.log(Level.FINEST, "Batch update query {0}", query);

    int anzahl = batchExecute(query, rowsOfColumnValues);

    logger.log(Level.FINE, "{0} Datensätze wurden aktualisiert", anzahl);
    return anzahl;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#delete
   * (java.lang.String, java.util.List)
   */
  @Override
  public int delete(String query, List<Object> columnValues) throws SQLException {
    logger.log(Level.FINEST, "delete query {0}", query);

    int anzahl = batchExecute(query, Arrays.asList(columnValues));

    logger.log(Level.FINE, "{0} Datensätze wurden gelöscht", anzahl);
    return anzahl;
  }

  /**
   * Diese Methode generiert ein Insert-Query aus dem Typ-Objekt.
   *
   * @param typedObject
   *          ein Typ-Objekt, die in Datenbank persistiert wird.
   *
   * @return ein Insert-Query
   */
  private <T> String erzeugeInsertQuery(T typedObject, List<String> columnNameAlias) {
    TableName tableName = reflection.getAnnotation(typedObject.getClass(), TableName.class);
    validator.verifyArguments(tableName);

    StringJoiner columnJoiner = new StringJoiner(",", "(", ")");
    StringJoiner platzHalterJoiner = new StringJoiner(",", "(", ")");
    columnNameAlias.stream().forEach(columnname -> {
      columnJoiner.add(columnname);
      platzHalterJoiner.add("?");
    });

    StringJoiner query = new StringJoiner(" ");
    query.add("INSERT INTO");
    query.add(tableName.name());
    query.add(columnJoiner.toString());
    query.add("VALUES");
    query.add(platzHalterJoiner.toString());

    return query.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#
   * executeDDL(java.lang.String)
   */
  @Override
  public void executeDDL(List<String> queries) throws SQLException {
    validator.verifyQueries(queries);

    Connection connection = null;
    Statement preparedStatement = null;

    try {
      connection = oraclePoolConnection.getConnection();
      preparedStatement = connection.createStatement();
      for (String query : queries) {
        preparedStatement.addBatch(query);
      }
      preparedStatement.executeBatch();
      int insertOrupdateCount = preparedStatement.getUpdateCount();
      connection.commit();
      logger.log(Level.FINE, "Update Anzahl {0}", insertOrupdateCount);

    } finally {
      connection.close();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#
   * executeDDL(java.lang.String)
   */
  @Override
  public void executeDDL(String query, List<Object> placeholder) throws SQLException {
    logger.log(Level.FINEST, "DDL query {0}", query);

    batchExecute(query, Arrays.asList(placeholder));

  }

  /*
   * (non-Javadoc)
   *
   * @see
   * de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#insert
   * (java.lang.String, java.util.List)
   */
  @Override
  public void insert(String query, List<Object> columnValues) throws SQLException {
    logger.log(Level.FINEST, "Insert query {0}", query);

    int anzahl = batchExecute(query, Arrays.asList(columnValues));

    logger.log(Level.FINE, "{0} Daten wird inserted.", anzahl);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#insert
   * (Object)
   */
  @Override
  public <T> void insert(T typeObject) throws SQLException, IllegalAccessException {
    validator.verifyArguments(typeObject);

    batchInsert(Arrays.asList(typeObject));
  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#
   * selectOneRow(java.lang.String, java.util.List, java.lang.Class)
   */
  @Override
  public <T> T selectOneRow(String query, List<Object> columnValues, Class<T> clazz) throws SQLException {
    ReadItem<T> readItem = selectRow(query, columnValues, clazz);
    T typeObj = readItem.nextItem();

    // Prüfe ob noch mehr Row gibt
    if (Objects.nonNull(readItem.nextItem())) {
      readItem.close();
      throw new SQLException("Eine Zeile erwarted, aber es gibt mehr treffer.");
    }
    readItem.close();

    return typeObj;
  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#
   * selectRow(java.lang.String, java.util.List, java.lang.Class)
   */
  @Override
  public <T> ReadItem<T> selectRow(String query, List<Object> columnValues, Class<T> clazz) throws SQLException {
    logger.log(Level.FINEST, QUERY_LOG, query);
    validator.verifyArguments(query);
    validator.verifyArguments(columnValues);

    Connection connection = null;
    PreparedStatement preparedStatement = null;
    EntitityWrapper<T> entityMapper = null;

    try {

      connection = oraclePoolConnection.getConnection();
      preparedStatement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
      preparedStatement.setFetchSize(DEFAULT_FETCH_SIZE);
      platzhalterSetzer.setPlaceholder(preparedStatement, columnValues);

      ResultSet resultSet = preparedStatement.executeQuery();
      entityMapper = new EntitityWrapper<>();
      entityMapper.parseEntity(clazz);
      entityMapper.setResultSet(resultSet, preparedStatement, connection);
      return entityMapper;

    } catch (SQLException exception) {

      if (Objects.nonNull(connection)) {
        connection.rollback();
      }

      throw exception;

    }
  }

  @Override
  public <T> ReadItem<T> selectRow(String query, List<Object> columnValues, RowMapper<T> mapper) throws SQLException {
    logger.log(Level.FINEST, QUERY_LOG, query);
    validator.verifyArguments(query);
    validator.verifyArguments(columnValues);

    Connection connection = null;
    PreparedStatement preparedStatement = null;
    RowWrapper<T> entityMapper = null;

    try {

      connection = oraclePoolConnection.getConnection();
      preparedStatement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
      preparedStatement.setFetchSize(DEFAULT_FETCH_SIZE);
      platzhalterSetzer.setPlaceholder(preparedStatement, columnValues);

      ResultSet resultSet = preparedStatement.executeQuery();
      entityMapper = new RowWrapper<>();
      entityMapper.setMapper(mapper);
      entityMapper.setResultSet(resultSet, preparedStatement, connection);
      return entityMapper;

    } catch (SQLException exception) {

      connection.rollback();
      throw exception;

    }
  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#
   * selectRows(java.lang.String, java.util.List, java.lang.Class)
   */
  @Override
  public <T> List<T> selectRows(String query, List<Object> columnValues, Class<T> clazz) throws SQLException {
    logger.log(Level.FINEST, QUERY_LOG, query);
    validator.verifyArguments(query);
    validator.verifyArguments(columnValues);

    List<T> entities = new ArrayList<>();
    ReadItem<T> readItem = selectRow(query, columnValues, clazz);
    T typeObj = null;
    while ((typeObj = readItem.nextItem()) != null) {
      entities.add(typeObj);
    }

    readItem.close();

    return entities;
  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#
   * selectRows(java.lang.String, java.util.List,
   * de.ba.operativ.leistung.batch.framework.datenbank.DatenbankZeilenMapper)
   */
  @Override
  public <T> List<T> selectRows(String query, List<Object> columnValues, RowMapper<T> mapper) throws SQLException {
    logger.log(Level.FINEST, QUERY_LOG, query);
    validator.verifyArguments(query);
    validator.verifyArguments(columnValues);

    List<T> entities = new ArrayList<>();
    ReadItem<T> readItem = selectRow(query, columnValues, mapper);

    T typeObj = null;
    while ((typeObj = readItem.nextItem()) != null) {
      entities.add(typeObj);
    }

    readItem.close();

    return entities;
  }

  // @Override
  // public <T> List<T> selectRows(String query, Map<String, Object>
  // columnValues, RowMapper<T> mapper) throws SQLException {
  // logger.log(Level.FINEST, QUERY_LOG, query);
  // validator.verifyArguments(query);
  // validator.verifyArguments(columnValues);
  //
  // Connection connection = null;
  // PreparedStatement preparedStatement = null;
  // RowWrapper<T> entityMapper = null;
  //
  // try {
  //
  // connection = oraclePoolConnection.getConnection();
  // preparedStatement = connection.prepareStatement(query,
  // ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
  // ResultSet.HOLD_CURSORS_OVER_COMMIT);
  // preparedStatement.setFetchSize(DEFAULT_FETCH_SIZE);
  // platzhalterSetzer.setzePlatzhalter(preparedStatement, columnValues);
  //
  // ResultSet resultSet = preparedStatement.executeQuery();
  // entityMapper = new RowWrapper<>();
  // entityMapper.setMapper(mapper);
  // entityMapper.setResultSet(resultSet, preparedStatement, connection);
  // List<T> entities = new ArrayList<>();
  //
  // T typeObj = null;
  // while ((typeObj = entityMapper.nextItem()) != null) {
  // entities.add(typeObj);
  // }
  //
  // entityMapper.close();
  // return entities;
  //
  // } catch (SQLException exception) {
  //
  // connection.rollback();
  // throw exception;
  //
  // }
  // }

  /**
   * Setzt die ConnectionManager. Erlaubt entweder
   * {@link NonTransactionalConnectionManager} oder
   * {@link TransactionalConnectionManager}
   *
   * @param connectionManager
   *          ein {@link ConnectionManager}-Objekt
   */
  public void setConnectionManager(ConnectionManager connectionManager) {

  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.TransactionManager#
   * transactionBegin()
   */
  @Override
  public void begin() {

    getTransactionManager().begin();

  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.TransactionManager#
   * transactionClose()
   */
  @Override
  public void close() {

    getTransactionManager().close();

  }

  @Override
  public void commit() {

    getTransactionManager().commit();

  }

  /*
   * (non-Javadoc)
   *
   * @see de.ba.operativ.leistung.batch.framework.datenbank.TransactionManager#
   * transactionRollback()
   */
  @Override
  public void rollback() {

    getTransactionManager().rollback();

  }

  /**
   * Diese Hilfe-Method versucht ConnectionManager down-cast zu
   * TransactionManager. Wenn mann w&auml;hrend DatenbankOpetaion Inject der
   * Field mit Qualifier {@link Transactional} annotiert hat, dann wird den
   * Down-Cast erfolgreich sien. Sonst warf ein {@link TransactionException}.
   *
   * @return {@link Transactional}-Objekt.
   *
   * @throws TransactionException
   */
  private Transactional getTransactionManager() {

    throw new RuntimeException(
        "Diese ConnectionManager unterstützt keine Transaktion. Für transaktionalen Datenbankoperation sollte mann den DatenbankOpeation-Objekt mit @Transactionl Annotation vorsehen.");
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * de.ba.operativ.leistung.batch.framework.datenbank.DatenbankOperation#update
   * (java.lang.String, java.util.List)
   */
  @Override
  public int update(String query, List<Object> columnValues) throws SQLException {
    logger.log(Level.FINEST, "Update query {0}", query);

    int anzahl = batchExecute(query, Arrays.asList(columnValues));

    logger.log(Level.FINE, "{0} Datensatz wurde aktualisiert.", anzahl);
    return anzahl;
  }

  @Override
  public void batchInsert(String tableName, List<String> columns, List<Map<String, String>> rows) throws SQLException {
    if (Objects.isNull(chacheTableInfo.get(tableName))) {
      List<Map<String, String>> map = selectRows(SELECT_META_INFORMATION, Arrays.asList(tableName), new TableSchemaMapper());
      Map<String, String> columnInfo = new HashMap<>();
      map.forEach(columnInfo::putAll);
      chacheTableInfo.put(tableName, columnInfo);
    }

    Map<String, String> rowInfo = chacheTableInfo.get(tableName);
    Set<String> inDb = rowInfo.keySet();
    if (!inDb.containsAll(columns)) {
      throw new IllegalArgumentException(String.format("Column in database: %s, given: %s are different.", inDb, columns));
    }

    final String query = createInsertQuery(tableName, columns);
    logger.log(Level.FINE, "Insert Query {0}", query);
    int insertedRows = batchExecute(query, getRowValues(columns, rows, chacheTableInfo.get(tableName)));
    logger.log(Level.INFO, "{0} rows has been written into {1}", new Object[] { insertedRows, tableName });
  }

  private List<List<Object>> getRowValues(List<String> columns, List<Map<String, String>> rows, Map<String, String> metainfo) {
    List<List<Object>> listOfRow = new ArrayList<>();

    for (Map<String, String> row : rows) {
      List<Object> values = new ArrayList<>();
      for (String column : columns) {
        String columnValue = row.get(column);
        values.add(Objects.isNull(columnValue) ? null : formatter.getFormatedValue(metainfo.get(column), columnValue));
      }
      listOfRow.add(values);
    }

    return listOfRow;
  }

  private String createInsertQuery(String tableName, List<String> columns) {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    sb.append(tableName).append(" ");
    sb.append(columns.stream().collect(Collectors.joining(",", "(", ")")));
    sb.append(" values (").append(String.join(",", Collections.nCopies(columns.size(), "?"))).append(")");
    return sb.toString();
  }

  public class TableSchemaMapper implements RowMapper<Map<String, String>> {
    private static final String COLUMN_NAME_KEY = "COLUMN_NAME";
    private static final String DATA_TYPE_KEY = "DATA_TYPE";

    private Map<String, Integer> columns;
    private String tableName;

    @Override
    public Map<String, String> mapRow(ResultSet resultSet) throws SQLException {
      if (Objects.isNull(columns) || Objects.isNull(tableName)) {
        columns = getColumnNames(resultSet);
        tableName = getTableName(resultSet);
      }

      Map<String, String> row = new HashMap<>();
      row.put(resultSet.getString(COLUMN_NAME_KEY), resultSet.getString(DATA_TYPE_KEY));
      return row;
    }

    @Override
    public Map<String, Integer> getColumnNames() {
      return columns;
    }

    @Override
    public String getTableName() {
      return tableName;
    }
  }
}
