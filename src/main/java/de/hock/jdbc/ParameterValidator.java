/**
 *
 * Copyright (c) 2017 Bundesagentur fuer Arbeit. All Rights Reserved
 *
 */

package de.hock.jdbc;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

/**
 * Diese Klasse bitet die M&ouml;glichkeit, die eingabe Parameter zu validieren.
 * 
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 * 
 */
public class ParameterValidator {

  private Predicate<Object> verifyEmpty = str -> ((String) str).isEmpty();
  private Predicate<Object> verifyEmptyCollection = collection -> ((Collection<?>) collection).isEmpty();
  private Predicate<Object> verifyNull = Objects::isNull;

  /**
   * Diese Hilfe-Methode validiert die Eingabeparameter.
   *
   * @param resultSet
   *          ResultSet
   * @param columnName
   *          Spaltenname ein Wert zu lesen
   * @param classType
   *          KlassenType, die gelesene Wert zu mappen
   */
  public <T> void verifyArguments(ResultSet resultSet, String columnName, Class<T> classType) {
    if (verifyNull.test(resultSet)) {
      throw new IllegalArgumentException("ResultSet darf nicht null sein!");
    }
    if (verifyNull.or(verifyEmpty).test(columnName)) {
      throw new IllegalArgumentException("Columnname darf nicht null oder leer sein!");
    }
    if (verifyNull.test(classType)) {
      throw new IllegalArgumentException("Classtype darf nicht null sein!");
    }
  }

  /**
   * Diese Hilfe-Methode validiert den Eingabeparameter <code>query</code>.
   *
   * Falls der Eingabe-Queryparameter null oder leer ist, dann warf ein
   * {@link IllegalArgumentException} Exception.
   *
   * @param query,
   *          erlaubt {@link String}
   */
  public void verifyArguments(String query) {
    if (verifyNull.or(verifyEmpty).test(query)) {
      throw new IllegalArgumentException("Query darf nicht null oder leer sein!");
    }
  }

  /**
   * Diese Hilfe-Methode validiert die Eingabeparameter.
   *
   * @param typeObject
   *          ein Typ-objekt
   */
  public <T> void verifyArguments(T typeObject) {
    if (verifyNull.test(typeObject)) {
      throw new IllegalArgumentException("Argument darf nicht null sein!");
    }
  }

  /**
   * Diese Hilfe-Methode validiert die Eingabeparameter. Falls die Validierung
   * fehl schlagt, dann warf ein Exception mit eingabe String.
   *
   * @param typeObject
   *          ein Typ-objekt
   * @param string
   *          konkret Fehlermeldung
   */
  public <T> void verifyArguments(T object, String string) {
    if (verifyNull.test(object)) {
      throw new IllegalArgumentException(string);
    }
  }

  /**
   * Dieses Method validiert ResultSet gegen angegebene Entityklasse.
   *
   * @param columns
   *          Spaltenname liste, die im ResultSet verfügbar.
   * @param fields
   *          public Getter/Setter Methode in der Entity-Klasse
   * @param className
   *          Name der Klasse
   */
  public void verifyColumnsname(Collection<String> columns, Map<String, Field> fields, String className) {
    BinaryOperator<String> accumulator = (result, columnName) -> result.isEmpty() ? columnName : String.join(", ", result, columnName);
    String missingColumnNames = columns.stream().filter(columnName -> verifyNull.test(fields.get(columnName))).reduce("", accumulator);

    if (verifyEmpty.negate().test(missingColumnNames)) {
      throw new IllegalArgumentException(String.format("Die Entityklasse hat %s keine passende felder für %s", className, missingColumnNames));
    }
  }

  /**
   * Diese Methode validiert die List arugment. Die List argument darf nicht
   * null oder leer sein.
   *
   * @param eine
   *          list Objekt.
   */
  public <T> void verifyListArguments(List<T> typedObjects) {
    if (verifyNull.or(verifyEmptyCollection).test(typedObjects)) {
      throw new IllegalArgumentException("Collection argument darf nicht null oder leer sein!");
    }
  }

  /**
   * Diese Methode validiert, ob setter/getter methode gibt passend zu column.
   *
   * @param columns
   *          ein liste mit column name
   * @param methods
   *          ein Map mit Method Referenz
   * @param name
   *          name des Klasse
   * @throws EntitityWrapperException
   */
  public void verifyMethodsname(List<String> columns, Map<String, Method> methods, String name) {
    BinaryOperator<String> accumulator = (result, columnName) -> result.isEmpty() ? columnName : String.join(", ", result, columnName);
    String missingColumnNames = columns.stream().filter(columnName -> verifyNull.test(methods.get(columnName))).reduce("", accumulator);

    if (verifyEmpty.negate().test(missingColumnNames)) {
      throw new IllegalArgumentException(String.format("Die Entityklasse hat %s keine passende felder für %s", name, missingColumnNames));
    }
  }

  /**
   * Diese Hilfe-Methode validiert den Eingabeparameter <code>query</code>.
   *
   * Falls der Eingabe-Queryparameter null oder leer ist, dann warf ein
   * {@link IllegalArgumentException} Exception.
   *
   * @param query,
   *          erlaubt {@link String}
   */
  public void verifyQueries(List<String> queries) {
    if (verifyNull.or(verifyEmptyCollection).test(queries)) {
      throw new IllegalArgumentException("Query liste darf nicht null oder leer sein!");
    }

    if (queries.stream().anyMatch(verifyNull.or(verifyEmpty))) {
      throw new IllegalArgumentException("Query liste hat ein oder mehrere leer Query. Liste darf keine null oder leer Query haben!");
    }
  }
}
