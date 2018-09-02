package de.hock.jdbc;

import java.beans.Transient;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EntityReflection {

  private Function<Field, String> fieldNameInUppercase = f -> f.getName().toUpperCase();

  private BiPredicate<Method, String> isGetter =
      (Method m, String s) -> m.getName().startsWith(s) && (m.getParameterCount() == 0) && !m.getReturnType().equals(void.class);

  private BiPredicate<Method, String> isSetter =
      (Method m, String s) -> m.getName().startsWith(s) && (m.getParameterCount() == 1) && m.getReturnType().equals(void.class);

  private Function<Entry<String, Field>, String> mapFieldByAliasName = entry -> {
    Field field = entry.getValue();
    Column column = getFieldAnnotation(field, Column.class);
    return Objects.nonNull(column) ? column.name() : entry.getKey();
  };

  private Function<Method, String> nameInUppercase = m -> m.getName().substring(3).toUpperCase();

  private String prefixGet = "get";
  private String prefixSet = "set";

  /**
   * Dise Hilfe Methode liefert eine Map von Feldern (public, protected, package
   * default und private), die mit <code>annotationClass</code> annotiert ist.
   * Die benutzte Key ist Feldername in Uppercase.
   *
   * @param clazz
   *          ein Class-Typ
   * @return liefert ein {@link HashMap}-Objekt, welche enth&auml;lt alle
   *         Annotated-Feldern.
   */
  public <A extends Annotation, T> Map<String, A> getAnnotatedFields(Class<T> clazz, Class<A> annotationClass) {
    Map<String, A> annotatedFields = new HashMap<>();
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      A annotation = field.getAnnotation(annotationClass);
      annotatedFields.computeIfAbsent(fieldNameInUppercase.apply(field), key -> annotation);
    }

    return annotatedFields;
  }

  /**
   * Dise Hilfe Methode liest Klass Annotation, die mit
   * <code>annotationClass</code>-Annottiert ist.
   *
   * @param clazz
   *          ein Class-Typ
   * @param tableName
   *          ein TableName annotation
   * @return liefert ein {@link HashMap}-Objekt, welche enth&auml;lt alle
   *         Annotated-Feldern.
   */
  public <A extends Annotation, T> A getAnnotation(Class<T> clazz, Class<A> annotationClass) {
    return clazz.getAnnotation(annotationClass);
  }

  /**
   * Diese Methode liest alle Feldern name, dabei mapped er Feldername mit
   * {@link Column}-Annotation und anschließend liefert es alle Columnname passt
   * zu einer Datenbanktabelle.
   *
   * @param typedObject
   *          ein Typ-Objekt
   * @return eine Liste mit Columnnamen alias
   */
  public <T> List<String> getColumnNameAlias(T typedObject) {
    Map<String, Field> fields = getDeclaredFields(typedObject.getClass());
    return fields.entrySet().stream().map(mapFieldByAliasName).collect(Collectors.toList());
  }

  /**
   * Diese Hilfe Methode liest alle Feldern (public, protected, package default
   * und private) außer transient Feldern aus der Klasse und liefert es
   * zur&uuml;ck. Die benutzte Key ist Feldername in Uppercase.
   *
   * @param clazz
   *          ein Class-Typ
   * @return liefert ein {@link HashMap}-Objekt, welche enth&auml;lt alle
   *         Feldern.
   */
  public <T> Map<String, Field> getDeclaredFields(Class<T> clazz) {
    Map<String, Field> fieldMap = new HashMap<>();
    Field[] fields = clazz.getDeclaredFields();

    for (Field field : fields) {
      if (Modifier.isTransient(field.getModifiers()) || Objects.nonNull(field.getAnnotation(Transient.class))) {
        // Ignore
        continue;
      }

      fieldMap.put(fieldNameInUppercase.apply(field), field);
    }

    return fieldMap;
  }

  /**
   * Diese Hilfe Methode liest alle Feldern (public, protected, package default
   * und private) außer transient Feldern aus der Klasse und liefert es
   * zur&uuml;ck. die R&uuml;ckgabe Map wird mit alias {@link Column}
   * expandiert. Die benutzte Key ist Feldername in Uppercase.
   *
   * @param clazz
   *          ein Class-Typ
   * @return liefert ein {@link HashMap}-Objekt, welche enth&auml;lt alle
   *         Feldern.
   */
  public <T> Map<String, Field> getDeclaredFields(Class<T> clazz, Class<Column> annotationClass) {
    Map<String, Field> fieldMap = new HashMap<>();
    Field[] fields = clazz.getDeclaredFields();

    for (Field field : fields) {
      if (Modifier.isTransient(field.getModifiers()) || Objects.nonNull(field.getAnnotation(Transient.class))) {
        // Ignore
        continue;
      }

      fieldMap.put(fieldNameInUppercase.apply(field), field);
      Column column = field.getAnnotation(annotationClass);
      if (Objects.nonNull(column)) {
        fieldMap.put(column.name(), field);
      }
    }

    return fieldMap;
  }

  /**
   * Liefert die Annotation-Objekt, die mit <code>annotationClass</code>
   * annotiert ist. Wenn der Feld nicht mit dem <code>annotationClass</code>
   * annotiert ist, dann liefert null zur&uuml;ck.
   *
   * @param field
   *          ein Field Objekt
   * @param annotationClass
   *          ein Annotation-Klass Typ
   * @return annotation Objekt typisiert mit eingabe
   *         <code>annotationClass</code>
   */
  public <A extends Annotation> A getFieldAnnotation(Field field, Class<A> annotationClass) {
    return field.getAnnotation(annotationClass);
  }

  /**
   * Dise Hilfe Methode liest alle Getter-Methode aus der Klasse und liefert es
   * zur&uuml;ck. Die benutzte Key ist Feldername in Uppercase.
   *
   * @param clazz
   *          ein Class-Typ
   * @return liefert ein {@link HashMap}, welches enth&auml;lt alle
   *         Getter-Methode.
   */
  public <T> Map<String, Method> getGetterMethods(Class<T> clazz) {
    Map<String, Method> getterMethode = new HashMap<>();
    Method[] methode = clazz.getMethods();

    for (Method method : methode) {
      if (isGetter.test(method, prefixGet)) {
        getterMethode.put(nameInUppercase.apply(method), method);
      }
    }

    return getterMethode;
  }

  /**
   * Dise Hilfe Methode liest alle Setter-Methode aus der Klasse und liefert es
   * zur&uuml;ck. Die benutzte Key ist Feldername in Uppercase.
   *
   * @param clazz
   *          ein Class-Typ
   * @return liefert ein {@link HashMap}, welches enth&auml;lt alle
   *         Setter-Methode.
   */
  public <T> Map<String, Method> getSetterMethods(Class<T> clazz) {
    Map<String, Method> setterMethode = new HashMap<>();
    Method[] methode = clazz.getMethods();

    for (Method method : methode) {
      if (isSetter.test(method, prefixSet)) {
        setterMethode.put(nameInUppercase.apply(method), method);
      }
    }

    return setterMethode;
  }

  /**
   * Diese Hilfemethode bestimmt, ob der Class-Typ Enum ist oder nicht.
   *
   * @param clazz
   *          ein Class-Typ
   * @return <code>true</code> wenn der Class-Typ Enum ist, sonst
   *         <code>false</code>.
   */
  public <T> boolean isEnum(Class<T> clazz) {
    return clazz.isEnum();
  }

  /**
   * Diese Hilfemethode bestimmt, ob der Class-Typ Java Lang Klasse ist oder
   * nicht.
   *
   * @param clazz
   *          ein Class-Typ
   * @return <code>true</code> wenn der Class-Typ Java Lang Klasse ist, sonst
   *         <code>false</code>.
   */
  public <T> boolean isPremitiveType(Class<T> clazz) {
    boolean isPremitiveType;

    switch (clazz.getTypeName()) {
    case "java.lang.String":
    case "java.math.BigDecimal":
    case "java.lang.Boolean":
    case "java.lang.Byte":
    case "java.lang.Short":
    case "java.lang.Integer":
    case "java.lang.Long":
    case "java.lang.Float":
    case "java.lang.Double":
    case "java.time.LocalDate":
    case "java.time.LocalTime":
    case "java.time.LocalDateTime":
    case "byte[]":
      isPremitiveType = true;
      break;
    default:
      isPremitiveType = clazz.isPrimitive();
    }

    return isPremitiveType;
  }
}
