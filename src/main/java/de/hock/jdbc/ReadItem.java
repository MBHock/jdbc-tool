/**
 * 
 */
package de.hock.jdbc;

import java.sql.SQLException;

/**
 * Diese Interface bietet die M&ouml;glichkeit, ein Datenbanktabelle zeilweise
 * zu lesen und daruf ein Typisiert Objekt zu liefern.
 *
 * @param <T>
 *          ein Typ-Objekt. Gelesene Datenbank Row wird zu dieser Typ modeliert.
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a> *
 */
public interface ReadItem<T> {

  /**
   * Liest die n&auml;chte Row aus der {@link java.sql.ResultSet} und darauf
   * Modeliert ein Typ-Objekt.
   *
   * @return ein Typ-Objekt
   * @throws SQLException
   *           wenn Datenbankoperation lauft Fehlschlag.
   * @throws EntitityWrapperException
   *           wenn ReasultSet zu diser Objket nicht modelieren werden kann.
   */
  public T nextItem() throws SQLException;

  /**
   * Datenbank Ressources zu freigeben.
   */
  void close();

}
