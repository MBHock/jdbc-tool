/**
 * 
 */
package de.hock.jdbc;

/**
 *
 *
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 */
public interface Transactional {

  void begin();

  void commit();

  void close();

  void rollback();
}