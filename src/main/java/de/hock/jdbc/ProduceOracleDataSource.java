/**
 * 
 */
package de.hock.jdbc;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

import de.hock.run.ConfigProperty;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

/**
 * @author hockm002
 *
 */
public class ProduceOracleDataSource {

  private static PoolDataSource pds;

  public synchronized static PoolDataSource getOraclePool(Properties properties) {
    if (Objects.isNull(pds)) {
      try {
        pds = PoolDataSourceFactory.getPoolDataSource();

        pds.setConnectionFactoryClassName(ConfigProperty.DATA_SOURCE_CLASS.propKey());

        pds.setInitialPoolSize(Integer.valueOf(properties.getProperty(ConfigProperty.INIT_POOL.propKey(), ConfigProperty.INIT_POOL.defaultValue())));
        pds.setMinPoolSize(Integer.valueOf(properties.getProperty(ConfigProperty.MIN_POOL.propKey(), ConfigProperty.MIN_POOL.defaultValue())));
        pds.setMaxPoolSize(Integer.valueOf(properties.getProperty(ConfigProperty.MAX_POOL.propKey(), ConfigProperty.MAX_POOL.defaultValue())));

        pds.setConnectionWaitTimeout(10);
        pds.setInactiveConnectionTimeout(3 * 60);
        pds.setMaxStatements(10);
        pds.setValidateConnectionOnBorrow(true);

        pds.setURL((String) properties.get(ConfigProperty.DATABASE_URL.propKey()));
        String username = (String) properties.get(ConfigProperty.DATABASE_BASIS_USERNAME.propKey());
        pds.setUser(Objects.isNull(username) || username.isEmpty() ? (String) properties.get(ConfigProperty.DATABASE_USERNAME.propKey()) : username);
        pds.setPassword((String) properties.get(ConfigProperty.DATABASE_PASSWORD.propKey()));
      } catch (SQLException ex) {
        throw new RuntimeException(ex);
      }
    }

    return pds;
  }
}
