/**
 *
 */
package de.hock.jdbc;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 */
@Retention(RUNTIME)
@Target({ TYPE })
public @interface TableName {

  String name() default "";

}
