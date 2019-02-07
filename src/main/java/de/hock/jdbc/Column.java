/**
 *
 */
package de.hock.jdbc;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Mit dieser Annotation kann mann Klass Felder zu einer Tabellespaltenname
 * mappen.
 *
 * @author <a href="mailto:Mojammal.Hoque.B@gmail.de">Mojammal Hock</a>
 * 
 */
@Retention(RUNTIME)
@Target({ FIELD })
public @interface Column {

  String name() default "";

}
