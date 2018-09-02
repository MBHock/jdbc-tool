/**
 * 
 */
package de.hock.filehandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author hockm002
 *
 */
public class FileSystemHandler {

  private static final int MAX_DEPTH_LEVEL = 5;
  private static final Logger logger = Logger.getLogger(FileSystemHandler.class.getSimpleName());

  public List<File> readFilenamesFromPath(String path) throws IOException {

    // String importPath = (String)
    // properties.get(ConfigProperty.IMPORT_DIR.getKey());
    if (Objects.nonNull(path) || !path.isEmpty()) {

      return Files.list(Paths.get(path)).map(Path::toFile).filter(File::isFile).collect(Collectors.toList());

    }

    logger.log(Level.WARNING, "No File found in {0}", path);
    return Collections.emptyList();
  }

  public List<File> readFilenamesFromPathWithEndPattern(String path, String endPattern) throws IOException {

    if (Objects.nonNull(path) && !path.isEmpty()) {

      return Files.walk(Paths.get(path), FileVisitOption.FOLLOW_LINKS).map(Path::toFile).filter(File::isFile)
          .filter(file -> file.toString().endsWith(endPattern)).collect(Collectors.toList());

    }

    logger.log(Level.WARNING, "No File found in {0} with pattern {1}", new String[] { path, endPattern });
    return Collections.emptyList();
  }

  public Stream<Path> getStreamOfPathFilterByPattern(String targetPath, String endPattern) throws IOException {

    if (Objects.nonNull(targetPath) || !targetPath.isEmpty()) {

      return Files.walk(Paths.get(targetPath), MAX_DEPTH_LEVEL).filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
          .filter(path -> path.endsWith(endPattern));

    }

    logger.log(Level.WARNING, "No File found in {0} with pattern {1}", new String[] { targetPath, endPattern });
    return Stream.empty();
  }
}
