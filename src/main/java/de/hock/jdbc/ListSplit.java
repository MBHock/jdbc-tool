package de.hock.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ListSplit {

  public <T> List<List<T>> splitList(List<T> elements, int sublistItemSize) {

    List<List<T>> sublists = new ArrayList<>();
    if (Objects.isNull(elements) || elements.isEmpty()) {
      return sublists;
    }

    int numberOfElements = elements.size();
    int chopSize = numberOfElements / sublistItemSize;

    int loopCount = 0;
    int fromIndex = 0;
    int toIndex = 0;
    while (loopCount < chopSize) {
      fromIndex = loopCount * sublistItemSize;
      loopCount++;
      toIndex = loopCount * sublistItemSize;
      sublists.add(elements.subList(fromIndex, toIndex));
    }

    if (toIndex < numberOfElements) {
      sublists.add(elements.subList(toIndex, numberOfElements));
    }

    return sublists;
  }
}
