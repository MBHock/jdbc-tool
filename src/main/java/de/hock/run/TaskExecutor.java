/**
 * 
 */
package de.hock.run;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hockm002
 *
 */
public class TaskExecutor {

  final private ExecutorService service;
  // private Integer taskToBeExecuted;

  public TaskExecutor(int numberOfThread) {
    // , int numberOfTaskExecution
    // numberOfTaskExecution <= numberOfThread ? numberOfTaskExecution :
    // numberOfThread
    // taskToBeExecuted = numberOfTaskExecution;

    service = Executors.newFixedThreadPool(numberOfThread);
  }

  public void executeTask(Runnable runnable, Integer numberOfTaskExecution) {
    for (int counter = 0; counter < numberOfTaskExecution; counter++) {
      service.execute(runnable);
    }
  }

  public void shutdown() throws InterruptedException {
    service.shutdown();
    service.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
  }
}
