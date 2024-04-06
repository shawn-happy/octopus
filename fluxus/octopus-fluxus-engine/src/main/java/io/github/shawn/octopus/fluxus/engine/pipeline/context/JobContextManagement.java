package io.github.shawn.octopus.fluxus.engine.pipeline.context;

/** Job上下文管理 */
public class JobContextManagement {
  private static final ThreadLocal<JobContext> CONTEXT = new ThreadLocal<>();

  public static void setJob(JobContext job) {
    CONTEXT.set(job);
  }

  public static JobContext getJob() {
    return CONTEXT.get();
  }

  public static void clear() {
    CONTEXT.remove();
  }
}
