package masterworker;

public abstract class TaskExecutorFactory {
	
	public abstract TaskExecutor createExecutor(final String task);

}
