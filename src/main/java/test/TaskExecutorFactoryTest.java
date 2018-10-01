package test;

import masterworker.TaskExecutor;
import masterworker.TaskExecutorFactory;

public class TaskExecutorFactoryTest extends TaskExecutorFactory{

	@Override
	public TaskExecutor createExecutor(String task) {
		return new TaskExecutorTest(task);
	}

}
