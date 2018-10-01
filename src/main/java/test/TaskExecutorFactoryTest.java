package test;

import com.nec.masterSlave.TaskExecutor;
import com.nec.masterSlave.TaskExecutorFactory;

public class TaskExecutorFactoryTest extends TaskExecutorFactory{

	@Override
	public TaskExecutor createExecutor(String task) {
		return new TaskExecutorTest(task);
	}

}
