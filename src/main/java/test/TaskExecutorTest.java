package test;

import com.nec.masterSlave.TaskExecutor;

public class TaskExecutorTest extends TaskExecutor{
	
	private String task;
	
	TaskExecutorTest(String task){
		this.task = task;
	}

	protected boolean handle() {
		System.out.println("Get task: print " + task + " and sleep 5s");
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Awake!");
		return true;
	}

}
