package masterworker;

import java.util.concurrent.BlockingQueue;

public abstract class TaskExecutor implements Runnable {
	
	private BlockingQueue<String> finishReceiver;
	private String privateTask;

	public void run() {
		if(handle())
			finishReceiver.add(Master.TASK_FINISH + privateTask);
		else
			finishReceiver.add(Master.TASK_FAILED + privateTask);
	}
	
	protected abstract boolean handle();
	
	void setFinishReceiver(BlockingQueue<String> finishReceiver) {
		this.finishReceiver = finishReceiver;
	}
	
	void setPrivateTask(String privateTask) {
		this.privateTask = privateTask;
	}
	
}
