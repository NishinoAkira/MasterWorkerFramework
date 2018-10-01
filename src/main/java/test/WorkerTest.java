package test;

import java.io.IOException;

import masterworker.Worker;
import masterworker.TaskExecutorFactory;

public class WorkerTest{

	public static void main(String args[]) {
		TaskExecutorFactory fact = new TaskExecutorFactoryTest();
		Worker slave = new Worker("192.168.1.101", fact);
		try {
			slave.startWork();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
