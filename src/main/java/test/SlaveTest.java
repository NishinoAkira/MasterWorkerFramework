package test;

import java.io.IOException;

import com.nec.masterSlave.Slave;
import com.nec.masterSlave.TaskExecutorFactory;

public class SlaveTest{

	public static void main(String args[]) {
		TaskExecutorFactory fact = new TaskExecutorFactoryTest();
		Slave slave = new Slave("192.168.1.101", fact);
		try {
			slave.startWork();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
