package com.nec.masterSlave;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

public class Slave {
	
	private String masterIP;
	private String id;
	private SlaveHB heartBeat;
	private Context context;
	private Socket requester;
	private int receiveTimeOut = 10000;
	private int sendTimeOut = 1000;
	private int waitInterval = 6000;
	private int maxRetry = 3;
	private boolean running = false;
	private ThreadPoolExecutor threadPool;
	private BlockingQueue<String> finishReceiver;
	private int maxThreadNum = 4;
	private TaskExecutorFactory executorFactory;
	private int workingThreadNumber;

	
	public Slave(String masterIP, TaskExecutorFactory executorFactory) {
		this.masterIP = masterIP;
		this.executorFactory = executorFactory;
	}
	
	public final void startWork() throws IOException {
		id = getId();
		context = ZMQ.context(1);
		heartBeat = new SlaveHB(masterIP, id, context.socket(ZMQ.REQ), this);
		heartBeat.start();
		requester = context.socket(ZMQ.REQ);
		requester.connect("tcp://" + masterIP + ":" + Master.TASK_PORT);
		requester.setReceiveTimeOut(receiveTimeOut);
		requester.setSendTimeOut(sendTimeOut);
		threadPool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		finishReceiver = new LinkedBlockingQueue<String>();
		workingThreadNumber = 0;
		running = true;
		execute();
	}
	
	private void execute() {
		try {
			while(running) {
				if(!getAndAssignTask())
					break;
			}
		} catch(InterruptedException e) {
			// TODO
			e.printStackTrace();
		} finally {
			threadPool.shutdown();
			heartBeat.stopWorking();
			try {
				heartBeat.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			requester.close();
			context.close();
			running = false;
		}
	}
	
	private boolean getAndAssignTask() throws InterruptedException {
		if(!finishReceiver.isEmpty() || workingThreadNumber >= maxThreadNum) {
			String msgReceived = finishReceiver.poll(10, TimeUnit.SECONDS);
			if(msgReceived != null) {
				if(msgReceived.startsWith(Master.TASK_FINISH))
					sendTaskFinished(msgReceived.substring(Master.TASK_FINISH.length()));
				else if(msgReceived.startsWith(Master.TASK_FAILED))
					sendTaskFailed(msgReceived.substring(Master.TASK_FAILED.length()));
				workingThreadNumber--;
			}
			System.out.println("workingThreadNumber = " + workingThreadNumber);
			System.out.println("2");
		} else {
			String task = sendGetTask();
			if(task != null) {
				switch(task){
				case Master.EXIT: 
					return false;
				case Master.WAIT:
					Thread.sleep(waitInterval);
					break;
				default: 
					TaskExecutor executor = executorFactory.createExecutor(task);
					executor.setFinishReceiver(finishReceiver);
					executor.setPrivateTask(task);
					threadPool.execute(executor);
					workingThreadNumber++;
				}
			}
			System.out.println("workingThreadNumber = " + workingThreadNumber);
		}
		return true;
	}
	
	private String sendGetTask() {
		String ret;
		try {
			ret = sendMsgAndRecv(Master.REQUEST + id);
		} catch(ZMQException e) {
			System.out.println("send get task time out");
			ret = null;
		}
		return ret;
	}
	
	private void sendTaskFinished(String task) {
		try {
			sendMsg(Master.TASK_FINISH + id + ":" + task);
		} catch(ZMQException e) {
			// TODO
			e.printStackTrace();
		}
	}
	
	private void sendTaskFailed(String task) {
		try {
			sendMsg(Master.TASK_FAILED + id + ":" + task);
		} catch(ZMQException e) {
			// TODO
			e.printStackTrace();
		}
	}
	
	private boolean sendMsg(String msgToSend) {
		int retryTimes = 0;
		while(retryTimes < maxRetry) {
			if(requester.send(msgToSend)) {
				requester.recvStr();
				return true;
			}
			else
				retryTimes += 1;
		}
		return false;
	}
	
	private String recvMsg() {
		int retryTimes = 0;
		String result;
		while(retryTimes < maxRetry) {
			if((result = requester.recvStr()) != null)
				return result;
			else
				retryTimes += 1;
		}
		return null;
	}
	
	private String sendMsgAndRecv(String msgToSend) {
		int retryTimes = 0;
		while(retryTimes < maxRetry) {
			if(requester.send(msgToSend)) {
				return recvMsg();
			}
			else
				retryTimes += 1;
		}
		return null;
	}
	
	void stopWorkForce() {
		requester.close();
		context.close();
		running = false;
	}
	
	private String getId() throws IOException {
		String hostname = executeProcess("hostname");
		String uuid = executeProcess("uuidgen");
		return hostname + " " + uuid;
	}
	
	private String executeProcess(String cmd) throws IOException {
		ProcessBuilder builder = new ProcessBuilder("/bin/sh", "-c", cmd);
		Process process = builder.start();
		InputStream is = process.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder cmdout = new StringBuilder();
		String line = null;
		while((line = reader.readLine()) != null) {
			cmdout.append(line);
		}
		return cmdout.toString();
	}

}
