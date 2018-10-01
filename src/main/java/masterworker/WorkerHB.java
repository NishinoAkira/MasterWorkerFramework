package masterworker;

import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

final class WorkerHB extends Thread {
	
	private boolean running = false;
	private String masterIP;
	private String selfId;
	private int receiveTimeOut = 10000;
	private int sendTimeOut = 1000;
	private int heartBeatInterval = 1000;
	private int maxRetry = 3;
	private Socket requester;
	private Worker slave;
	
	WorkerHB(String masterIP, String selfId, Socket requester, Worker slave){
		this.masterIP = masterIP;
		this.selfId = selfId;
		this.requester = requester;
		this.slave = slave;
	}
	
	void stopWorking() {
		running = false;
	}
	
	public void run() {
		running = true;
		requester.setReceiveTimeOut(receiveTimeOut);
		requester.setSendTimeOut(sendTimeOut);
		requester.connect("tcp://" + masterIP + ":" + Master.HEART_BEAT_PORT);
		try {
			while(running) {
				if(!sendMsg(MasterHB.HEART_BEAT_MESSAGE + selfId)) {
					exitForce();
				}
				Thread.sleep(heartBeatInterval);
			}
			sendMsg(MasterHB.BYE_MESSAGE + selfId);
		} catch (InterruptedException e) {
			System.out.println("Interrupted.");
		} catch (ZMQException e) {
			exitForce();
		} finally {
			requester.close();
			running = false;
		}
	}
	
	private boolean sendMsg(String msgToSend) throws ZMQException{
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
	
	private void exitForce() {
		System.out.println("Can not connect to the master node. Exit...");
		requester.close();
		slave.stopWorkForce();
		System.exit(1);
	}

}
