package com.nec.masterSlave;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.zeromq.ZMQ.Socket;

final class MasterHB extends Thread {
	
	static final String HEART_BEAT_MESSAGE = "gRy1WGuBlC";
	static final String BYE_MESSAGE = "kjW7W9t8gS";
	
	private int heartBeatTimeOut;
	private int heartBeatRecvTimeOut = 10000;
	private int heartBeatSendTimeOut = 1000;
	private HeartBeatScanner scanner = null;
	private boolean running = false;
	private Master master;
	private Socket replier;
	private Map<String, Long> lastHeartBeat;
	
	MasterHB(int heartBeatTimeOut, Master master, Socket replier){
		super();
		this.heartBeatTimeOut = heartBeatTimeOut;
		this.master = master;
		this.replier = replier;
	}
	
	/**
	 * Call this method to stop this thread's work gracefully.
	 * If you call this method, you should wait for some time until this thread exit successfully.
	 * It will stop heart beat scanner first, before stop this thread self.
	 */
	void stopWorking() {
		if(scanner != null) {
			scanner.stopWorking();
			try {
				scanner.join();
			} catch (InterruptedException e) {
				// TODO something
				e.printStackTrace();
			}
		}
		running = false;
	}
	
	/**
	 * Pre-process before run.
	 */
	private void ignite() {
		lastHeartBeat = new ConcurrentHashMap<String, Long>();
		scanner = new HeartBeatScanner(lastHeartBeat, heartBeatTimeOut, master);
		scanner.start();
		replier.bind("tcp://*:" + Master.HEART_BEAT_PORT);
		replier.setReceiveTimeOut(heartBeatRecvTimeOut);
		replier.setSendTimeOut(heartBeatSendTimeOut);
		running = true;
	}
	
	public void run() {
		ignite();
		
		// A loop to get heart beat message from worker's.
		// When the message is null, it means there is no message in "heartBeatRecvTimeOut" ms.
		// If the message starts with "HEART_BEAT_MESSAGE", get id and from it and put the id and receive time to hash map.
		// If the message starts with "BYE_MESSAGE", it means the worker will end and delete it from hash map.
		while(running) {
			String msg = replier.recvStr();
			// TODO what should do when all worker dead?
			if(msg == null)
				continue;
			System.out.println(msg);
			replier.send("");
			if(msg.startsWith(HEART_BEAT_MESSAGE))
				lastHeartBeat.put(msg.substring(HEART_BEAT_MESSAGE.length()), 
						System.currentTimeMillis());
			else if(msg.startsWith(BYE_MESSAGE))
				lastHeartBeat.remove(msg.substring(BYE_MESSAGE.length()));
		}
		System.out.println("heart beat thread end.");
		replier.close();
	}

	/**
	 * Check if there has any connected worker.
	 * @return true if there has any connected worker, else false.
	 */
	boolean AnyWorkerConnected() {
		return !lastHeartBeat.isEmpty();
	}
	
}
