package masterworker;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

final class HeartBeatScanner extends Thread {

	private Map<String, Long> lastHeartBeat; 
	private Map<String, Integer> blackList;
	private int timeout;
	private int scanInterval = 30000;
	private int retry = 3;
	private boolean running = false;
	private Master master;
	
	HeartBeatScanner(Map<String, Long> lastHeartBeat, int timeout, Master master){
		this.lastHeartBeat = lastHeartBeat;
		this.timeout = timeout;
		this.master = master;
	}
	
	void stopWorking() {
		running = false;
	}
	
	public void run() {
		running = true;
		blackList = new HashMap<String, Integer>();
		try {
			while(running) {
				scan();
				Thread.sleep(scanInterval);
			}
		} catch(InterruptedException e) {
			// TODO something
		} finally {
			running = false;
			System.out.println("Scanner end.");
		}
	}
	
	private void scan() {
		Iterator<Entry<String, Long>> hbInfo = lastHeartBeat.entrySet().iterator();
		Long nowTime = System.currentTimeMillis();
		while(hbInfo.hasNext()) {
			Entry<String, Long> entry = hbInfo.next();
			long heartBeatTime = entry.getValue();
			String id = entry.getKey();
			if(nowTime - heartBeatTime > timeout)
				dealWithDeadWorker(id);
			else
				blackList.put(id, 0);
		}
	}
	
	private void dealWithDeadWorker(String id) {
		int failedTimes = blackList.getOrDefault(id, 0);
		if(failedTimes >= retry) {
			lastHeartBeat.remove(id);
			blackList.remove(id);
			master.removeWorker(id);
		} else {
			blackList.put(id, failedTimes + 1);
		}
	}
	
}
