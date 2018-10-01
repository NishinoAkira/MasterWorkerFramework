package com.nec.masterSlave;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import common.IPAddress;

public final class Master{
	
	public static final int SUCCESS_CODE = 0;
	
	public static final int NO_ANY_WORKER = -1;
	public static final int CAN_NOT_LAUNCH_ANY_WORKER = -2;
	public static final int CONNECT_FAILED = -3;
	public static final int WORKER_FILE_NOT_FOUND = -4;
	public static final int FORMAT_ERROR = -5;
	
	public static final int HEART_BEAT_PORT = 52361;
	public static final int TASK_PORT = HEART_BEAT_PORT + 1;
	
	private static final String KNOWN_HOST_FILE = "~/.ssh/known_hosts";
	private static final String IDENTITY_RSA_FILE = "~/.ssh/id_rsa";
	
	static final String REQUEST = "7kY8WTgm44";
	static final String TASK_FINISH = "oeZ2jemSJB";
	static final String TASK_FAILED = "ZZO0t5clVF";
	static final String WAIT = "l5qRLfBK9A";
	static final String EXIT = "JwzVyT3AIC";
	static final String DEFAULT_MSG = "";
	
	private List<String> launchWorkerList;
	private Queue<String> taskQueue;
	private Map<String, List<String>> executingTask;
	private JSch jsch;
	private String workerListPath = null;
	private String workerStartCommand;
	private int workerLaunchRetry = 3;
	private int connectTimeout = 10000;
	private int heartBeatTimeout = 30000;
	private int heartBeatRecvTimeOut = 10000;
	private int heartBeatSendTimeOut = 1000;
	private MasterHB heartBeat;
	private Context context;
	private Socket replier;
	
	public Master(List<String> taskList){
		this.taskQueue = new ConcurrentLinkedQueue<String>(taskList);
	}
	
	public Master(List<String> taskList, int heartBeatTimeout){
		this(taskList);
		this.heartBeatTimeout = heartBeatTimeout;
	}
	
	public Master(List<String> taskList, String workerStartCommand, String workerListPath){
		this(taskList);
		this.workerStartCommand = new String(workerStartCommand);
		this.workerListPath = new String(workerListPath);
	}
	
	public Master(List<String> taskList, String workerStartCommand, String workerListPath,
			int workerLaunchRetry, int connectTimeout, int heartBeatTimeout){
		this(taskList, workerStartCommand, workerListPath);
		this.workerLaunchRetry = workerLaunchRetry;
		this.connectTimeout = connectTimeout;
		this.heartBeatTimeout = heartBeatTimeout;
	}
	
	/**
	 * Pre-process before run
	 */
	private void ignite(){
		executingTask = new Hashtable<String, List<String>>();
		context = ZMQ.context(1);
		heartBeat = new MasterHB(heartBeatTimeout, this, context.socket(ZMQ.REP));
		heartBeat.start();
		replier = context.socket(ZMQ.REP);
		replier.bind("tcp://*:" + Master.TASK_PORT);
		replier.setReceiveTimeOut(heartBeatRecvTimeOut);
		replier.setSendTimeOut(heartBeatSendTimeOut);
		System.out.println("prepare ok. waiting request.");
	}
	
	// TODO Is this necessary?
	public void stopWorking() {
		if(heartBeat != null)
			heartBeat.stopWorking();
		taskQueue.clear();
		executingTask.clear();
	}
	
	public void run() {
		ignite();
		while(!taskQueue.isEmpty() || !executingTask.isEmpty() || heartBeat.AnyWorkerConnected()) {
			System.out.println("wait req.");
			String msg = replier.recvStr();
			if(msg == null) continue;
			System.out.println(msg);
			if(msg.startsWith(REQUEST)) {
				String id = msg.substring(REQUEST.length());
				sendTask(id);
			}else if(msg.startsWith(TASK_FINISH)) {
				String idAndTask = msg.substring(TASK_FINISH.length());
				int spliter = idAndTask.indexOf(":");
				String id = idAndTask.substring(0, spliter);
				String task = idAndTask.substring(spliter + 1);
				List<String> taskList = executingTask.getOrDefault(id, null);
				if(taskList != null) {
					taskList.remove(task);
					if(taskList.isEmpty())
						executingTask.remove(id);
				}
				replier.send(DEFAULT_MSG);
			}else {
				// Unknown message.
				replier.send(DEFAULT_MSG);
			}
		}
		System.out.println("end.");
		replier.close();
		heartBeat.stopWorking();
		try {
			heartBeat.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		context.close();
	}
	
	void removeWorker(String id) {
		List<String> taskList = executingTask.remove(id);
		if(taskList != null) {
			for(String task: taskList) {
				taskQueue.add(task);
			}
		}
	}
	
	private void sendTask(String id) {
		if(!taskQueue.isEmpty()) {
			String task = taskQueue.peek();
			boolean sendResult = replier.send(task);
			if(!sendResult)
				return;
			taskQueue.poll();
			List<String> taskList = executingTask.getOrDefault(id, null);
			if(taskList == null) {
				taskList = new LinkedList<String>();
				executingTask.put(id, taskList);
			}
			taskList.add(task);
		}
		else if(!executingTask.isEmpty())
			replier.send(WAIT);
		else {
			replier.send(EXIT);
		}
	}
	
	/////////////////////////////////////////////////////////////
	/// Methods for workers launcher
	/////////////////////////////////////////////////////////////
	
	/**
	 * Public method. Use this method to launch all workers in the worker list.
	 * @return An error
	 * @throws IOException
	 * @throws JSchException
	 */
	public int launchAllWorkers() throws IOException, JSchException {
		jsch = new JSch();
		jsch.setKnownHosts(KNOWN_HOST_FILE);
		jsch.addIdentity(IDENTITY_RSA_FILE);
		int exitCode = getWorkerList();
		if(exitCode != SUCCESS_CODE) {
			return exitCode;
		}
		if(launchWorkerList == null || launchWorkerList.isEmpty()) {
			return NO_ANY_WORKER;
		}
		
		int successCounter = launchAllWorkersWithRetry();
		if(successCounter == 0)
			return CAN_NOT_LAUNCH_ANY_WORKER;
		return SUCCESS_CODE;
	}
	
	public boolean launchWorker(String ip) {
		return launchWorkerWithRetry(ip);
	}
	
	/**
	 * The method to load the worker list.
	 * A valid format of one line can be: valid IP address, starts with # means comment, only space means nothing
	 * 
	 * @return Master.SUCCESS_CODE          when success.
	 * @return Master.WORKER_FILE_NOT_FOUND when the worker list's path is not being defined or can't be found.
	 * @return Master.FORMAT_ERROR          when there is a invalid line in the worker list file.
	 * @throws IOException
	 */
	private int getWorkerList() throws IOException {
		if(workerListPath == null)
			return WORKER_FILE_NOT_FOUND;
		List<String> list = new ArrayList<String>();
		FileInputStream fis = null;
		BufferedReader reader = null;
		try {
			fis = new FileInputStream(workerListPath);
			reader = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			int counter = 0;    // For counting line number
			while((line = reader.readLine()) != null) {
				counter++;
				String ipStr = line.replace(" ", "");
				if(ipStr.startsWith("#") || ipStr.isEmpty())
					continue;
				IPAddress ip = new IPAddress(ipStr);
				if(!ip.isValid()) {
					System.out.println("The IP address in line " + counter + " is invalid.");
					return FORMAT_ERROR;
				}
				list.add(ip.getIP());
			}
		} catch(FileNotFoundException e){
			return WORKER_FILE_NOT_FOUND;
		} finally {
			if(fis != null)
				fis.close();
			if(reader != null)
				reader.close();
		}
		launchWorkerList = new ArrayList<String>(list);
		return SUCCESS_CODE;
	}
	
	/**
	 * 
	 * @return
	 */
	private int launchAllWorkersWithRetry() {
		int successCounter = 0;
		
		for(String ip: launchWorkerList)
			if(launchWorkerWithRetry(ip))
				successCounter++;
		return successCounter;
	}
	
	/**
	 * 
	 * @param ip
	 * @return
	 */
	private boolean launchWorkerWithRetry(String ip) {
		int retry = 0;
		while(retry < workerLaunchRetry) {
			if(launch(ip) == SUCCESS_CODE)
				return true;
			retry++;
		}
		if(retry >= workerLaunchRetry) {
			System.out.println("Worker " + ip + "can not be launched. Please check the log for the reason.");
		}
		return false;
	}
	
	/**
	 * launch one worker.
	 * 
	 * @param ip
	 * @return
	 */
	private int launch(String ip){
		Session session = null;
		ChannelExec channelExec = null;
		
		try {
			session = jsch.getSession(ip);
			session.setConfig("PreferredAuthentications", "publickey");
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect(connectTimeout);
			channelExec = (ChannelExec) session.openChannel("exec");
			// work background.
			channelExec.setCommand(workerStartCommand + " &");
			channelExec.connect(connectTimeout);
			
			// Why not check a exit status here?
			// Because the code only fork a new process background to do the work
			// so it is unable know the process's exit status is true or false (the process is still running)
			// we can only confirm the status of workers by heart beat.
				
		} catch(JSchException e) {
			// write a log here.
			return CONNECT_FAILED;
		} finally {
			if(channelExec != null)
				channelExec.disconnect();
			if(session != null)
				session.disconnect();
		}
		return SUCCESS_CODE;
	}

}
