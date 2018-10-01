package test;

import java.util.ArrayList;
import java.util.List;

import masterworker.Master;

public class MasterTest {
	
	public static void main(String args[]) {
		List<String> list = new ArrayList<String>();
		for(int i = 0; i < 20; i++) {
			list.add(i + "");
		}
		System.out.println("list ok.");
		Master master = new Master(list);
		master.run();
	}
	
}
