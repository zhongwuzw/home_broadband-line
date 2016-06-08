package com.opencassandra.descfile;

import java.lang.Thread;

class summary extends Thread{
	DescFile desc = new DescFile();  
	@Override
	public void run(){
		DescFile.main(new String[]{});
	}
}

class monitor extends Thread{
	  DescMFile mon = new DescMFile();
	  @Override
	public void run(){
		  DescMFile.main(new String[]{});
	  }
}

class gps extends Thread{
	DescFile desc = new DescFile();
	@Override
	public void run(){
		DescFile.queryCassandraReport();
	}
}

public class ThreadTest {
  public static void main(String[] args) throws Exception{
	  Thread summary = new summary();
	  Thread monitor = new monitor();
	  Thread gps = new gps();
	  //先启动summary、monitor进程  最后启动gps进程 
	  summary.start();
	  monitor.start();
	  gps.start();
    
  }
}