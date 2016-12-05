package com.chinamobile.lab.httpserver;
import org.apache.mina.example.httpserver.codec.Server;

import com.chinamobile.httpserver.data.GlobalPara;

public class HttpServerTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		GlobalPara gp = new GlobalPara();
//		ActivemqConsumer.getInstance().init();
//		ActivemqProducer.getInstance().init();
		if(args.length != 0){
			new Server(args);
		}else{
			new Server();
		}
//		
//		//定时扫描缓存队列文件上传的log进行入库，默认时间150000ms
//		SpeedtestFiletInsert myUFI = new SpeedtestFiletInsert();
//		myUFI.start();
//		
//		//定时扫描缓存队列文件上传的log进行入库，默认时间150000ms
//		FtpFiletInsert myFFI = new FtpFiletInsert();
//		myFFI.start();
//		
//		//定时扫描缓存队列文件上传的log进行入库，默认时间150000ms
//		HttpFiletInsert myHFI = new HttpFiletInsert();
//		myHFI.start();
//		
//		//定时扫描缓存队列文件上传的log进行入库，默认时间150000ms
//		PingFiletInsert myPFI = new PingFiletInsert();
//		myPFI.start();
//		
//		//定时扫描缓存队列文件上传的log进行入库，默认时间150000ms
//		BrowseFiletInsert byPFI = new BrowseFiletInsert();
//		byPFI.start();
//		
//		//定时扫描测试结果文件进行结果入库，默认时间150000ms
//		SpeedTestAnalyse mySTA = new SpeedTestAnalyse();
////		mySTA.start();
//		mySTA.deal();
//		
//		//定时扫描测试结果文件进行结果入库，默认时间150000ms
//		FtpAnalyse myFA = new FtpAnalyse();
//		myFA.start();
//		
//		//定时扫描测试结果文件进行结果入库，默认时间150000ms
//		HttpAnalyse myHA = new HttpAnalyse();
//		myHA.start();
//		
//		//定时扫描测试结果文件进行结果入库，默认时间150000ms
//		PingAnalyse myPA = new PingAnalyse();
//		myPA.start();
//		
//		//定时扫描测试结果文件进行结果入库，默认时间150000ms
//		BrowseAnalyse byPA = new BrowseAnalyse();
//		byPA.start();
//		
//		//定时调取存储过程
//		DatabaseProc myDP = new DatabaseProc();
//		myDP.start();
		
	}
	
}
