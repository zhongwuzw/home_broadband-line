package cn.speed.jdbc.utils;

import cn.speed.history.dao.DatabaseProcDao;

public class DatabaseProc extends Thread{

	public DatabaseProcDao myDp = new DatabaseProcDao();
	public DatabaseProc(){
		
	}
	
	public boolean proc(){
		myDp.excuteProc("{CALL bak_into_speedtest_history()} ");
		myDp.excuteProc("{CALL bak_into_ftp_history()} ");
		myDp.excuteProc("{CALL bak_into_http_history()} ");
		myDp.excuteProc("{CALL bak_into_ping_history()} ");
		myDp.excuteProc("{CALL bak_into_browse_history()} ");
		return true;
	}
	
	public void run(){
		while(true){
			try {
				this.proc();
				Thread.sleep(70000);//70000
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				try {
					Thread.sleep(70000);//70000
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} catch (Exception e){
				e.printStackTrace();
				try {
					Thread.sleep(70000);//70000
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}
}
