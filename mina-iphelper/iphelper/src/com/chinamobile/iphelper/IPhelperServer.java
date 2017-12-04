package com.chinamobile.iphelper;


public class IPhelperServer {
	public static void main(String[] args) {
		if(args.length != 0){
			new Server(args);
		}else{
			new Server();
		}
	}
}
