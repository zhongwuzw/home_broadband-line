package com.opencassandra.descfile;

import java.util.ArrayList;
import java.util.List;

public class CsvWriteTest {
	public static CsvWrite csv = new CsvWrite();
	public static void main(String[] args) {
		List list = new ArrayList();
		list.add("4,1");
		list.add("5,2");
		list.add("63");
		CsvWrite.start();
		CsvWrite.write(list);
		fun1();
		fun2();
		CsvWrite.stop();
	}
	public static void fun1(){
		List list = new ArrayList();
		list.add("a,a");
		list.add("b,b");
		CsvWrite.write(list);
	}
	public static void fun2(){
		List list = new ArrayList();
		list.add("test2,test2");
		list.add("test1,test1");
		CsvWrite.write(list);
	}
}
