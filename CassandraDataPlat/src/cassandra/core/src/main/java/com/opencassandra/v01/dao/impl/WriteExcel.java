package com.opencassandra.v01.dao.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.format.Colour;
import jxl.format.UnderlineStyle;
import jxl.write.DateFormat;
import jxl.write.DateTime;
import jxl.write.Label;
import jxl.write.Number;
import jxl.write.NumberFormat;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;

public class WriteExcel {

	/**
	 * @param args
	 */
//	public static void main(String[] args) {
//		try {
//			File file = new File("D:\\test.xls");
//			OutputStream os = new FileOutputStream(file);
//			WritableWorkbook wwb = Workbook.createWorkbook(os);
//			WritableSheet ws = wwb.createSheet("test", 0);
//			for (int i = 0; i < 10; i++) {
//				for (int j = 0; j < 10; j++) {
//					Label label = new Label(i, j, i + j + "");
//					ws.addCell(label);
//				}
//			}
//			//写入工作表
//			wwb.write();
//			wwb.close();
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//	}
	public static void main(String[] args) {
		try {
			String filePath = "D://test.xls";
			WriteExcel we = new WriteExcel();
			we.readExcel(filePath);
			File writeFile = new File("D://test1.xls");
			writeFile.createNewFile();
			OutputStream os = new FileOutputStream(writeFile);	
			we.writeExcel(os);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	//输出excel
	public void writeExcel(OutputStream os){
		try {
			WritableWorkbook wwb = Workbook.createWorkbook(os);
			WritableSheet ws = wwb.createSheet("Test Sheet", 0);
			//开始添加数据
			//1.添加lable对象
			Label label = new Label(0,0,"测试");
			ws.addCell(label);
			//添加带有字型的formatting对象
			WritableFont wf = new WritableFont(WritableFont.TIMES,18,WritableFont.BOLD,true);
			WritableCellFormat wcf = new WritableCellFormat(wf);
			Label labelcf = new Label(1,0,"this is a test label",wcf);
			//添加带有字体颜色的Formatting
			WritableFont wfc = new WritableFont(WritableFont.ARIAL,10,WritableFont.NO_BOLD,false,UnderlineStyle.NO_UNDERLINE,Colour.DARK_YELLOW);
			WritableCellFormat wcfFc = new WritableCellFormat(wfc);
			Label labelCF = new Label(1,0,"OK",wcfFc);
			ws.addCell(labelCF);
			//2.添加Number对象
			jxl.write.Number labelN = new jxl.write.Number(0,1,3.1415926); 
			ws.addCell(labelN);
			//添加带有formatting的Number对象
			NumberFormat nf = new NumberFormat("#.##");
			WritableCellFormat wcfN = new WritableCellFormat(nf);
			Number labelNF = new Number(1,1,3.1415926,wcfN);
			ws.addCell(labelNF);
			// 3.添加Boolean对象
			jxl.write.Boolean labelB = new jxl.write.Boolean(0, 2, true);
			ws.addCell(labelB);
			jxl.write.Boolean labelB1 = new jxl.write.Boolean(1, 2, false);
			ws.addCell(labelB1);  
			//4.添加DateTime对象
			DateTime labelDT = new DateTime(0,3,new Date());
			ws.addCell(labelDT);
			//5.添加带有formatting的DateTime对象
			DateFormat df = new DateFormat("dd MM yyyy hh:mm:ss");
			WritableCellFormat wcfDF = new WritableCellFormat(df);
			DateTime labelDTF = new DateTime(1, 3, new java.util.Date(), wcfDF);
			ws.addCell(labelDTF);
			//7.写入工作表   
            wwb.write();   
            wwb.close();   
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	//读取数据
	public void readExcel(String filePath){
		try {
			Workbook readweb = null;
			
			InputStream instream = new FileInputStream(filePath);
			readweb = Workbook.getWorkbook(instream);
			Sheet readSheet = readweb.getSheet(0);
			//获取Sheet表中包含的总行数
			int Rowcounts = readSheet.getRows();
			//获取Sheet表中包含的总列数
			int Columncounts = readSheet.getColumns();
			
			for (int i = 0; i < Rowcounts; i++) {
				for (int j = 0; j < Columncounts; j++) {
					Cell cell = readSheet.getCell(j, i);
					System.out.print(cell.getContents()+"   ");
				}
				System.out.println();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
