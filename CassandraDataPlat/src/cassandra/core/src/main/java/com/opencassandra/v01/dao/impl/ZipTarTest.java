package com.opencassandra.v01.dao.impl;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.csvreader.CsvReader;

public class ZipTarTest {

	public static void main(String[] args) {
		String inPath = "C:\\Users\\issuser\\Desktop\\2015_01_08_11_22_09_023-country-city.zip";
		File in = new File(inPath);
		String outPath = inPath.substring(0, inPath.lastIndexOf("."));
		System.out.println(outPath);
		unZip(inPath, outPath);
	}
	
	/**
	 * 解压zip格式的压缩包
	 * 
	 * @param filePath
	 *            压缩文件路径
	 * @param outPath
	 *            输出路径
	 * @return 解压成功或失败标志
	 */
	public static Boolean unZip(String inPath, String outPath) {
		String unzipfile = inPath; // 解压缩的文件名
		try {
			ZipInputStream zin = new ZipInputStream(new FileInputStream(
					unzipfile));
			ZipEntry entry;
			// 创建文件夹
			while ((entry = zin.getNextEntry()) != null) {
				if (entry.isDirectory()) {
					File directory = new File(outPath, entry.getName());
					if (!directory.exists()) {
						if (!directory.mkdirs()) {
							System.exit(0);
						}
					}
					zin.closeEntry();
				} else {
					File myFile = new File(outPath + File.separator
							+ entry.getName());
					if (!myFile.exists()) {
						if (!myFile.getParentFile().exists()) {
							myFile.getParentFile().mkdirs();
						}
						myFile.createNewFile();
					}
					FileOutputStream fout = new FileOutputStream(myFile);
					DataOutputStream dout = new DataOutputStream(fout);
					byte[] b = new byte[1024];
					int len = 0;
					while ((len = zin.read(b)) != -1) {
						dout.write(b, 0, len);
					}
					dout.close();
					fout.close();
					zin.closeEntry();
				}
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	/**
	 * 读取CSV文件
	 */
	public ArrayList<String[]> readCsv(File filePath) {
		ArrayList<String[]> csvList = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath.getAbsolutePath();
			CsvReader reader = new CsvReader(csvFilePath, ',', Charset
					.forName("GB2312")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
				String[] values = null;
				values = reader.getValues();
				csvList.add(values);
			}
			reader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {

		}
		return csvList;
	}
}
