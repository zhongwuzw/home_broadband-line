package com.chinamobile.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;

import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipFile;
import org.apache.tools.zip.ZipOutputStream;

public class ZipUtil {

	/**
	 * 压缩文件或者文件夹 压缩采用gb2312编码，其它编码方式可能造成文件名与文件夹名使用中文的情况下压缩后为乱码。。。
	 * 
	 * @param source
	 *            要压缩的文件或者文件夹 建议使用"c:/abc"或者"c:/abc/aaa.txt"这种形式来给定压缩路径
	 *            使用"c:\\abc"
	 *            或者"c:\\abc\\aaa.txt"这种形式来给定路径的话，可能导致出现压缩和解压缩路径意外故障。。。
	 * @param zipFileName
	 *            压缩后的zip文件名称 压缩后的目录组织与windows的zip压缩的目录组织相同。
	 *            会根据压缩的目录的名称，在压缩文件夹中创建一个改名的根目录， 其它压缩的文件和文件夹都在该目录下依照原来的文件目录组织形式
	 * @throws IOException
	 *             压缩文件的过程中可能会抛出IO异常，请自行处理该异常。
	 */
	public static void ZIP(String source, String zipFileName)
			throws IOException {
		ZipOutputStream zos = new ZipOutputStream(new File(zipFileName));

		// 设置压缩的时候文件名编码为gb2312
		zos.setEncoding("gb2312");
		// System.out.println(zos.getEncoding());

		File f = new File(source);

		if (f.isDirectory()) {
			// 如果直接压缩文件夹
			ZIPDIR(source, zos, f.getName() + "/");// 此处使用/来表示目录，如果使用\\来表示目录的话，会导致压缩后的文件目录组织形式在解压缩的时候不能正确识别。
		} else {
			// 如果直接压缩文件
			ZIPDIR(f.getPath(), zos, new File(f.getParent()).getName() + "/");
			ZIPFile(f.getPath(), zos, new File(f.getParent()).getName() + "/"
					+ f.getName());
		}

		zos.closeEntry();
		zos.close();
	}

	/**
	 * zip 压缩单个文件。 除非有特殊需要，否则请调用ZIP方法来压缩文件！
	 * 
	 * @param sourceFileName
	 *            要压缩的原文件
	 * @param zipFileName
	 *            压缩后的文件名
	 * @param zipFileName
	 *            压缩后的文件名
	 * @throws IOException
	 *             抛出文件异常
	 */
	public static void ZIPFile(String sourceFileName, ZipOutputStream zos,
			String tager) throws IOException {
		// System.out.println(tager);
		ZipEntry ze = new ZipEntry(tager);
		zos.putNextEntry(ze);

		// 读取要压缩文件并将其添加到压缩文件中
		FileInputStream fis = new FileInputStream(new File(sourceFileName));
		byte[] bf = new byte[2048];
		int location = 0;
		while ((location = fis.read(bf)) != -1) {
			zos.write(bf, 0, location);
		}
		fis.close();
	}

	/**
	 * 压缩目录。 除非有特殊需要，否则请调用ZIP方法来压缩文件！
	 * 
	 * @param sourceDir
	 *            需要压缩的目录位置
	 * @param zos
	 *            压缩到的zip文件
	 * @param tager
	 *            压缩到的目标位置
	 * @throws IOException
	 *             压缩文件的过程中可能会抛出IO异常，请自行处理该异常。
	 */
	public static void ZIPDIR(String sourceDir, ZipOutputStream zos,
			String tager) throws IOException {
		// System.out.println(tager);
		ZipEntry ze = new ZipEntry(tager);
		zos.putNextEntry(ze);
		// 提取要压缩的文件夹中的所有文件
		File f = new File(sourceDir);
		File[] flist = f.listFiles();
		if (flist != null) {
			// 如果该文件夹下有文件则提取所有的文件进行压缩
			for (File fsub : flist) {
				if (fsub.isDirectory()) {
					// 如果是目录则进行目录压缩
					ZIPDIR(fsub.getPath(), zos, tager + fsub.getName() + "/");
				} else {
					// 如果是文件，则进行文件压缩
					ZIPFile(fsub.getPath(), zos, tager + fsub.getName());
				}
			}
		}
	}

	/**
	 * 解压缩zip文件
	 * 
	 * @param sourceFileName
	 *            要解压缩的zip文件
	 * @param desDir
	 *            解压缩到的目录
	 * @throws IOException
	 *             压缩文件的过程中可能会抛出IO异常，请自行处理该异常。
	 */
	public static String UnZIP(String sourceFileName, String desDir)
			throws IOException {
		String filesPath = "";
		// 创建压缩文件对象
		ZipFile zf = new ZipFile(new File(sourceFileName));

		// 获取压缩文件中的文件枚举
		Enumeration<ZipEntry> en = zf.getEntries();
		int length = 0;
		byte[] b = new byte[2048];

		// 提取压缩文件夹中的所有压缩实例对象
		while (en.hasMoreElements()) {
			ZipEntry ze = en.nextElement();
			// System.out.println("压缩文件夹中的内容："+ze.getName());
			// System.out.println("是否是文件夹："+ze.isDirectory());
			// 创建解压缩后的文件实例对象

			String fileName = ze.getName();// 获得文件名，包括路径
			System.out.println("String fileName(默认)"+fileName);
			fileName = new String((ze.getName()).getBytes("gbk"),"utf-8");// 获得文件名，包括路径
			System.out.println("String fileName(GBK->UTF-8)"+fileName);
			fileName = new String((ze.getName()).getBytes("gb2312"),"utf-8");// 获得文件名，包括路径
			System.out.println("String fileName(gb2312->UTF-8)"+fileName);
			fileName = new String((ze.getName()).getBytes("ISO-8859-1"),"utf-8");// 获得文件名，包括路径
			System.out.println("String fileName(ISO-8859-1->UTF-8)"+fileName);
			
			File f = new File(desDir + ze.getName());
			filesPath += f.getAbsolutePath()+";";
			//File f = new File(desDir + fileName);
			// System.out.println("解压后的内容："+f.getPath());
			// System.out.println("是否是文件夹："+f.isDirectory());
			// 如果当前压缩文件中的实例对象是文件夹就在解压缩后的文件夹中创建该文件夹
			if (ze.isDirectory()&&!f.exists()) {
				ze.setUnixMode(755);//解决linux乱码
				f.mkdirs();
			} else if(!ze.isDirectory()){
				ze.setUnixMode(644);//解决linux乱码  
				// 如果当前解压缩文件的父级文件夹没有创建的话，则创建好父级文件夹
				if (!f.getParentFile().exists()) {
					f.getParentFile().mkdirs();
				}

				// 将当前文件的内容写入解压后的文件夹中。
				OutputStream outputStream = new FileOutputStream(f);
				InputStream inputStream = zf.getInputStream(ze);
				while ((length = inputStream.read(b)) > 0)
					outputStream.write(b, 0, length);

				inputStream.close();
				outputStream.close();
			}
		}
		zf.close();
		return filesPath;
	}
	
	/**
	 * 解压缩zip文件
	 * 
	 * @param sourceFileName
	 *            要解压缩的zip文件
	 * @param desDir
	 *            解压缩到的目录
	 * @param endWith
	 *            为文件末尾添加内容
	 * @throws IOException
	 *             压缩文件的过程中可能会抛出IO异常，请自行处理该异常。
	 */
	public static String UnZIP(String sourceFileName, String desDir, String endWith)
			throws IOException {
		String filesPath = "";
		// 创建压缩文件对象
		ZipFile zf = new ZipFile(new File(sourceFileName));

		// 获取压缩文件中的文件枚举
		Enumeration<ZipEntry> en = zf.getEntries();
		int length = 0;
		byte[] b = new byte[2048];

		// 提取压缩文件夹中的所有压缩实例对象
		while (en.hasMoreElements()) {
			ZipEntry ze = en.nextElement();
			// System.out.println("压缩文件夹中的内容："+ze.getName());
			// System.out.println("是否是文件夹："+ze.isDirectory());
			// 创建解压缩后的文件实例对象

//			String fileName = ze.getName();// 获得文件名，包括路径
//			System.out.println("String fileName(默认)"+fileName);
//			fileName = new String((ze.getName()).getBytes("gbk"),"utf-8");// 获得文件名，包括路径
//			System.out.println("String fileName(GBK->UTF-8)"+fileName);
//			fileName = new String((ze.getName()).getBytes("gb2312"),"utf-8");// 获得文件名，包括路径
//			System.out.println("String fileName(gb2312->UTF-8)"+fileName);
//			fileName = new String((ze.getName()).getBytes("ISO-8859-1"),"utf-8");// 获得文件名，包括路径
//			System.out.println("String fileName(ISO-8859-1->UTF-8)"+fileName);
		
			File f = new File(desDir + ze.getName()+endWith);
			filesPath += f.getAbsolutePath()+";";
			//File f = new File(desDir + fileName);
			// System.out.println("解压后的内容："+f.getPath());
			// System.out.println("是否是文件夹："+f.isDirectory());
			// 如果当前压缩文件中的实例对象是文件夹就在解压缩后的文件夹中创建该文件夹
			if (ze.isDirectory()) {
				ze.setUnixMode(755);//解决linux乱码
				File fd = new File(desDir + ze.getName());
				if(!fd.exists()){
					fd.mkdirs();
				}
			} else if(!ze.isDirectory()){
				ze.setUnixMode(644);//解决linux乱码  
				// 如果当前解压缩文件的父级文件夹没有创建的话，则创建好父级文件夹
				if (!f.getParentFile().exists()) {
					f.getParentFile().mkdirs();
				}

				// 将当前文件的内容写入解压后的文件夹中。
				OutputStream outputStream = new FileOutputStream(f);
				InputStream inputStream = zf.getInputStream(ze);
				while ((length = inputStream.read(b)) > 0)
					outputStream.write(b, 0, length);

				inputStream.close();
				outputStream.close();
			}
		}
		zf.close();
		return filesPath;
	}
}
