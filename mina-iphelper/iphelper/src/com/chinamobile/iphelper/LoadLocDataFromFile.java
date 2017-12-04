package com.chinamobile.iphelper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class LoadLocDataFromFile {

	public static List<String> loadLocationJson(String filepath) {
		List<String> locations = new ArrayList<String>();

		try {
			// read file content from file
			InputStreamReader isr = new InputStreamReader(new FileInputStream(filepath), "UTF-8");
			BufferedReader br = new BufferedReader(isr);
			String str = null;
			while ((str = br.readLine()) != null) {
				locations.add(str);
			}
			br.close();
			isr.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return locations;
	}
}
