package cn.speed.history.test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import cn.speed.history.service.MySelectService;

public class Test {
	static public void main(String[] args) {
//		Test myTest = new Test();
//		
//		MySelectService service = new MySelectService();
//		List<Map<String, Object>> list = service.test();
//		JSONArray jsonArray = JSONArray.fromObject(list);
//		String jsonData = jsonArray.toString();
//		System.out.println(jsonData);
//
//		List<String> list2insert = new ArrayList<String>();
//		list2insert.add(myTest.getNowTime("yyyy-MM-dd HH:mm:ss"));
//		list2insert.add("123");
//		list2insert.add("prottype");
//		list2insert.add("upthread");
//		list2insert.add("downthread");
//		list2insert.add("upspeed");
//		list2insert.add("downspeed");
//		list2insert.add("upspeedavg");
//		list2insert.add("downspeedavg");
//		service.testUpdate(list2insert);
		
		HashMap<String, ArrayList<String[]>> a = new HashMap<String, ArrayList<String[]>>();
		ArrayList<String[]> result = new ArrayList<String[]>();
		result.add(new String[]{"liugang","liying","what"});
		a.put("liugang", result);
		ArrayList<String[]> res = a.get("liugang");
		for(int i=0; (res!=null)&&(i<res.size()); i++){
			String[] str = res.get(i);
			for(int j=0; j<str.length; j++){
				System.out.print(str[j]+"-------");
			}
			System.out.println("");
		}
		result.add(new String[]{"liugang","gaodonghui","where"});
		ArrayList<String[]> res1 = a.get("liugang");
		for(int i=0; (res1!=null)&&(i<res1.size()); i++){
			String[] str = res1.get(i);
			for(int j=0; j<str.length; j++){
				System.out.print(str[j]+"^^^^^^^^^^");
			}
			System.out.println("");
		}
	}

	// 获取当天时间
	public String getNowTime(String dateformat) {
		Date now = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat(dateformat);// 可以方便地修改日期格式
		String hehe = dateFormat.format(now);
		return hehe;
	}
}
