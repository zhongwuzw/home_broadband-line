package cn.speed.history.process;

import java.util.List;
import java.util.Map;

public interface ResultFileAnalyse {
	List<Map<String,Object>> anlyse();
	boolean insert2Database(List<String> data);
	boolean clearLog(String id);
	boolean deal();
	boolean dealWithSpec();
}
