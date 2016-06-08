package cn.speed.history.process;
import java.util.List;
import java.util.Map;

public interface DealWithCsv{

	/**
	 * 读取deviceInfo.CSV文件
	 */
	public Map<String,Object> readInfoCsv(String filePath);
	
	/**
	 * 读取Detail.CSV文件
	 */
	public Map<String,Object> readDetailCsv(String filePath,Integer fil_id);

	/**
	 * 读取Detail.CSV文件
	 */
	public void insertData(String infoCsv, String detailCsv,Integer fil_id);

	/**
	 * 读取数据库寻找未入库CSV结果文件
	 */
	public boolean findFile(Map<String,Object> resultSet);
}
