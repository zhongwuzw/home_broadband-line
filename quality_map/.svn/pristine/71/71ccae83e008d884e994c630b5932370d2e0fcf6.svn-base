package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.NetworkTypeDao;
import com.qualitymap.vo.NetworkType;

/**
 * 获取网络制式数据统计
 * @author：kxc
 * @date：Apr 11, 2016
 */
public class NetworkTypeDaoImpl implements NetworkTypeDao{

	private SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}
	
	@Override
	public void saveType(NetworkType nettype) {
		// TODO Auto-generated method stub
		/*Transaction transaction = getSession().beginTransaction();
		 transaction.commit();*/
		 getSession().save(nettype);
	}

	@Override
	public void deleteType(NetworkType news) {
		// TODO Auto-generated method stub
	/*	 news = new NetworkType();

        news.setId(32422);  //用下面那句效果一样，只是多了句select
*/
         news = (NetworkType)getSession().get(NetworkType.class, 8);
		getSession().delete(news);
	}

	@Override
	public List<NetworkType> find() {
		// TODO Auto-generated method stub
		List<NetworkType> nettypeList = getSession().createQuery("from NetworkType").list();
		return nettypeList;
	}

	@Override
	public void updateType(NetworkType nettype) {
		// TODO Auto-generated method stub
		getSession().update(nettype);
	}

	/**
	 * 获取联网方式信息
	 */
	@Override
	public List<Map<String, Object>> getNetTypeData(String month, String groupid) {
		String sql = " SELECT net_type,sum(new_user_num) newUserNum,sum(accumulativ_num) accumulativ_num ,sum(newly_increase_num) newly_increase_num," +
				"province ,sum(new_user_num)/(SELECT   	sum(new_user_num) newUserNum  FROM   network_operator WHERE `month` ='" + month + "' and groupid in ("+groupid+"))*100 proportion  " +
				"from network_type WHERE `month` ='" + month + "' and groupid in ("+groupid+") GROUP BY net_type ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
}
