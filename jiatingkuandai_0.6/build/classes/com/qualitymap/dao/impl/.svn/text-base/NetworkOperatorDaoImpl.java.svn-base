package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.NetworkOperatorDao;
import com.qualitymap.vo.NetworkOperator;

/**
 * 网络分析数据统计
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class NetworkOperatorDaoImpl implements NetworkOperatorDao {


	private SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}
	
	@Override
	public void save(NetworkOperator netopt) {
		// TODO Auto-generated method stub
		getSession().save(netopt);
		
	}
	
	@Override
	public void deletOper(NetworkOperator operator) {
		// TODO Auto-generated method stub
		getSession().delete(operator);
	}

	@Override
	public List<NetworkOperator> findById() {
		// TODO Auto-generated method stub
		List<NetworkOperator> operatorList = getSession().createQuery("from NetworkOperator").list();
		return operatorList;
	}

	@Override
	public void update(NetworkOperator nOperator) {

        Transaction trans = getSession().beginTransaction();

        getSession().update(nOperator);
        trans.commit();
	}

	/**
	 * 获取运营商占比信息
	 */
	@Override
	public List<Map<String, Object>> getOperatorData(String month, String groupid) {
		String sql = " SELECT operator,sum(new_user_num) newUserNum from network_operator WHERE `month` ='" + month + "'  and new_user_num!='0'  and groupid in ("+groupid+") GROUP BY operator order by sum(new_user_num) desc limit 0,10 ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取运营商详细信息
	 */
	@Override
	public List<Map<String, Object>> getDetailData(String month, String groupid) {
		String sql = " SELECT operator,sum(new_user_num) newUserNum,sum(accumulativ_num) accumulativ_num ,sum(newly_increase_num) newly_increase_num," +
				" sum(new_user_num)/(SELECT   	sum(new_user_num) newUserNum  FROM   network_operator WHERE `month` ='" + month + "' and groupid in ("+groupid+"))*100  proportion  " +
				"from network_operator WHERE `month` ='" + month + "' and new_user_num!='0' and groupid in ("+groupid+") GROUP BY operator order by sum(new_user_num) asc ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

}
