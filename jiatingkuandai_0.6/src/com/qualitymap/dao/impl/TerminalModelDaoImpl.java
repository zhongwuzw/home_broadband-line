package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.TerminalModelDao;
import com.qualitymap.vo.TerminalModel;

/**
 * 终端分析
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class TerminalModelDaoImpl implements TerminalModelDao{

	private SessionFactory sessionFactory;

	@Override
	public void saveModel(TerminalModel terminalModel) {
		// TODO Auto-generated method stub
		getSession().save(terminalModel);
	}

	@Override
	public void deleteqModel(TerminalModel terminalModel) {
		// TODO Auto-generated method stub
		getSession().delete(terminalModel);
	}

	@Override
	public List<TerminalModel> findModel() {
		// TODO Auto-generated method stub
		List<TerminalModel> modelList = getSession().createQuery("from TerminalModel").list();
		return modelList;
	}

	@Override
	public void updateModel(TerminalModel terminalModel) {
		// TODO Auto-generated method stub
		getSession().update(terminalModel);
	}

	
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}

	
	/**
	 * 终端分布数据统计
	 */
	@Override
	public List<Map<String, Object>> getTerminalModelData(String month, String groupid, String terminalType) {
		String sql = " SELECT sum(testtimes) testtimes,terminal_model  FROM terminal_model  WHERE `month` ='" + month + "'  " +
				"and groupid in ("+groupid+") and terminal_type in ("+terminalType+") GROUP BY terminal_model order by sum(testtimes) desc limit 0,10";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
}
