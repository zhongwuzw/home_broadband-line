package com.cmri.ots.sso.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cmri.base.bean.SessionUser;
import com.cmri.ots.sso.bean.Org;
import com.cmri.ots.sso.bean.User;
import com.cmri.ots.sso.bean.UserAgent;
import com.cmri.ots.sso.bean.UserStatues;
import com.cmri.ots.sso.dao.AuthDao;
@Service
public class AuthDaoImpl implements AuthDao{

	@Autowired
	SqlSessionTemplate sqlSessionTemplate;
	
	@Override
	public UserAgent getByMessage(UserAgent agent) {
		return sqlSessionTemplate.selectOne("agentdao.getuseragent", agent);
	}

	@Override
	public void saveuserAgent(UserAgent userAgent) {
		sqlSessionTemplate.insert("agentdao.saveUserAgent",userAgent);
	}

	@Override
	public void getorgbyuser(User userco) {
		
	}

	@Override
	public User doLogin(User userco) {
		Map map = new HashMap();
		map.put("username", userco.getUsername());
		map.put("password", userco.getPassword());
		return sqlSessionTemplate.selectOne("userDao.doLogin",map);
	}

	@Override
	public void updateUser(User user) {
		sqlSessionTemplate.update("userDao.updateUser",user);
	}

	@Override
	public List getroles(User userco) {
		return sqlSessionTemplate.selectList("userDao.getRolesByuser",userco);
	}

	@Override
	public Map getResource(String roleId) {
		Map result = new HashMap();
		Map map = new HashMap();
		map.put("roleId", roleId);
		if(roleId!=null&&"".equals(roleId)){
			List<Map> list =  sqlSessionTemplate.selectList("userDao.getResource",map);
			for(Map maptmp :list){
				result.put(maptmp.get("resource_tag"), maptmp.get("resource_Type"));
			}
			return result;
		}
		return null;
	}

	@Override
	public List<Org> searchUserTeam(Integer id) {
		Map map = new HashMap();
		map.put("uid", id);
		return sqlSessionTemplate.selectList("userDao.searchUserTeam",map);
	}

	@Override
	public List getUserGroup(SessionUser sessionUser) {
		Map map = new HashMap();
		map.put("id", sessionUser.getId());
		return sqlSessionTemplate.selectList("userDao.getUserGroup",map);
	}

	@Override
	public void saveUserstatues(UserStatues usersta) {
		sqlSessionTemplate.insert("userDao.save_user_statues",usersta);
	}

	@Override
	public User getUserByApiKey(String apikey) {
		Map map = new HashMap();
		map.put("key",apikey);
		return sqlSessionTemplate.selectOne("userDao.getUserByApiKey",map);
	}

	@Override
	public JSONArray queryByUserId(Integer userId) {
		Map map = new HashMap();
		map.put("userId",userId);
		List<Map> list = sqlSessionTemplate.selectList("userDao.queryByUserId",map);
		JSONArray array = new JSONArray();
		for(Map tmp : list){
			JSONObject obj = new JSONObject();
			obj.put("id", tmp.get("id") == null? "" : map.get("id") + "");
			obj.put("name", tmp.get("name"));
			obj.put("group_id", tmp.get("group_id"));
			obj.put("parent_org_id", tmp.get("parent_org_id"));
			obj.put("org_id",tmp.get("org_id"));
			obj.put("key",tmp.get("org_key"));
			obj.put("type", tmp.get("type"));
			array.add(obj);
		}
		return array;
	}

	@Override
	public JSONArray queryByParentOrgId(String orgId, String groupId, String id) {
		Map map = new HashMap();
		map.put("org_id",orgId);
		map.put("group_id",groupId);
		map.put("id",id);
		List<Map> list = sqlSessionTemplate.selectList("userDao.queryByParentOrgId",map);
		
		JSONArray array = new JSONArray();
		for(Map tmp : list){
			JSONObject obj = new JSONObject();
			obj.put("id", tmp.get("id") == null? "" : map.get("id") + "");
			obj.put("name", tmp.get("name"));
			obj.put("group_id", tmp.get("group_id"));
			obj.put("parent_org_id", tmp.get("parent_org_id"));
			obj.put("org_id",tmp.get("org_id"));
			obj.put("key",tmp.get("org_key"));
			obj.put("type", tmp.get("type"));
			array.add(obj);
		}
		return array;
	}

	@Override
	public List<Object> getGrouById(Integer id) {
		Map map = new HashMap();
		map.put("id",id);
		List<Map> list =sqlSessionTemplate.selectList("userDao.getGrouById",map);
		List<Object> result = new ArrayList();
		for(Map m :list){
			result.add(m.get("group_org_id")); 
		}
		return result;
	}

	@Override
	public TreeMap getgroup_Org(Integer id) {
		Map map = new HashMap();
		map.put("id",id);
		TreeMap<Object, List> treemap = new TreeMap<Object, List>();
		List<Map> list = sqlSessionTemplate.selectList("userDao.getgroup_Org",map);
		for(Map tmp:list){
			if(treemap.containsKey(tmp.get("group_id"))){
				treemap.get(tmp.get("group_id")).add(tmp.get("org_id"));
			}else{
				List tmplist = new ArrayList();
				tmplist.add(tmp.get("org_id"));
				treemap.put(tmp.get("group_id"), tmplist);
			}
		}
		return treemap;
		
	}

	@Override
	public User getUserById(Integer userId) {
		Map map = new HashMap();
		map.put("userId",userId);
		return sqlSessionTemplate.selectOne("userDao.getUserByid",map);
	}

	@Override
	public JSONArray queryByUserGroup(Integer id) {
		Map param = new HashMap();
		param.put("userId",id);
		List<Map> list = sqlSessionTemplate.selectList("userDao.queryByUserGroup",param);
		JSONArray result = new JSONArray();
		for (Map map :list) {
			JSONObject obj = new JSONObject();
			obj.put("id", map.get("id"));
			obj.put("name", map.get("group_name"));
			result.add(obj);
		}
		return result;
	}

	@Override
	public Map getInfo(Integer id) {
		Map param = new HashMap();
		param.put("userId",id);
		return sqlSessionTemplate.selectOne("userDao.getInfo",param);
	}

	@Override
	public List<Map> getRolesInfoByUserId(Integer id) {
		Map param = new HashMap();
		param.put("userId",id);
		return sqlSessionTemplate.selectList("userDao.getRolesInfoByUserId",param);
	}

	@Override
	public Org getOrgByOrgId(Integer orgId) {
		Map param = new HashMap();
		param.put("orgId",orgId);
		return sqlSessionTemplate.selectOne("userDao.getOrgByOrgId",param);
	}

	@Override
	public Integer modifyPassword(Integer id, String newPassword) {
		Map param = new HashMap();
		param.put("id",id);
		param.put("password",newPassword);
		return sqlSessionTemplate.update("userDao.updateUser",param);
	}
}
