package com.cmri.ots.sso.dao;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.json.JSONArray;

import com.cmri.base.bean.SessionUser;
import com.cmri.ots.sso.bean.Org;
import com.cmri.ots.sso.bean.User;
import com.cmri.ots.sso.bean.UserAgent;
import com.cmri.ots.sso.bean.UserStatues;

public interface AuthDao {

	UserAgent getByMessage(UserAgent agent);

	void saveuserAgent(UserAgent userAgent);

	void getorgbyuser(User userco);

	User doLogin(User userco);

	void updateUser(User user);

	List getroles(User userco);

	Map getResource(String roleId);

	List<Org> searchUserTeam(Integer id);

	List getUserGroup(SessionUser sessionUser);

	void saveUserstatues(UserStatues usersta);

	User getUserByApiKey(String apikey);

	JSONArray queryByUserId(Integer userId);

	JSONArray queryByParentOrgId(String orgId, String groupId, String id);

	List<Object> getGrouById(Integer id);

	TreeMap getgroup_Org(Integer id);

	User getUserById(Integer userId);

	JSONArray queryByUserGroup(Integer id);

	Map getInfo(Integer id);

	List<Map> getRolesInfoByUserId(Integer id);

	Org getOrgByOrgId(Integer orgId);

	Integer modifyPassword(Integer id, String encodePassword);
	
}
