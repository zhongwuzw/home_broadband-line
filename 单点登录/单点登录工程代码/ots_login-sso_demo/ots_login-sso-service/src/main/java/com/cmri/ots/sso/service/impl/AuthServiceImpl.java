package com.cmri.ots.sso.service.impl;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.util.JSONUtils;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;

import com.cmri.base.bean.SessionUser;
import com.cmri.common.redis.JedisCilent;
import com.cmri.ots.sso.AuthService;
import com.cmri.ots.sso.bean.Org;
import com.cmri.ots.sso.bean.User;
import com.cmri.ots.sso.bean.UserAgent;
import com.cmri.ots.sso.bean.UserStatues;
import com.cmri.ots.sso.dao.AuthDao;
import com.cmri.ots.sso.util.SSOUtil;

@Service
public class AuthServiceImpl implements AuthService {

	@Autowired
	private AuthDao authDaoImpl;

	
	public User doLogin(User userco) {
		return authDaoImpl.doLogin(userco);
	}

	
	public UserAgent getByMessage(String userAngetMessage) {
		UserAgent agent = new UserAgent();
		agent.setMessage(userAngetMessage);
		agent = authDaoImpl.getByMessage(agent);
		return agent;
	}

	
	public void saveuserAgent(UserAgent userAgent) {
		authDaoImpl.saveuserAgent(userAgent);
	}

	
	public void updateUser(User user) {
		authDaoImpl.updateUser(user);
	}

	
	public void setRoleId(SessionUser sessionUser, User userco) {
		List<Map> roles = authDaoImpl.getroles(userco);
		String ids = "";
		for (Map map : roles) {
			if (map.get("role_id") != null) {
				ids += map.get("role_id") + ",";
			}
		}
		if(ids.length()>0){
			sessionUser.setRoleId(ids.substring(0, ids.length() - 1));
		}else{
			sessionUser.setRoleId(null);
		}
		
	}

	public void setResource(SessionUser sessionUser) {
		sessionUser
				.setResource(authDaoImpl.getResource(sessionUser.getRoleId()));
	}

	public List<Org> searchUserTeam(Integer id) {
		return authDaoImpl.searchUserTeam(id);
	}

	public String getGroupId(SessionUser sessionUser) {
		StringBuffer sb = new StringBuffer("");
		List<Map> groups = authDaoImpl.getUserGroup(sessionUser);
		for (Map group : groups) {
			if (group != null && group.get("group_org_id") != null) {
				sb.append(group.get("group_org_id") + ",");
			}
		}
		if (!sb.toString().equals("")) {
			return (sb.substring(0, sb.length() - 1));
		}
		return null;
	}

	
	public void saveUserstatues(UserStatues usersta) {
		authDaoImpl.saveUserstatues(usersta);
	}

	
	public Integer queryTUserByApiKey(String apikey) {
		User user = authDaoImpl.getUserByApiKey(apikey);
		if (user != null) {
			return user.getId();
		}
		return null;
	}

	
	public JSONObject queryTreeByUserId(Integer userId) {

		JSONArray list = authDaoImpl.queryByUserId(userId);
		JSONObject orgs = new JSONObject();
		queryTree(list, orgs);

		JSONArray orgsArr = new JSONArray();
		Iterator<String> it = orgs.keys();
		while (it.hasNext()) {
			orgsArr.add(it.next());
		}
		JSONObject obj = new JSONObject();
		obj.put("orgs", orgsArr);
		obj.put("info", list);
		return obj;
	}

	public JSONArray queryTree(JSONArray list, JSONObject orgs) {
		JSONArray arr = new JSONArray();
		for (int i = 0; i < list.size(); i++) {
			JSONObject obj = list.optJSONObject(i);
			String id = obj.optString("id");
			String orgId = obj.optString("org_id");
			String type = obj.optString("type");
			String groupId = obj.optString("group_id");
			if (type.equals("1")) {
				orgs.put(orgId, orgId);
			}
			JSONArray groupArr = authDaoImpl.queryByParentOrgId(orgId, groupId,
					id);
			obj.put("suborgs", queryTree(groupArr, orgs));
			obj.remove("id");
			obj.remove("group_id");
			obj.remove("parent_org_id");
			arr.add(obj);
		}
		return arr;
	}

	@Override
	public String publicLogin(String username, String pwd, String referer,
			String userAgentstr, String RemoteAddr) {
		JSONObject loginResult = new JSONObject();
		if (referer == null || userAgentstr == null || RemoteAddr == null) {
			loginResult.put("status", "1");
			loginResult.put("error", "请上传完整参数");
			return loginResult.toString();
		}
		try {
			User userco = new User();
			userco.setUsername(java.net.URLDecoder.decode(username, "UTF-8"));
			userco.setPassword(java.net.URLDecoder.decode(pwd, "UTF-8"));
			User user = this.doLogin(userco);

			if (user != null) {
				user.setLastLoginHost(referer);
				// ////////////////////����useragent
				try {
					Integer count = user.getCount();
					if (count == null) {
						count = 0;
					}
					user.setCount(++count);
					user.setLastIp(RemoteAddr);
					user.setLastTime(new Date());
					UserAgent userAgent = this.getByMessage(userAgentstr);
					if (userAgent == null) {
						userAgent = new UserAgent();
						userAgent.setMessage(userAgentstr);
						this.saveuserAgent(userAgent);
					}
					user.setUser_agent_id(userAgent.getId());
					this.updateUser(user);
				} catch (Exception e) {
					e.printStackTrace();
				}
				// //////////////////////

				SessionUser sessionUser = new SessionUser();
				sessionUser.setUsername(user.getUsername());
				sessionUser.setPassword(user.getPassword());
				sessionUser.setId(user.getId());
				sessionUser.setPersonName(user.getName());
				//查询默认分组（非全部分组），并查询该分组的默认
				List grouplist = authDaoImpl.getGrouById(user.getId());
				if(grouplist!=null&&grouplist.size()>0){
					sessionUser.setGroup_id(grouplist.get(0).toString());
				}
				sessionUser.setGroups(grouplist);
				sessionUser.setOrgs(authDaoImpl.getgroup_Org(user.getId()));
				this.setRoleId(sessionUser, user);
				System.out.println((sessionUser.getRoleId() + "----"));
				// ��ȡȨ��resource
				this.setResource(sessionUser);

				List<Org> list = this.searchUserTeam(sessionUser.getId());

				if (list.size() > 0) {
					sessionUser.setOrgId(list.get(0).getId());
					sessionUser.setWholeOrgId(list.get(0).getWhole_org_id());
					sessionUser.setOrgName(list.get(0).getName());
					sessionUser.setOrgKey(list.get(0).getOrg_key());

				}

				String uuid = UUID.randomUUID().toString();
				JedisCilent.insertObj(uuid, 600 * 30, sessionUser);
				String groupId = this.getGroupId(sessionUser);
				if (groupId != null) {
					JedisCilent.insertObj(uuid + "-group", 600 * 30, groupId);
				}

				// logger.debug("getSessionId:" + sessionUser.getSessionId());
				loginResult.put("key", uuid);
				loginResult.put("name", user.getName());
				loginResult.put("apiKey", user.getApi_key());

				statLoginStatus(user, "SUCCESS");
				// chartService.initData(sessionUser);
			} else {
				loginResult.put("status", "1");
				statLoginStatus(user, "PARAMS ERROR");
			}
			return loginResult.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private void statLoginStatus(User user, String message) {
		try {
			UserStatues usersta = new UserStatues();
			usersta.setUser_id(user.getId());
			usersta.setName(user.getName());
			usersta.setCreate_id(user.getCreater());
			usersta.setCreate_time(user.getCreateTime());
			usersta.setUpdate_id(user.getUpdater());
			usersta.setUpdate_time(user.getUpdateTime());
			usersta.setUsername(user.getUsername());
			usersta.setLast_ip(user.getLastIp());
			usersta.setLast_time(user.getLastTime());
			usersta.setUser_agent_id(user.getUser_agent_id());
			Integer count = user.getCount();
			if (count == null) {
				count = 0;
			}
			user.setCount(++count);
			usersta.setDescription(message);
			usersta.setLast_login_host(user.getLastLoginHost());
			user.setLastTime(new Date());
			this.saveUserstatues(usersta);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String validLogin(String key) {
		JSONObject jsonObject = new JSONObject();
		SessionUser user = returnSessionUser(key);
		try {
			if (user != null) {
				jsonObject.put("status", "1");
				jsonObject.put("username", user.getUsername());
				jsonObject.put("personName", user.getPersonName());
				jsonObject.put("orgName", user.getOrgName());
				jsonObject.put("roleId", user.getRoleId());
				jsonObject.put("orgid", user.getOrgId());
				Map map = user.getResource();
				List list = new ArrayList();
				if (map != null) {
					for (Object keys : map.keySet()) {
						list.add(keys);
					}
				}
				jsonObject.put("loginresource", JSONArray.fromObject(list));
			} else {
				jsonObject.put("status", "2");
			}
		} catch (Exception e) {
			e.printStackTrace();
			jsonObject.put("status", "2");
		}
		return jsonObject.toString();
	}
	
	@Override
	public String getOrgInfo(String key) {
		int status = 200;
		String message = "SUCCESS";
		SessionUser user = returnSessionUser(key);
		Integer userId = null;
		if(user != null && user.getId() != null){
			userId = user.getId();
		}
		JSONObject detail = null;
		if(userId != null){
			detail = this.queryTreeByUserId(userId);
		}else{
			status = 404;
			message = "user is null";
		}
		
		JSONObject obj = new JSONObject();
		obj.put("status", status);
		obj.put("message", message);
		obj.put("detail", detail);
		return obj.toString();
	}
	@Override
	public String getUserInfo(String key) {
		int status = 200;
		String message = "SUCCESS";
		SessionUser user = returnSessionUser(key);
		Integer userId = null;
		if(user != null && user.getId() != null){
			userId = user.getId();
		}
		JSONObject detail = null;
		if(userId != null){
			try {
				detail = this.getUserdetail(userId);
			} catch (Exception e) {
				status = 404;
				message = "user is null";
				e.printStackTrace();
			}
		}else{
			status = 404;
			message = "user is null";
		}
		
		JSONObject obj = new JSONObject();
		obj.put("status", status);
		obj.put("message", message);
		obj.put("detail", detail);
		return obj.toString();
	}
	
	private JSONObject getUserdetail(Integer userId) {
		//user\groups\orgs
		JSONObject obj = new JSONObject();
		User user = authDaoImpl.getUserById(userId);
		TreeMap<Object, List> treemap = authDaoImpl.getgroup_Org(userId);
		JSONArray groups =new JSONArray();
		JSONArray orgs = new JSONArray();
		for(Object group :treemap.keySet()){
			groups.add(group);
			for(Object org: treemap.get(group)){
				orgs.add(org);
			}
		}
		JSONObject userinfo = new JSONObject();
		userinfo.put("nickname", user.getName());
		userinfo.put("username", user.getUsername());
		userinfo.put("apiKey", user.getApi_key());
		obj.put("userinfo", userinfo);
		obj.put("orgs", orgs);
		obj.put("groups", groups);
		return obj;
	}
	@Override
	public String getInfo(String key) {
		SessionUser user =returnSessionUser(key);
		
		//获取用户基本信息对象
		Map psn = authDaoImpl.getInfo(user.getId());
		
		//获取当前用户拥有的角色
		List<Map> roleList = authDaoImpl.getRolesInfoByUserId(user.getId());
		
		StringBuffer roleStr = new StringBuffer();
		for(int i=0; i<roleList.size(); i++){
			roleStr.append(roleList.get(i).get("name"));
			if(i < roleList.size()-1){
				roleStr.append(",");
			}
		}
		
		//获取当前用户所在的部门
		String dept = "";
		if(user.getOrgId() != null){
			Org org = authDaoImpl.getOrgByOrgId(user.getOrgId());
			if(org != null){
				if(org.getWhole_org_id().length() <= 4){
					dept = org.getName();
				} else{
					if(org.getParent_id()!=null){
						dept = authDaoImpl.getOrgByOrgId(org.getParent_id()).getName();
					}
				}
			} else{
				dept = "";
			}
		}
		
		//构造标准格式的Map数据
		Map<String, Object> pMap = new HashMap<String, Object>();
		
		pMap.put("id", user.getId());
		pMap.put("name", user.getPersonName());
		pMap.put("level", psn==null?"":psn.get("level"));
		pMap.put("phone", psn==null?"":psn.get("phone"));
		pMap.put("leader", psn==null?"":psn.get("leader"));
		pMap.put("mailbox", psn==null?"":psn.get("mailbox"));
		pMap.put("position", roleStr.toString());
		pMap.put("department", dept);
		
		String jsonData = JSONObject.fromObject(pMap).toString();
		return jsonData;
	}

	/**
	 * @param uuid
	 * @return ���key��ȡuser
	 */
	public SessionUser returnSessionUser(String uuid) {//--����SessionUser
		if(uuid != null && !"".equals(uuid)){
			Object obj = JedisCilent.getObj(uuid);
			if(obj !=null){
				SessionUser ruser =	(SessionUser)obj;
				return ruser;
			}
		}
		return null;
	}
	
	@Override
	public String queryByUserGroup(String key) {
		SessionUser user =returnSessionUser(key);
		String message = "SUCCESS";
		JSONObject obj = new JSONObject();
		if(user != null){
			JSONArray list = authDaoImpl.queryByUserGroup(user.getId());
			obj.put("dataItems", list);
		}else{
			message = "user is null";
		}
		obj.put("message", message);
		return obj.toString();
	}


	public static void main(String[] args) {
		AuthService s = new AuthServiceImpl();
		System.out.println(s.getInfo("00d51323-b3c6-43da-af32-0648171cf46"));
	}


	@Override
	public String modifyPassword(String jsonParam) throws UnsupportedEncodingException {
		JSONObject jsonObject = JSONObject.fromObject(jsonParam);
		String username = jsonObject.optString("username");
		String oldPassword = jsonObject.optString("oldPassword");
		String newPassword = jsonObject.optString("newPassword");
		User userco = new User();
		userco.setUsername(java.net.URLDecoder.decode(username, "UTF-8"));
		userco.setPassword(SSOUtil.encodePassword(java.net.URLDecoder.decode(oldPassword, "UTF-8")));
		User user = this.doLogin(userco);
		JSONObject jsonResult = new JSONObject();
		if (user != null) {
			Integer count = authDaoImpl
					.modifyPassword(user.getId(), SSOUtil.encodePassword(java.net.URLDecoder.decode(newPassword, "UTF-8")));
			if (count == 1) {
				// 修改成功
				jsonResult.put("status", 1);
			} else {
				// 修改失败
				jsonResult.put("status", 0);
			}
		} else {
			// 旧密码错误
			jsonResult.put("status", 2);
		}
		return jsonResult.toString();
	}


	@Override
	public void changeOrg(String jsonParam, String key) {
		SessionUser sessionUser = returnSessionUser(key);
		JSONObject jsonObject = JSONObject.fromObject(jsonParam);
		String groupId = jsonObject.getString("orgId");
		//此处获取的orgId为groupId
		try {
			sessionUser.setGroup_id(groupId);
			//org信息不变，当需要用到org信息时 通过orgs.get(groupId)获取
			try {
				JedisCilent.insertObj(key, 600 * 30, sessionUser);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	@Override
	public void logout(String key) {
		JedisCilent.delObj(key);
	}

}
