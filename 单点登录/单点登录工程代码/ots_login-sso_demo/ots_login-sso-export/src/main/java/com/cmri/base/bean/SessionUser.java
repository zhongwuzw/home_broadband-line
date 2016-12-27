package com.cmri.base.bean;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class SessionUser  implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1873557037798604339L;
	private Integer id;
	private String username;
	private String password;
	private String personName;
	@Deprecated
	private Integer orgId;
	private String orgKey;
	private String wholeOrgId;
	private String orgName;
	private String roleId;
	private String roleName;
	private String sessionId;
	private Map resource;
	//所有group
	private List<Object> groups;
	//默认group
	private String group_id;
	private TreeMap orgs;
	
	
	public String getGroup_id() {
		return group_id;
	}
	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}
	public List<Object> getGroups() {
		return groups;
	}
	public void setGroups(List<Object> groups) {
		this.groups = groups;
	}
	public TreeMap getOrgs() {
		return orgs;
	}
	public void setOrgs(TreeMap orgs) {
		this.orgs = orgs;
	}
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	/**
	 * @return the wholeOrgId
	 */
	public String getWholeOrgId() {
		return wholeOrgId;
	}
	/**
	 * @param wholeOrgId the wholeOrgId to set
	 */
	public void setWholeOrgId(String wholeOrgId) {
		this.wholeOrgId = wholeOrgId;
	}
	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}
	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}
	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}
	/**
	 * @return the personName
	 */
	public String getPersonName() {
		return personName;
	}
	/**
	 * @param personName the personName to set
	 */
	public void setPersonName(String personName) {
		this.personName = personName;
	}
	/**
	 * @return the orgId
	 */
	@Deprecated
	public Integer getOrgId() {
		return orgId;
	}
	/**
	 * @param orgId the orgId to set
	 */
	@Deprecated
	public void setOrgId(Integer orgId) {
		this.orgId = orgId;
	}
	/**
	 * @return the orgName
	 */
	public String getOrgName() {
		return orgName;
	}
	/**
	 * @param orgName the orgName to set
	 */
	public void setOrgName(String orgName) {
		this.orgName = orgName;
	}
	
	
	
	public String getRoleId() {
		return roleId;
	}
	public void setRoleId(String roleId) {
		this.roleId = roleId;
	}
	public String getRoleName() {
		return roleName;
	}
	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getOrgKey() {
		return orgKey;
	}
	public void setOrgKey(String orgKey) {
		this.orgKey = orgKey;
	}
	public Map getResource() {
		return resource;
	}
	public void setResource(Map resource) {
		this.resource = resource;
	}
	
	
}
