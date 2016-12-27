package com.cmri.ots.sso.bean;

import java.util.Date;
import java.util.Set;



public class User {
	private Integer id ;
	private Integer creater;
	private Integer updater;
	private Date createTime;
	private Date updateTime;
	private String username;
	private String password;
	private String name;
	private Set<Integer> roles;
	private Set<Integer> orgs;
	private Date lastTime;
	private String lastIp;
	private Integer count;
	private String api_key;
	private String lastLoginHost;
	private Integer user_agent_id;
	
	
	
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public Integer getUser_agent_id() {
		return user_agent_id;
	}
	public void setUser_agent_id(Integer user_agent_id) {
		this.user_agent_id = user_agent_id;
	}
	public Integer getCreater() {
		return creater;
	}
	public void setCreater(Integer creater) {
		this.creater = creater;
	}
	public Integer getUpdater() {
		return updater;
	}
	public void setUpdater(Integer updater) {
		this.updater = updater;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Set<Integer> getRoles() {
		return roles;
	}
	public void setRoles(Set<Integer> roles) {
		this.roles = roles;
	}
	public Set<Integer> getOrgs() {
		return orgs;
	}
	public void setOrgs(Set<Integer> orgs) {
		this.orgs = orgs;
	}
	public String getLastIp() {
		return lastIp;
	}
	public void setLastIp(String lastIp) {
		this.lastIp = lastIp;
	}
	public Integer getCount() {
		return count;
	}
	public void setCount(Integer count) {
		this.count = count;
	}
	
	public String getApi_key() {
		return api_key;
	}
	public void setApi_key(String api_key) {
		this.api_key = api_key;
	}
	public String getLastLoginHost() {
		return lastLoginHost;
	}
	public void setLastLoginHost(String lastLoginHost) {
		this.lastLoginHost = lastLoginHost;
	}
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
	public Date getUpdateTime() {
		return updateTime;
	}
	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}
	public Date getLastTime() {
		return lastTime;
	}
	public void setLastTime(Date lastTime) {
		this.lastTime = lastTime;
	}
	
}
