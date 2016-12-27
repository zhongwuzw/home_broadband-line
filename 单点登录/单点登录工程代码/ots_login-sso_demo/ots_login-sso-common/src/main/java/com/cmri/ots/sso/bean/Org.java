package com.cmri.ots.sso.bean;

import java.util.Set;


public class Org {
	/**  <p>.</p> */
	private static final long serialVersionUID = 1000000000001000L;
	private Integer id;
	private String name;
	private String desction;
//	private Org parent;
//	private Set<User> users;
	private String whole_org_id;
	private String whole_org_name;
	private String org_key;
	private Integer rightgroupid;
	private String icon;
	private String api_key;
	private String code;
	private Integer list_order;
	private Integer is_prj;
	private Integer parent_id;
	
	
	
	public Integer getParent_id() {
		return parent_id;
	}

	public void setParent_id(Integer parent_id) {
		this.parent_id = parent_id;
	}

	public Integer getRightgroupid() {
		return rightgroupid;
	}

	public void setRightgroupid(Integer rightgroupid) {
		this.rightgroupid = rightgroupid;
	}

	public String getIcon() {
		return icon;
	}

	public void setIcon(String icon) {
		this.icon = icon;
	}

	public String getApi_key() {
		return api_key;
	}

	public void setApi_key(String api_key) {
		this.api_key = api_key;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public Integer getList_order() {
		return list_order;
	}

	public void setList_order(Integer list_order) {
		this.list_order = list_order;
	}

	public Integer getIs_prj() {
		return is_prj;
	}

	public void setIs_prj(Integer is_prj) {
		this.is_prj = is_prj;
	}

	public String getName() {
		return name;
	}

	public String getDesction() {
		return desction;
	}

	public void setDesction(String desction) {
		this.desction = desction;
	}

	public String getOrg_key() {
		return org_key;
	}

	public void setOrg_key(String org_key) {
		this.org_key = org_key;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getWhole_org_id() {
		return whole_org_id;
	}

	public void setWhole_org_id(String whole_org_id) {
		this.whole_org_id = whole_org_id;
	}

	public String getWhole_org_name() {
		return whole_org_name;
	}

	public void setWhole_org_name(String whole_org_name) {
		this.whole_org_name = whole_org_name;
	}
	
}
