package com.cmri.ots.sso.biz;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.MethodBeforeAdvice;


public class DataSourceAdvice implements MethodBeforeAdvice {
	private Logger logger = LoggerFactory.getLogger(DataSourceAdvice.class);
    public void before(Method method, Object[] args, Object target) throws Throwable {
    	logger.debug("adviceBefore----");
    	String dataSource = "";
        if(method.getName().startsWith("add") 
            || method.getName().startsWith("create")
            || method.getName().startsWith("insert")
            || method.getName().startsWith("save")
            || method.getName().startsWith("edit")
            || method.getName().startsWith("update")
            || method.getName().startsWith("delete")
            || method.getName().startsWith("remove")){
        	dataSource = "slave";
            DataSourceSwitcher.setSlave();
        } else  {
        	dataSource = "master";
            DataSourceSwitcher.setMaster();
        }
        logger.debug("---advice class:" + target.getClass().getName() +
        		",method:" + method.getName() + ",chenge datasource:" + dataSource);
    }

}

