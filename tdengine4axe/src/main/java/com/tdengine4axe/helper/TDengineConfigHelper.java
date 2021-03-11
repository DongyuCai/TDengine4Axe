package com.tdengine4axe.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.axe.interface_.base.Helper;
import org.axe.interface_.mvc.AfterConfigLoaded;
import org.axe.util.PropsUtil;

import com.tdengine4axe.constant.TDengineConfigConstant;

public final class TDengineConfigHelper implements Helper{

	private static Properties CONFIG_PROPS;
	private static final List<AfterConfigLoaded> AFTER_CONFIG_LOADED_LIST = new ArrayList<>();
	
    public static void addAfterConfigLoadedCallback(AfterConfigLoaded callback){
    	synchronized (AFTER_CONFIG_LOADED_LIST) {
    		AFTER_CONFIG_LOADED_LIST.add(callback);
		}
    }
    
    public static Properties getCONFIG_PROPS() {
		return CONFIG_PROPS;
	}
    
    @Override
    public synchronized void init() throws Exception{
    	if(CONFIG_PROPS == null){
    		CONFIG_PROPS = PropsUtil.loadProps(TDengineConfigConstant.CONFIG_FILE);
    	}
    	
    	//加载完配置后，执行
    	for(AfterConfigLoaded acl:AFTER_CONFIG_LOADED_LIST){
    		acl.doSomething(CONFIG_PROPS);
    	}
    }
	
	/**
     * 获取 JDBC 驱动
     */
    public static String getDriver() {
        return PropsUtil.getString(CONFIG_PROPS, TDengineConfigConstant.DRIVER, null);
    }

    /**
     * 获取 JDBC URL
     */
    public static String getUrl() {
        return PropsUtil.getString(CONFIG_PROPS, TDengineConfigConstant.URL, null);
    }

    /**
     * 获取 JDBC 用户名
     */
    public static String getUsername() {
        return PropsUtil.getString(CONFIG_PROPS, TDengineConfigConstant.USERNAME, null);
    }
    
    /**
     * 获取 JDBC 密码
     */
    public static String getPassword() {
        return PropsUtil.getString(CONFIG_PROPS, TDengineConfigConstant.PASSWORD, null);
    }
    
    /**
     * 获取 JDBC 数据源
     * 多个值用“,”逗号分隔
     */
    public static String getDatasource() {
    	//默认使用axe提供的dbcp数据源
    	return PropsUtil.getString(CONFIG_PROPS, TDengineConfigConstant.DATASOURCE, null);
    }
    
	@Override
	public void onStartUp() throws Exception {}
}
