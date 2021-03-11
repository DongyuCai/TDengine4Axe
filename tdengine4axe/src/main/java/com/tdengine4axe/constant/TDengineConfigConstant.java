package com.tdengine4axe.constant;

public class TDengineConfigConstant {
	public static final String CONFIG_FILE = "tdengine.properties";
	
	// #持久层配置
	public static final String DATASOURCE = "datasource";
	public static final String DRIVER = "driver";
	public static final String URL = "url";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	// 这个部分是jdbc配置的子项，需要JDBC_DATASOURCE+dataSourceName+以下名称配置{
	public static final String AUTO_CREATE_TABLE = "auto_create_table";
	public static final String SHOW_SQL = "show_sql";
	// }
}
