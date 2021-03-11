/**
 * MIT License
 * 
 * Copyright (c) 2021 CaiDongyu
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.tdengine4axe.helper;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.axe.bean.pojo.EntityFieldMethod;
import org.axe.interface_.base.Helper;
import org.axe.util.CastUtil;
import org.axe.util.LogUtil;
import org.axe.util.ReflectionUtil;
import org.axe.util.StringUtil;

import com.tdengine4axe.bean.SqlPackage;
import com.tdengine4axe.interface_.BaseDataSource;
import com.tdengine4axe.util.CommonSqlUtil;
import com.tdengine4axe.util.TDengineUtil;


/**
 * 数据库 助手类
 * @author CaiDongyu.
 */
public final class DataBaseHelper implements Helper{
//    private static final Logger LOGGER = LoggerFactory.getLogger(DataBaseHelper.class);
    
    private static ThreadLocal<HashMap<String,Connection>> CONNECTION_HOLDER;
    
    @Override
    public void init() throws Exception{
    	synchronized (this) {
    		//#数据库连接池
            CONNECTION_HOLDER = new ThreadLocal<>();
            //#SQL程咬金
            /*Set<Class<?>> sqlCyjClassSet = ClassHelper.getClassSetBySuper(SqlCyj.class);
            if(CollectionUtil.isNotEmpty(sqlCyjClassSet)){
            	if(sqlCyjClassSet.size() > 1){
            		throw new RuntimeException("find "+sqlCyjClassSet.size()+" SqlCyj");
            	}
            	for(Class<?> sqlCyjClass:sqlCyjClassSet){
            		sqlCyj = ReflectionUtil.newInstance(sqlCyjClass);
            		break;
            	}
            }*/
		}
    }

    /**
     * 获取数据库并链接
     * @throws SQLException 
     */
    public static Connection getConnection(String dataSourceName) throws SQLException {
        HashMap<String,Connection> connMap = CONNECTION_HOLDER.get();
		//LogUtil.log("test>获取链接");
        if (connMap == null || !connMap.containsKey(dataSourceName)) {
        	//如果此次操作连接不存在，则需要获取连接
        	//如果前面打开事物了，则这里会存在连接，不会再进来
            try {
            	Map<String, BaseDataSource> dsMap = DataSourceHelper.getDataSourceAll();
        		if(dsMap.containsKey(dataSourceName)){
        			Connection connection = dsMap.get(dataSourceName).getConnection();
        			if(connMap == null){
        				connMap = new HashMap<>();
        				CONNECTION_HOLDER.set(connMap);
        			}
        			connMap.put(dataSourceName, connection);
        			//LogUtil.log("test>新建链接");
        		}
            } catch (SQLException e) {
//                LOGGER.error("get connection failure", e);
                throw e;
            }
            
        }
        if(connMap != null && connMap.containsKey(dataSourceName)){
			//LogUtil.log("test>获取成功");
        	return connMap.get(dataSourceName);
        }else{
        	throw new RuntimeException("connot find connection of dataSource:"+dataSourceName);
        }
    }

    /**
     * 关闭链接
     * @throws SQLException 
     */
    public static void closeConnection(String dataSourceName) throws SQLException {
		//LogUtil.log("test>关闭链接");
    	HashMap<String, Connection> connMap = CONNECTION_HOLDER.get();
    	try {
    		do{
    			if(connMap == null) break;
    			Connection con = connMap.get(dataSourceName);
    			if(con == null) break;
				if(!con.isClosed()){
					DataSourceHelper.getDataSourceAll().get(dataSourceName).closeConnection(con);
					//LogUtil.log("test>关闭成功");
//					LOGGER.debug("release connection of dataSource["+dataSourceName+"]:"+con);
				}
    		}while(false);
		} catch (SQLException e) {
//			LOGGER.error("release connection of dataSource["+dataSourceName+"] failure", e);
			throw e;
		}  finally {
			if(connMap != null){
				boolean isAllConClosed = true;
				for(Connection con:connMap.values()){
					if(!con.isClosed()){
						isAllConClosed = false;
						break;
					}
				}
				if(isAllConClosed){
					//LogUtil.log("test>全部链接已关闭");
					CONNECTION_HOLDER.remove();
//					LOGGER.debug("clean CONNECTION_HOLDER");
				}
			}
        }
    }
    
    /**
     * sql前去数据库路上的终点站，出了这个方法，就是奈何桥了。
     * TDengine不支持预处理语句，只能手动替换
     */
    private static String getPrepareStatement(String dataSourceName,Connection conn, String sql, Object[] params) throws SQLException{
		// #空格格式化，去掉首位空格，规范中间的空格{
//		sql = sql.trim();
		while (sql.contains("  ")) {
			sql = sql.replaceAll("  ", " ");
		}
		sql = sql.trim();
		// }
    	
		//将取值符号，统统换成预处理样式，参数数量也要调整到对应位置
    	SqlPackage sp = TDengineUtil.convertGetFlag(sql, params);
    	sql = sp.getSql();
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
    	for(Object param:sp.getParams()){
    		if(Date.class.isAssignableFrom(param.getClass())){
    			//时间戳，需要格式化
    			sql = sql.replaceFirst("\\?", "\""+sdf.format((Date)param)+"\"");
    			continue;
    		}else if(String.class.isAssignableFrom(param.getClass())){
    			sql = sql.replaceFirst("\\?", "\""+String.valueOf(param)+"\"");
    		}else{
    			sql = sql.replaceFirst("\\?", String.valueOf(param));
    		}
    	}
    	LogUtil.log(sql);
    	return sql;
    }
    
    public static <T> List<T> queryEntityList(final Class<T> entityClass, String sql, Object[] params) throws SQLException {
    	   String dataSourceName = TableHelper.getCachedTableSchema(entityClass).getDataSourceName();
    	   return queryEntityList(entityClass, sql, params, dataSourceName);
    }
    
    /**
     * 查询实体列表
     * @throws SQLException 
     */
	public static <T> List<T> queryEntityList(final Class<T> entityClass, String sql, Object[] params, String dataSourceName) throws SQLException {
        List<T> entityList = new ArrayList<>();
        ResultSet table = null;
        Statement statement = null;
        Connection conn = getConnection(dataSourceName);
        try {
        	sql = getPrepareStatement(dataSourceName, conn, sql, params);
        	statement = conn.createStatement();
        	table = statement.executeQuery(sql);
        	List<EntityFieldMethod> entityFieldMethodList = ReflectionUtil.getSetMethodList(entityClass);
			Set<String> transientField = new HashSet<>();

	    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        	while(table.next()){
				T entity = ReflectionUtil.newInstance(entityClass);
				for(EntityFieldMethod entityFieldMethod:entityFieldMethodList){
					Field field = entityFieldMethod.getField();
					String fieldName = field.getName();
					if(transientField.contains(fieldName)){
						continue;
					}
					Method method = entityFieldMethod.getMethod();
					String columnName = StringUtil.camelToUnderline(fieldName);
					try {
						String value = table.getString(columnName);
						if(Date.class.isAssignableFrom(field.getType())){
							try {
								value = String.valueOf(sdf.parse(value).getTime());
							} catch (Exception e) {}
						}
						Object setMethodArg = CastUtil.castType(value,field.getType());
						ReflectionUtil.invokeMethod(entity, method, setMethodArg);
					} catch (SQLException e) {
						if(e.getMessage().contains("cannot find")){
							//字段不存在情况再次尝试 原驼峰字段名，能否取到值
							try {
								Object setMethodArg = CastUtil.castType(table.getObject(fieldName),field.getType());
								ReflectionUtil.invokeMethod(entity, method, setMethodArg);
							} catch (SQLException e1) {
								if(e1.getMessage().contains("cannot find")){
									//字段不存在情况可以不处理
								}else{
									//其他异常抛出
									throw e1;
								}
							}
						}else{
							//其他异常抛出
							throw e;
						}
					}
				}
				entityList.add(entity);
			}
        } catch (SQLException e) {
//            LOGGER.error("query entity list failure", e);
            throw e;
        } finally {
        	if(table != null){
        		try {
        			table.close();
				} catch (Exception e2) {}
        	}
        	if(statement != null){
        		try {
        			statement.close();
				} catch (Exception e2) {}
        	}
            if(conn.getAutoCommit()){
            	closeConnection(dataSourceName);
            }
        }
        return entityList;
    }

	public static <T> T queryEntity(final Class<T> entityClass, String sql, Object[] params) throws SQLException {
        String dataSourceName = TableHelper.getCachedTableSchema(entityClass).getDataSourceName();
        return queryEntity(entityClass, sql, params, dataSourceName);
	}
	
    /**
     * 查询单个实体
     * @throws SQLException 
     */
    public static <T> T queryEntity(final Class<T> entityClass, String sql, Object[] params, String dataSourceName) throws SQLException {
        T entity = null;
        ResultSet table = null;
        Statement statement = null;
        Connection conn = getConnection(dataSourceName);
        try {
        	sql =  getPrepareStatement(dataSourceName,conn, sql, params);
        	statement = conn.createStatement();
        	table = statement.executeQuery(sql);
        	if(table.next()){
    			List<EntityFieldMethod> entityFieldMethodList = ReflectionUtil.getSetMethodList(entityClass);
    			entity = ReflectionUtil.newInstance(entityClass);
    			for(EntityFieldMethod entityFieldMethod:entityFieldMethodList){
					Field field = entityFieldMethod.getField();
					String fieldName = field.getName();
					Method method = entityFieldMethod.getMethod();
					String columnName = StringUtil.camelToUnderline(fieldName);
					try {
						Object setMethodArg = CastUtil.castType(table.getObject(columnName),field.getType());
						ReflectionUtil.invokeMethod(entity, method, setMethodArg);
					} catch (SQLException e) {
						if(e.getMessage().contains("Column '"+columnName+"' not found")){
							//字段不存在情况再次尝试 原驼峰字段名，能否取到值
							try {
								Object setMethodArg = CastUtil.castType(table.getObject(fieldName),field.getType());
								ReflectionUtil.invokeMethod(entity, method, setMethodArg);
							} catch (SQLException e1) {
								if(e1.getMessage().contains("Column '"+fieldName+"' not found")){
									//字段不存在情况可以不处理
								}else{
									//其他异常抛出
									throw e1;
								}
							}
						}else{
							//其他异常抛出
							throw e;
						}
					}
				}
			}
        } catch (SQLException e) {
//            LOGGER.error("query entity failure", e);
            throw e;
        } finally {
        	if(table != null){
        		try {
        			table.close();
				} catch (Exception e2) {}
        	}
        	if(statement != null){
        		try {
        			statement.close();
				} catch (Exception e2) {}
        	}
            if(conn.getAutoCommit()){
                closeConnection(dataSourceName);
            }
        }
        return entity;
    }

    public static List<Map<String, Object>> queryList(String sql, Object[] params) throws SQLException {
        String dataSourceName = DataSourceHelper.getDefaultDataSourceName();
    	return queryList(sql, params, dataSourceName);
    }
    
    /**
     * 执行List查询
     * @throws SQLException 
     */
    public static List<Map<String, Object>> queryList(String sql, Object[] params, String dataSourceName) throws SQLException {
        List<Map<String, Object>> result = new ArrayList<>();
        ResultSet table = null;
        Statement statement = null;
        Connection conn = getConnection(dataSourceName);
        try {
        	sql =  getPrepareStatement(dataSourceName,conn, sql, params);
        	statement = conn.createStatement();
        	table = statement.executeQuery(sql);
        	ResultSetMetaData rsmd = table.getMetaData();
			while(table.next()){
				Map<String, Object> row = new HashMap<>();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					row.put(rsmd.getColumnLabel(i), table.getObject(i));
	        	}
				result.add(row);
			}
        } catch (SQLException e) {
//            LOGGER.error("execute queryList failure", e);
            throw e;
        } finally {
        	if(table != null){
        		try {
        			table.close();
				} catch (Exception e2) {}
        	}
        	if(statement != null){
        		try {
        			statement.close();
				} catch (Exception e2) {}
        	}
            if(conn.getAutoCommit()){
                closeConnection(dataSourceName);
            }
        }
        return result;
    }
    
    /*public static Map<String, Object> queryMap(String sql, Object[] params) throws SQLException {
        String dataSourceName = TableHelper.getTableDataSourceName(null);
        return queryMap(sql, params, dataSourceName);
    }*/
    
    /**
     * 执行单条查询
     * @throws SQLException 
     */
    public static Map<String, Object> queryMap(String sql, Object[] params, String dataSourceName) throws SQLException {
        Map<String, Object> result = null;
        ResultSet table = null;
        Statement statement = null;
        Connection conn = getConnection(dataSourceName);
        try {
        	sql =  getPrepareStatement(dataSourceName,conn, sql, params);
        	statement = conn.createStatement();
        	table = statement.executeQuery(sql);
        	ResultSetMetaData rsmd = table.getMetaData();
        	if(table.next()){
        		result = new HashMap<>();
        		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        			result.put(rsmd.getColumnLabel(i), table.getObject(i));
	        	}
			}
        } catch (SQLException e) {
//            LOGGER.error("execute queryMap failure", e);
            throw e;
        } finally {
        	if(table != null){
        		try {
        			table.close();
				} catch (Exception e2) {}
        	}
        	if(statement != null){
        		try {
        			statement.close();
				} catch (Exception e2) {}
        	}
            if(conn.getAutoCommit()){
                closeConnection(dataSourceName);
            }
        }
        return result;
    }

	/*public static <T> T queryPrimitive(String sql, Object[] params) throws SQLException {
    	String dataSourceName = TableHelper.getTableDataSourceName(null);
    	return queryPrimitive(sql, params, dataSourceName);
	}*/

    /**
     * 执行返回结果是基本类型的查询
     * @throws SQLException 
     */
	@SuppressWarnings("unchecked")
	public static <T> T queryPrimitive(String sql, Object[] params, String dataSourceName) throws SQLException {
    	T result = null;
    	ResultSet table = null;
        Statement statement = null;
        Connection conn = getConnection(dataSourceName);
        try {
        	sql =  getPrepareStatement(dataSourceName,conn, sql, params);
        	statement = conn.createStatement();
        	table = statement.executeQuery(sql);
        	if(table.next()){
            	ResultSetMetaData rsmd = table.getMetaData();
            	if(rsmd.getColumnCount() > 0);
        			result = (T)table.getObject(1);
			}
        	
        } catch (SQLException e) {
//            LOGGER.error("execute queryPrimitive failure", e);
            throw e;
        } finally {
        	if(table != null){
        		try {
        			table.close();
				} catch (Exception e2) {}
        	}
        	if(statement != null){
        		try {
        			statement.close();
				} catch (Exception e2) {}
        	}
            if(conn.getAutoCommit()){
                closeConnection(dataSourceName);
            }
        }
        return result;
    }
    

	public static long countQuery(String sql, Object[] params) {
		String dataSourceName = DataSourceHelper.getDefaultDataSourceName();
		return countQuery(sql, params, dataSourceName);
	}
	
    /**
     * 执行返回结果是基本类型的查询
     */
    public static long countQuery(String sql, Object[] params,String dataSourceName) {
    	long result = 0;
        try {
        	//包装count(1)语句
        	sql = CommonSqlUtil.convertSqlCount(sql);
        	
        	//免转换，因为queryPrimitive会做
//        	SqlPackage sp = SqlHelper.convertGetFlag(sql, params);
            result = CastUtil.castLong(queryPrimitive(sql, params, dataSourceName));
        } catch (Exception e) {
//            LOGGER.error("execute countQuery failure", e);
            throw new RuntimeException(e);
        }
        return result;
    }
    
    public static int executeUpdate(String sql, Object[] params) throws SQLException {
        String dataSourceName = DataSourceHelper.getDefaultDataSourceName();
        return executeUpdate(sql, params, dataSourceName);
    }

    /**
     * 执行更新语句 （包括 update、delete）
     * @throws SQLException 
     */
    public static int executeUpdate(String sql, Object[] params, String dataSourceName) throws SQLException {
        int rows = 0;
        Statement statement = null;
        Connection conn = getConnection(dataSourceName);
        try {
        	sql =  getPrepareStatement(dataSourceName,conn, sql, params);
        	statement = conn.createStatement();
        	rows = statement.executeUpdate(sql);
        } catch (SQLException e) {
//            LOGGER.error("execute update failure", e);
            throw e;
        } finally {
        	if(statement != null){
        		try {
        			statement.close();
				} catch (Exception e2) {}
        	}
            if(conn.getAutoCommit()){
                closeConnection(dataSourceName);
            }
        }
        return rows;
    }
    
    public static int insertEntity(Object entity) throws SQLException {
    	String dataSourceName = TableHelper.getCachedTableSchema(entity).getDataSourceName();
    	return insertEntity(entity, dataSourceName);
    }
    
    /**
     * 插入实体
     * @throws SQLException 
     */
    public static int insertEntity(Object entity,String dataSourceName) throws SQLException {
    	if(entity == null)
    		throw new RuntimeException("insertEntity failure, insertEntity param is null!");
    	SqlPackage sp = TDengineUtil.getInsertSqlPackage(entity);
    	int rows = executeUpdate(sp.getSql(), sp.getParams(), dataSourceName);
    	if(rows <= 0){
    		throw new SQLException("insertEntity failure, effected rows is 0!");
    	}
    	return rows;
    }
    
	public static <T> T getEntity(T entity) throws SQLException{
    	String dataSourceName = TableHelper.getCachedTableSchema(entity).getDataSourceName();
		return getEntity(entity, dataSourceName);
	}
    
    @SuppressWarnings("unchecked")
	public static <T> T getEntity(T entity,String dataSourceName) throws SQLException{
    	if(entity == null)
    		throw new RuntimeException("getEntity failure, getEntity param is null!");
        SqlPackage sp = CommonSqlUtil.getSelectByIdSqlPackage(entity);
        entity = (T)queryEntity(entity.getClass(), sp.getSql(), sp.getParams(), dataSourceName);
    	return (T)entity;
    }

	@Override
	public void onStartUp() throws Exception {}
    
}
