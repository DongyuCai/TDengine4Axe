/**
 * MIT License
 * 
 * Copyright (c) 2017 CaiDongyu
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
package com.tdengine4axe.proxy;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.axe.annotation.aop.Aspect;
import org.axe.interface_.proxy.Proxy;
import org.axe.proxy.ProxyChain;
import org.axe.util.CastUtil;
import org.axe.util.CollectionUtil;
import org.axe.util.ReflectionUtil;
import org.axe.util.StringUtil;

import com.tdengine4axe.annotation.Dao;
import com.tdengine4axe.annotation.ResultProxy;
import com.tdengine4axe.annotation.Sql;
import com.tdengine4axe.bean.Page;
import com.tdengine4axe.bean.PageConfig;
import com.tdengine4axe.bean.SqlPackage;
import com.tdengine4axe.bean.TableSchema;
import com.tdengine4axe.helper.DataBaseHelper;
import com.tdengine4axe.helper.DataSourceHelper;
import com.tdengine4axe.interface_.BaseRepository;
import com.tdengine4axe.interface_.SqlResultProxy;
import com.tdengine4axe.util.CommonSqlUtil;
import com.tdengine4axe.util.TDengineUtil;

/**
 * Dao?????? ???????????? @Dao???????????????
 * 
 * @author CaiDongyu on 2016/4/19.
 */
@Aspect(Dao.class)
public final class DaoAspect implements Proxy {

	@Override
	public Object doProxy(ProxyChain proxyChain) throws Throwable {
		Object result = null;
		Method targetMethod = proxyChain.getTargetMethod();
		Object[] methodParams = proxyChain.getMethodParams();
		Class<?> daoClass = proxyChain.getTargetClass();
		// ??????Dao????????????????????????????????????Sql???Entity??????????????????????????????????????????
		String daoDataSourceName = daoClass.getAnnotation(Dao.class).dataSource();
		if (StringUtil.isEmpty(daoDataSourceName)) {
			daoDataSourceName = DataSourceHelper.getDefaultDataSourceName();
		}

		// ?????????sql???????????????
		Type returnType = null;
		Class<?> rawType = null;
		SqlResultProxy sqlResultProxy = null;
		if (targetMethod.isAnnotationPresent(ResultProxy.class)) {
			ResultProxy resultProxy = targetMethod.getAnnotation(ResultProxy.class);
			Class<? extends SqlResultProxy> proxyClass = resultProxy.value();
			returnType = resultProxy.returnType();// ????????????????????????????????????????????????List<Map<String,Object>>?????????
			rawType = resultProxy.rawType();// ????????????????????????????????????????????????List<Map<String,Object>>?????????
			sqlResultProxy = ReflectionUtil.newInstance(proxyClass);
		}

		if (targetMethod.isAnnotationPresent(Sql.class)) {
			Sql sqlAnnotation = targetMethod.getAnnotation(Sql.class);
			String rawSql = sqlAnnotation.value();
			String headAfterUnion = sqlAnnotation.headAfterUnion();// 2019/2/14
																	// sql????????????
			String tailAfterUnion = sqlAnnotation.tailAfterUnion();// 2019/2/14
																	// sql????????????

			// #??????????????????
			rawSql = CommonSqlUtil.convertSqlAppendCommand(rawSql, methodParams);
			headAfterUnion = CommonSqlUtil.convertSqlAppendCommand(headAfterUnion, methodParams);
			tailAfterUnion = CommonSqlUtil.convertSqlAppendCommand(tailAfterUnion, methodParams);

			// #??????Sql?????????????????????
			// #??????sql?????????Entity???
			// CaiDongyu 2019/2/13{ ????????????????????????????????????????????????????????????
			Map<String, TableSchema> sqlEntityTableMap = CommonSqlUtil.convertSqlEntity2Table(rawSql);

			List<Map<String, String>> sqlEntityTableNameList = new ArrayList<>();
			for (String entityClassSimpleName : sqlEntityTableMap.keySet()) {
				TableSchema tableSchema = sqlEntityTableMap.get(entityClassSimpleName);
				Map<String, String> sqlEntityTableNameMap = new HashMap<>();
				sqlEntityTableNameMap.put(tableSchema.getTableName(), entityClassSimpleName);
				sqlEntityTableNameList.add(sqlEntityTableNameMap);
			}

			String sql = CommonSqlUtil.convertRawSql(rawSql, sqlEntityTableMap, sqlEntityTableNameList);

			headAfterUnion = CommonSqlUtil.convertRawSql(headAfterUnion, sqlEntityTableMap, sqlEntityTableNameList);
			tailAfterUnion = CommonSqlUtil.convertRawSql(tailAfterUnion, sqlEntityTableMap, sqlEntityTableNameList);

			// sqlAry??????sql????????????????????????????????????
			if (sql.trim().toUpperCase().startsWith("SELECT")) {
				// }
				returnType = returnType == null ? targetMethod.getGenericReturnType() : returnType;
				rawType = rawType == null ? targetMethod.getReturnType() : rawType;
				if (returnType instanceof ParameterizedType) {
					Type[] actualTypes = ((ParameterizedType) returnType).getActualTypeArguments();
					// ????????????????????????Page???List???Map??????
					if (Page.class.isAssignableFrom(rawType) || // ???????????????????????????Page??????
							ReflectionUtil.compareType(List.class, rawType)) {

						sql = convertPageSql(sql, daoDataSourceName, methodParams);

						// TODO ?????????????????????DaoHelper????????????????????????????????????????????????????????????
						sql = unionSql(sql, headAfterUnion, tailAfterUnion);
						result = listResult(actualTypes[0], daoDataSourceName, sql, methodParams);

						if (Page.class.isAssignableFrom(rawType)) {
							// ????????????????????????????????????
							result = pageResult(sql, methodParams, (List<?>) result, daoDataSourceName);
						}
					} else if (ReflectionUtil.compareType(Map.class, rawType)) {
						// Map????????????????????????
						/*
						 * if (StringUtil.isEmpty(dataSourceName)) { result =
						 * DataBaseHelper.queryMap(sql, methodParams,
						 * parameterTypes); } else {
						 */
						result = DataBaseHelper.queryMap(unionSql(sql, headAfterUnion, tailAfterUnion), methodParams,
								daoDataSourceName);
						// }
					}
				} else {
					if (Page.class.isAssignableFrom(rawType) || ReflectionUtil.compareType(List.class, rawType)) {
						// Page???List
						/*
						 * if (StringUtil.isEmpty(dataSourceName)) { result =
						 * DataBaseHelper.queryList(sql, methodParams,
						 * parameterTypes); } else { result =
						 * DataBaseHelper.queryList(sql, methodParams,
						 * dataSourceName); }
						 */
						sql = convertPageSql(sql, daoDataSourceName, methodParams);

						sql = unionSql(sql, headAfterUnion, tailAfterUnion);
						result = listResult(returnType, daoDataSourceName, sql, methodParams);

						if (Page.class.isAssignableFrom(rawType)) {
							// ????????????????????????????????????
							result = pageResult(sql, methodParams, (List<?>) result, daoDataSourceName);
						}
					} else if (ReflectionUtil.compareType(Map.class, rawType)) {
						// Map
						/*
						 * if (StringUtil.isEmpty(dataSourceName)) { result =
						 * DataBaseHelper.queryMap(sql, methodParams,
						 * parameterTypes); } else {
						 */
						result = DataBaseHelper.queryMap(unionSql(sql, headAfterUnion, tailAfterUnion), methodParams,
								daoDataSourceName);
						// }
					} else if (ReflectionUtil.compareType(Object.class, rawType)) {
						// Object
						/*
						 * if (StringUtil.isEmpty(dataSourceName)) { result =
						 * DataBaseHelper.queryMap(sql, methodParams,
						 * parameterTypes); } else {
						 */
						result = DataBaseHelper.queryMap(unionSql(sql, headAfterUnion, tailAfterUnion), methodParams,
								daoDataSourceName);
						// }
					} else if (ReflectionUtil.compareType(String.class, rawType)
							|| ReflectionUtil.compareType(Byte.class, rawType)
							|| ReflectionUtil.compareType(Boolean.class, rawType)
							|| ReflectionUtil.compareType(Short.class, rawType)
							|| ReflectionUtil.compareType(Character.class, rawType)
							|| ReflectionUtil.compareType(Integer.class, rawType)
							|| ReflectionUtil.compareType(Long.class, rawType)
							|| ReflectionUtil.compareType(Float.class, rawType)
							|| ReflectionUtil.compareType(Double.class, rawType)) {
						// String
						result = getBasetypeOrDate(unionSql(sql, headAfterUnion, tailAfterUnion), methodParams,
								daoDataSourceName);
						result = CastUtil.castType(result, rawType);
					} else if ((rawType).isPrimitive()) {
						if (ReflectionUtil.compareType(void.class, rawType)) {
							// void
							/*
							 * if (StringUtil.isEmpty(dataSourceName)) { result
							 * = DataBaseHelper.queryList(sql, methodParams,
							 * parameterTypes); } else {
							 */
							result = DataBaseHelper.queryList(unionSql(sql, headAfterUnion, tailAfterUnion),
									methodParams, daoDataSourceName);
							// }
						} else {
							// ????????????
							/*
							 * if (StringUtil.isEmpty(dataSourceName)) { result
							 * = DataBaseHelper.queryPrimitive(sql,
							 * methodParams); } else {
							 */
							result = DataBaseHelper.queryPrimitive(unionSql(sql, headAfterUnion, tailAfterUnion),
									methodParams, daoDataSourceName);
							// }
						}
					} else {
						// Entity
						if (StringUtil.isEmpty(daoDataSourceName)) {
							result = DataBaseHelper.queryEntity(rawType, unionSql(sql, headAfterUnion, tailAfterUnion),
									methodParams);
						} else {
							result = DataBaseHelper.queryEntity(rawType, unionSql(sql, headAfterUnion, tailAfterUnion),
									methodParams, daoDataSourceName);
						}
					}
				}
			} else {
				if (StringUtil.isEmpty(daoDataSourceName)) {
					result = DataBaseHelper.executeUpdate(sql, methodParams);
				} else {
					result = DataBaseHelper.executeUpdate(sql, methodParams, daoDataSourceName);
				}
			}
		} else if (BaseRepository.class.isAssignableFrom(daoClass)
				&& !ReflectionUtil.compareType(BaseRepository.class, daoClass)) {
			String methodName = targetMethod.getName();
			Class<?>[] paramAry = targetMethod.getParameterTypes();
			if ("insertEntity".equals(methodName)) {
				// # Repository.insertEntity(Object entity);
				if (paramAry.length == 1 && ReflectionUtil.compareType(paramAry[0], Object.class)) {
					Object entity = methodParams[0];
					// 2018/12/29 ?????????????????????

					if (StringUtil.isEmpty(daoDataSourceName)) {
						result = DataBaseHelper.insertEntity(entity);
					} else {
						result = DataBaseHelper.insertEntity(entity, daoDataSourceName);
					}
				}
			} else if ("getEntity".equals(methodName)) {
				// # Repository.getEntity(T entity);
				if (paramAry.length == 1 && ReflectionUtil.compareType(paramAry[0], Object.class)) {
					Object entity = methodParams[0];
					if (StringUtil.isEmpty(daoDataSourceName)) {
						result = DataBaseHelper.getEntity(entity);
					} else {
						result = DataBaseHelper.getEntity(entity, daoDataSourceName);
					}
				}
			} else {
				result = proxyChain.doProxyChain();
			}
		} else {
			result = proxyChain.doProxyChain();
		}

		// ?????????Sql????????????????????????????????????
		if (sqlResultProxy != null) {
			result = sqlResultProxy.proxy(result);
		}
		return result;
	}

	// ???Sql??????????????????????????????DataBaseHelper????????????????????????????????????
	private String convertPageSql(String sql, String dataSourceName, Object[] methodParams) {
		// ?????????1???sql??????????????????????????????????????????????????????union??????
		SqlPackage sp = TDengineUtil.convertPagConfig(sql, methodParams);
		return sp.getSql();
	}

	private String unionSql(String sql, String headAfterUnion, String tailAfterUnion) {
		// Select ????????????union all????????????
		StringBuilder sqlBuf = new StringBuilder();
		if (StringUtil.isNotEmpty(headAfterUnion)) {
			sqlBuf.append(headAfterUnion).append(" ");
		}
		// ????????????headAfterUnion??????tailAfterUnion??????????????????sql?????????
		if (StringUtil.isNotEmpty(headAfterUnion) || StringUtil.isNotEmpty(tailAfterUnion)) {
			sqlBuf.append("SELECT * FROM (");
			sqlBuf.append(sql);
			sqlBuf.append(") t_").append(StringUtil.getRandomString(6));
		} else {
			sqlBuf.append(sql);
		}
		if (StringUtil.isNotEmpty(tailAfterUnion)) {
			sqlBuf.append(" ").append(tailAfterUnion);
		}
		return sqlBuf.toString();// select??????????????????sql?????????????????????1???
	}

	private Object getBasetypeOrDate(String sql, Object[] methodParams, String dataSourceName) throws SQLException {
		// Date
		Map<String, Object> resultMap = DataBaseHelper.queryMap(sql, methodParams,  dataSourceName);
		do {
			if (resultMap == null)
				break;
			if (CollectionUtil.isEmpty(resultMap))
				break;

			Set<String> keySet = resultMap.keySet();
			return resultMap.get(keySet.iterator().next());
		} while (false);
		return null;
	}

	private Object getBasetypeOrDateList(String sql, Object[] methodParams, String dataSourceName) throws SQLException {
		List<Map<String, Object>> resultList = null;
		if (StringUtil.isEmpty(dataSourceName)) {
			resultList = DataBaseHelper.queryList(sql, methodParams);
		} else {
			resultList = DataBaseHelper.queryList(sql, methodParams,  dataSourceName);
		}
		List<Object> list = new ArrayList<Object>();
		if (CollectionUtil.isNotEmpty(resultList)) {
			for (Map<String, Object> row : resultList) {
				if (row.size() == 1) {
					list.add(row.entrySet().iterator().next().getValue());
				}
			}
		}
		return list;
	}

	private Object listResult(Type returnType, String dataSourceName, String sql, Object[] methodParams) throws SQLException {
		Object result = null;
		if (returnType instanceof ParameterizedType) {
			// List<Map<String,Object>>
			if (ReflectionUtil.compareType(Map.class, (Class<?>) ((ParameterizedType) returnType).getRawType())) {
				if (StringUtil.isEmpty(dataSourceName)) {
					result = DataBaseHelper.queryList(sql, methodParams);
				} else {
					result = DataBaseHelper.queryList(sql, methodParams, dataSourceName);
				}
			}
		} else if (returnType instanceof WildcardType) {
			// List<?>
			if (StringUtil.isEmpty(dataSourceName)) {
				result = DataBaseHelper.queryList(sql, methodParams);
			} else {
				result = DataBaseHelper.queryList(sql, methodParams, dataSourceName);
			}
		} else {
			if (ReflectionUtil.compareType(Object.class, (Class<?>) returnType)) {
				// List<Object>
				if (StringUtil.isEmpty(dataSourceName)) {
					result = DataBaseHelper.queryList(sql, methodParams);
				} else {
					result = DataBaseHelper.queryList(sql, methodParams, dataSourceName);
				}
			} else if (ReflectionUtil.compareType(Map.class, (Class<?>) returnType)) {
				// List<Map>
				if (StringUtil.isEmpty(dataSourceName)) {
					result = DataBaseHelper.queryList(sql, methodParams);
				} else {
					result = DataBaseHelper.queryList(sql, methodParams, dataSourceName);
				}
			} else if (ReflectionUtil.compareType(String.class, (Class<?>) returnType)
					|| ReflectionUtil.compareType(Date.class, (Class<?>) returnType)
					|| ReflectionUtil.compareType(Byte.class, (Class<?>) returnType)
					|| ReflectionUtil.compareType(Boolean.class, (Class<?>) returnType)
					|| ReflectionUtil.compareType(Short.class, (Class<?>) returnType)
					|| ReflectionUtil.compareType(Character.class, (Class<?>) returnType)
					|| ReflectionUtil.compareType(Integer.class, (Class<?>) returnType)
					|| ReflectionUtil.compareType(Long.class, (Class<?>) returnType)
					|| ReflectionUtil.compareType(Float.class, (Class<?>) returnType)
					|| ReflectionUtil.compareType(Double.class, (Class<?>) returnType)) {
				// List<String>
				result = getBasetypeOrDateList(sql, methodParams, dataSourceName);
			} else {
				// Entity
				if (StringUtil.isEmpty(dataSourceName)) {
					result = DataBaseHelper.queryEntityList((Class<?>) returnType, sql, methodParams);
				} else {
					result = DataBaseHelper.queryEntityList((Class<?>) returnType, sql, methodParams, dataSourceName);
				}
			}
		}

		return result;
	}

	/**
	 * ??????List?????????????????????
	 */
	private <T> Page<T> pageResult(String sql, Object[] params, List<T> records,
			String dataSourceName) {

		PageConfig pageConfig = CommonSqlUtil.getPageConfigFromParams(params);
		Object[] params_ = new Object[params.length - 1];
		for (int i = 0; i < params_.length; i++) {
			params_[i] = params[i];
		}
		long count = 0;
		if (StringUtil.isEmpty(dataSourceName)) {
			count = DataBaseHelper.countQuery(sql, params_);
		} else {
			count = DataBaseHelper.countQuery(sql, params_, dataSourceName);
		}
		pageConfig = pageConfig == null ? new PageConfig(1, count) : pageConfig;
		long pages = count / pageConfig.getPageSize();
		if (pages * pageConfig.getPageSize() < count)
			pages++;

		return new Page<>(records, pageConfig, count, pages);
	}

}
