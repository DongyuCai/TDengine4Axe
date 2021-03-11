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
package com.tdengine4axe.helper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.axe.annotation.common.Comment;
import org.axe.bean.pojo.EntityFieldMethod;
import org.axe.helper.ioc.ClassHelper;
import org.axe.interface_.base.Helper;
import org.axe.util.CollectionUtil;
import org.axe.util.ReflectionUtil;
import org.axe.util.StringUtil;

import com.tdengine4axe.annotation.Column;
import com.tdengine4axe.annotation.Id;
import com.tdengine4axe.annotation.Table;
import com.tdengine4axe.annotation.Tag;
import com.tdengine4axe.bean.TableSchema;
import com.tdengine4axe.bean.TableSchema.ColumnSchema;
import com.tdengine4axe.interface_.SuperTable;
import com.tdengine4axe.util.CommonSqlUtil;
import com.tdengine4axe.util.TDengineUtil;

/**
 * @Table 数据库 entity 助手类 解析表名，字段名 
 * 剥离自DataBaseHelper 
 * @author CaiDongyu on 2016/5/6. 
 */
public final class TableHelper implements Helper{

	// #@Table 实体
	private static Map<String, TableSchema> ENTITY_TABLE_MAP;

	@Override
	public void init() throws Exception{
		synchronized (this) {
			ENTITY_TABLE_MAP = new HashMap<>();
			// #加载所有@Table指定的Entity类
			// select a.* from ClassA    这句sql里的ClassA，就是entityClassMap里的key
			Set<Class<?>> tableClassSet = ClassHelper.getClassSetByAnnotation(Table.class);
			
			for (Class<?> entityClass : tableClassSet) {
				String entityClassSimpleName = entityClass.getSimpleName();
				if (ENTITY_TABLE_MAP.containsKey(entityClassSimpleName)) {
					throw new Exception("find the same entity class: " + entityClass.getName() + " == "
							+ ENTITY_TABLE_MAP.get(entityClassSimpleName).getEntityClass().getName());
				}
				
				TableSchema tableSchema = convertEntityClass2TableSchema(entityClass);
				
				ENTITY_TABLE_MAP.put(entityClassSimpleName, tableSchema);
			}
			
		}
	}

	public static TableSchema convertEntityClass2TableSchema(Class<?> entityClass) throws Exception{
		String entityClassName = entityClass.getSimpleName();
		
		TableSchema tableSchema = new TableSchema();
		
		Table tableAnnotation = entityClass.getAnnotation(Table.class);
		tableSchema.setTableName(tableAnnotation.tableName());
		tableSchema.setSuperTable(SuperTable.class.isAssignableFrom(entityClass));
		tableSchema.setTableComment(StringUtil.isEmpty(tableAnnotation.comment())?"":tableAnnotation.comment());
		tableSchema.setDataSourceName(StringUtil.isEmpty(tableAnnotation.dataSource())?DataSourceHelper.getDefaultDataSourceName():tableAnnotation.dataSource());
		tableSchema.setAutoCreate(tableAnnotation.autoCreate());
		
		tableSchema.setEntityClass(entityClass);

		// #检测表名是否影响
		String sqlKeyword = TDengineUtil.KEYWORD;
		// * 如果KEYWORDS中包含此类名，并且类对于的表名两者不一样，那么就不行了
		if (CommonSqlUtil.checkIsSqlKeyword(sqlKeyword,entityClassName.toUpperCase())) {
			if (!entityClassName.equalsIgnoreCase(tableSchema.getTableName()))
				throw new Exception("invalid class name[" + entityClassName + "] because sql keyword will be affected!");
		}
		
		// #取含有get方法的字段，作为数据库表字段，没有get方法的字段，认为不是数据库表字段
		List<EntityFieldMethod> entityFieldMethodList = ReflectionUtil.getGetMethodList(entityClass);
		String idField = null;
		String idColumn = null;
		boolean hasTagColumn = false;
		List<ColumnSchema> mappingColumnList = new ArrayList<>();
		if(CollectionUtil.isNotEmpty(entityFieldMethodList)){
			for(EntityFieldMethod efm:entityFieldMethodList){
				Field field = efm.getField();
				ColumnSchema columnSchema = new ColumnSchema();
				//id column tag三个注解必须有一个，才是表结构字段
				if(field.isAnnotationPresent(Id.class)){
					//id只能有一个字段
					if(idField != null){
						throw new Exception("conflict @Id field finded in entity class [" + entityClassName + "]!");
					}
					//还必须是时间戳类型
					String columnDefine = TDengineUtil.javaType2TDengineColumnDefine(field, true);
					if(!columnDefine.contains("TIMESTAMP")){
						throw new Exception("wrong @Id field finded in entity class [" + entityClassName + "],only TIMESTAMP type can be defined!");
					}
					idField = field.getName();
					idColumn = StringUtil.camelToUnderline(idField);
					columnSchema.setId(true);
				}else if(field.isAnnotationPresent(Column.class)){
					columnSchema.setColumn(true);
				}else if(field.isAnnotationPresent(Tag.class)){
					columnSchema.setTag(true);
					hasTagColumn = true;
				}else{
					continue;
				}
				
				columnSchema.setFieldName(field.getName());
				if(field.isAnnotationPresent(Comment.class)){
					columnSchema.setComment(field.getAnnotation(Comment.class).value());
				}else{
					columnSchema.setComment("");
				}
				columnSchema.setColumnName(StringUtil.camelToUnderline(columnSchema.getFieldName()));
				
				
				columnSchema.setFieldType(field.getType().getSimpleName());
				columnSchema.setColumnSchema(efm);
				mappingColumnList.add(columnSchema);
				
				// 如果KEYWORDS中包含此字段名，并且字段名通过驼峰->下划线转换后，与原来不一致，那就不行
				if (CommonSqlUtil.checkIsSqlKeyword(sqlKeyword,columnSchema.getFieldName().toUpperCase())) {
					if (!columnSchema.getFieldName().equalsIgnoreCase(columnSchema.getColumnName()))
						throw new Exception(
								"invalid field name#[" + columnSchema.getColumnName() + "] in class#[" + entityClassName
										+ "] because sql keyword will be affected!");
				}
				
			}
		}
		tableSchema.setMappingColumnList(mappingColumnList);
		
		if(idField == null){
			throw new Exception("can not find @Id field in entity class [" + entityClassName + "]!");
		}
		tableSchema.setIdField(idField);
		tableSchema.setIdColumn(idColumn);
		
		if(tableSchema.isSuperTable() && !hasTagColumn){
			//超级表必须定义Tag
			throw new Exception("can not find @Tag field in super table entity class [" + entityClassName + "]!");
		}else if(!tableSchema.isSuperTable() && hasTagColumn){
			//普通表不可以定义Tag
			throw new Exception("@Tag field finded in table entity class [" + entityClassName + "],it is not an super table!");
		}
		
		return tableSchema;
	}
	
	/**
	 * 返回所有@Table标注的Entity类
	 */
	public static Map<String, TableSchema> getEntityTableSchemaCachedMap() {
		return ENTITY_TABLE_MAP;
	}

	/**
	 * 返回所有@Table标注的Entity类
	 */
	public static TableSchema getCachedTableSchema(Class<?> entityClass) {
		return ENTITY_TABLE_MAP.get(entityClass.getSimpleName());
	}

	/**
	 * 返回所有@Table标注的Entity类
	 */
	public static TableSchema getCachedTableSchema(Object entity) {
		//循环获取super类类型，直到获取到Table类
		Class<? extends Object> tableClass = entity.getClass();
		TableSchema tableSchema = getCachedTableSchema(tableClass);
		while(true){
			if(tableSchema != null){
				break;
			}
			tableClass = tableClass.getSuperclass();
			tableSchema = getCachedTableSchema(tableClass);
		};
		if(tableSchema == null){
			throw new RuntimeException(entity.getClass().getName() + " is not a table entity class,no @Table annotation is found on it or it super class set");
		}
		return tableSchema;
	}
	
	/**
	 * 根据Entity实例获取表名 必须有@Table注解
	 * 这个方法兼顾了分表
	 */
	public static String getRealTableName(Object entity) {
		//循环获取super类类型，直到获取到Table类
		Class<? extends Object> tableClass = entity.getClass();
		while(true){
			if(tableClass.isAnnotationPresent(Table.class)){
				break;
			}
			tableClass = tableClass.getSuperclass();
		};
		if(!tableClass.isAnnotationPresent(Table.class)){
			throw new RuntimeException(entity.getClass().getName() + " is not a table entity class,no @Table annotation is found on it or it super class set");
		}
		
		if(SuperTable.class.isAssignableFrom(tableClass)){
			//如果是可修改表名的接口实现类
			try {
				SuperTable tne = (SuperTable)entity;
				return tne.subTableName();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}else{
			//如果只是普通的表实体类，就用类解析表名
			return tableClass.getAnnotation(Table.class).tableName();
		}
	}
	
	@Override
	public void onStartUp() throws Exception {}
}
