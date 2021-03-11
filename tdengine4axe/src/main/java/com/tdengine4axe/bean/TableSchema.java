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
package com.tdengine4axe.bean;

import java.util.List;

import org.axe.bean.pojo.EntityFieldMethod;

/**
 * 封装 Dao Entity类的结构描述
 * @author CaiDongyu.
 */
public final class TableSchema {
	
	//字段结构
	public static final class ColumnSchema{
		//只有一个可能是true
		private boolean isId;
		private boolean isColumn;
		private boolean isTag;
		
		//变量名
		private String fieldName;
		//类型
		private String fieldType;
		//描述,TDengine建表没用
		private String comment;
		
		//字段名，驼峰处理后
		private String columnName;
		
		//字段结构
		private EntityFieldMethod columnSchema;
		
		
		public boolean isId() {
			return isId;
		}
		public void setId(boolean isId) {
			this.isId = isId;
			if(this.isId){
				this.isColumn = false;
				this.isTag = false;
			}
		}
		public boolean isColumn() {
			return isColumn;
		}
		public void setColumn(boolean isColumn) {
			this.isColumn = isColumn;
			if(this.isColumn){
				this.isId = false;
				this.isTag = false;
			}
		}
		public boolean isTag() {
			return isTag;
		}
		public void setTag(boolean isTag) {
			this.isTag = isTag;
			if(this.isTag){
				this.isId = false;
				this.isColumn = false;
			}
		}
		public String getFieldName() {
			return fieldName;
		}
		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}
		public String getColumnName() {
			return columnName;
		}
		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}
		public String getFieldType() {
			return fieldType;
		}
		public void setFieldType(String fieldType) {
			this.fieldType = fieldType;
		}
		public String getComment() {
			return comment;
		}
		public void setComment(String comment) {
			this.comment = comment;
		}
		public EntityFieldMethod getColumnSchema() {
			return columnSchema;
		}
		public void setColumnSchema(EntityFieldMethod columnSchema) {
			this.columnSchema = columnSchema;
		}
	}
	
	//是否是superTable模式
	private boolean superTable=false;
	//表名
	private String tableName;
	//表备注
	private String tableComment;
	//是否自动创建
	private boolean autoCreate=false;
	//数据源名称
	private String dataSourceName;
	
	//对应数据类
	private Class<?> entityClass;
	//映射的字段
	private List<ColumnSchema> mappingColumnList;
	
	//主键约束
	private String idField;
	private String idColumn;
	
	public boolean isSuperTable() {
		return superTable;
	}
	public void setSuperTable(boolean superTable) {
		this.superTable = superTable;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTableComment() {
		return tableComment;
	}
	public void setTableComment(String tableComment) {
		this.tableComment = tableComment;
	}
	public boolean getAutoCreate() {
		return autoCreate;
	}
	public void setAutoCreate(boolean isAutoCreate) {
		this.autoCreate = isAutoCreate;
	}
	public String getDataSourceName() {
		return dataSourceName;
	}
	public void setDataSourceName(String dataSourceName) {
		this.dataSourceName = dataSourceName;
	}
	public Class<?> getEntityClass() {
		return entityClass;
	}
	public void setEntityClass(Class<?> entityClass) {
		this.entityClass = entityClass;
	}
	public List<ColumnSchema> getMappingColumnList() {
		return mappingColumnList;
	}
	public void setMappingColumnList(List<ColumnSchema> mappingColumnList) {
		this.mappingColumnList = mappingColumnList;
	}
	public String getIdField() {
		return idField;
	}
	public void setIdField(String idField) {
		this.idField = idField;
	}
	public String getIdColumn() {
		return idColumn;
	}
	public void setIdColumn(String idColumn) {
		this.idColumn = idColumn;
	}
	
}
