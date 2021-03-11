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
package com.tdengine4axe.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.axe.annotation.common.ColumnDefine;
import org.axe.util.ReflectionUtil;
import org.axe.util.StringUtil;

import com.tdengine4axe.bean.PageConfig;
import com.tdengine4axe.bean.SqlPackage;
import com.tdengine4axe.bean.TableSchema;
import com.tdengine4axe.bean.TableSchema.ColumnSchema;
import com.tdengine4axe.helper.TableHelper;

/**
 * Sql 解析 助手类 剥离自DataBaseHelper @author CaiDongyu.
 */
public final class TDengineUtil {

	private TDengineUtil() {
	}

	/**
	 * tdengine sql 关键字，暂时跟mysql一样
	 */
	public static final String KEYWORD = ",ADD,ALL,ALTER," + "ANALYZE,AND,AS,ASC,ASENSITIVE,"
			+ "BEFORE,BETWEEN,BIGINT,BINARY," + "BLOB,BOTH,BY,CALL,CASCADE,CASE,"
			+ "CHANGE,CHAR,CHARACTER,CHECK,COLLATE," + "COLUMN,CONDITION,CONNECTION,"
			+ "CONSTRAINT,CONTINUE,CONVERT,CREATE," + "CROSS,CURRENT_DATE,CURRENT_TIME,"
			+ "CURRENT_TIMESTAMP,CURRENT_USER,CURSOR," + "DATABASE,DATABASES,DAY_HOUR,DAY_MICROSECOND,"
			+ "DAY_MINUTE,DAY_SECOND,DEC,DECIMAL,DECLARE," + "DEFAULT,DELAYED,DELETE,DESC,DESCRIBE,"
			+ "DETERMINISTIC,DISTINCT,DISTINCTROW," + "DIV,DOUBLE,DROP,DUAL,EACH,ELSE,ELSEIF,"
			+ "ENCLOSED,ESCAPED,EXISTS,EXIT,EXPLAIN,FALSE," + "FETCH,FLOAT,FLOAT4,FLOAT8,FOR,FORCE,FOREIGN,"
			+ "FROM,FULLTEXT,GOTO,GRANT,GROUP,HAVING," + "HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE,"
			+ "HOUR_SECOND,IF,IGNORE,IN,INDEX,INFILE,INNER," + "INOUT,INSENSITIVE,INSERT,INT,INT1,INT2,INT3,"
			+ "INT4,INT8,INTEGER,INTERVAL,INTO,IS,ITERATE," + "JOIN,KEY,KEYS,KILL,LABEL,LEADING,LEAVE,LEFT,"
			+ "LIKE,LIMIT,LINEAR,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,"
			+ "LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY," + "MATCH,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,"
			+ "MINUTE_MICROSECOND,MINUTE_SECOND,MOD,MODIFIES," + "NATURAL,NOT,NO_WRITE_TO_BINLOG,NULL,NUMERIC,"
			+ "ON,OPTIMIZE,OPTION,OPTIONALLY,OR,ORDER,OUT,OUTER," + "OUTFILE,PRECISION,PRIMARY,PROCEDURE,PURGE,RAID0,"
			+ "RANGE,READ,READS,REAL,REFERENCES,REGEXP,RELEASE," + "RENAME,REPEAT,REPLACE,REQUIRE,RESTRICT,RETURN,"
			+ "REVOKE,RIGHT,RLIKE,SCHEMA,SCHEMAS,SECOND_MICROSECOND,"
			+ "SELECT,SENSITIVE,SEPARATOR,SET,SHOW,SMALLINT,SPATIAL," + "SPECIFIC,SQL,SQLEXCEPTION,SQLSTATE,SQLWARNING,"
			+ "SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,"
			+ "SSL,STARTING,STRAIGHT_JOIN,TABLE,TERMINATED,THEN,TINYBLOB,"
			+ "TINYINT,TINYTEXT,TO,TRAILING,TRIGGER,TRUE,UNDO,UNION,"
			+ "UNIQUE,UNLOCK,UNSIGNED,UPDATE,USAGE,USE,USING,UTC_DATE,"
			+ "UTC_TIME,UTC_TIMESTAMP,VALUES,VARBINARY,VARCHAR,VARCHARACTER,"
			+ "VARYING,WHEN,WHERE,WHILE,WITH,WRITE,X509,XOR,YEAR_MONTH,ZEROFILL,"
			+ "ACTION,BIT,DATE,ENUM,NO,TEXT,TIME,TIMESTAMP,";

	// #所有列出的java到tdengine的类型转换
	private static Map<String, String> JAVA_TYPE_2_TDENGINE_TYPE_MAP = new HashMap<>(); // #所有列出的java到mysql的类型转换
	static {
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("byte", "TINYINT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.lang.Byte", "TINYINT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("short", "SMALLINT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.lang.Short", "SMALLINT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("int", "INT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.lang.Integer", "INT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("long", "BIGINT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.lang.Long", "BIGINT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("float", "FLOAT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.lang.Float", "FLOAT");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("double", "DOUBLE");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.lang.Double", "DOUBLE");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("boolean", "BOOL");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.lang.Boolean", "BOOL");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.lang.String", "NCHAR(255)");// 可以存中文，默认255个字节
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.sql.Date", "TIMESTAMP");
		JAVA_TYPE_2_TDENGINE_TYPE_MAP.put("java.util.Date", "TIMESTAMP");
	}

	// 只是给SchemaHelper用，在框架启动初期，还没有entity对象，只能有entityClass的时候，用于统一建表
	public static String getTableCreateSql(String dataSourceName, TableSchema tableSchema) {
		return getTableCreateSql(dataSourceName, tableSchema.getTableName(), tableSchema);
	}

	private static String getTableCreateSql(String dataSourceName, String tableName, TableSchema tableSchema) {
		StringBuilder createTableSqlBufer = new StringBuilder();
		
		if(tableSchema.isSuperTable()){
			//超级表
			createTableSqlBufer.append("CREATE STABLE IF NOT EXISTS ").append(tableName).append(" (");
		}else{
			//普通表
			createTableSqlBufer.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
		}
		
		
		List<ColumnSchema> mappingColumnList = tableSchema.getMappingColumnList();
		ColumnSchema idColumnSchema = null;
		List<ColumnSchema> normalColumnList = new ArrayList<>();
		List<ColumnSchema> tagColumnList = new ArrayList<>();
		for (ColumnSchema columnSchema : mappingColumnList) {
			if(columnSchema.isId()){
				// #主键
				idColumnSchema = columnSchema;
			}else if (columnSchema.isColumn()) {
				// #普通建处理
				normalColumnList.add(columnSchema);
			}else if(columnSchema.isTag()){
				// #标签
				tagColumnList.add(columnSchema);
			}
		}
		
		
		// #主键，其实根据TDengine的要求，就是TIMESTAMP类型，必须放第一个
		createTableSqlBufer.append(idColumnSchema.getColumnName());
		String columnDefine = javaType2TDengineColumnDefine(idColumnSchema.getColumnSchema().getField(), true);
		if (StringUtil.isEmpty(columnDefine)) {
			throw new RuntimeException(tableSchema.getEntityClass().getName() + "#[" + idColumnSchema.getFieldName()
					+ "] connot convert to TDengine type from " + idColumnSchema.getFieldType());
		}
		createTableSqlBufer.append(" ").append(columnDefine);

		// #普通建处理
		if(normalColumnList.size() > 0){
			createTableSqlBufer.append(",");
		}
		for (int i = 0; i < normalColumnList.size(); i++) {
			ColumnSchema columnSchema = normalColumnList.get(i);
			createTableSqlBufer.append(columnSchema.getColumnName());
			columnDefine = javaType2TDengineColumnDefine(columnSchema.getColumnSchema().getField(), true);
			if (StringUtil.isEmpty(columnDefine)) {
				throw new RuntimeException(tableSchema.getEntityClass().getName() + "#[" + columnSchema.getFieldName()
						+ "] connot convert to mysql type from " + columnSchema.getFieldType());
			}
			createTableSqlBufer.append(" ").append(columnDefine);

			if (i < normalColumnList.size() - 1) {
				createTableSqlBufer.append(",");
			}
		}

		createTableSqlBufer.append(") ");
		
		// #标签字段
		if(tagColumnList.size() > 0){
			createTableSqlBufer.append(" TAGS (");
			for (int i = 0; i < tagColumnList.size(); i++) {
				ColumnSchema columnSchema = tagColumnList.get(i);
				createTableSqlBufer.append(columnSchema.getColumnName());
				columnDefine = javaType2TDengineColumnDefine(columnSchema.getColumnSchema().getField(), true);
				if (StringUtil.isEmpty(columnDefine)) {
					throw new RuntimeException(tableSchema.getEntityClass().getName() + "#[" + columnSchema.getFieldName()
							+ "] connot convert to mysql type from " + columnSchema.getFieldType());
				}
				createTableSqlBufer.append(" ").append(columnDefine);

				if (i < normalColumnList.size() - 1) {
					createTableSqlBufer.append(",");
				}
			}
			createTableSqlBufer.append(")");
		}

		return createTableSqlBufer.toString();
	}

	public static String javaType2TDengineColumnDefine(Field field, boolean nullAble) {
		if (field.isAnnotationPresent(ColumnDefine.class)) {
			return field.getAnnotation(ColumnDefine.class).value();
		} else {
			return JAVA_TYPE_2_TDENGINE_TYPE_MAP.get(field.getType().getName());
		}
	}

	public static SqlPackage getInsertSqlPackage(Object entity) {
		StringBuilder sql = new StringBuilder("INSERT INTO ").append(TableHelper.getRealTableName(entity));
		TableSchema tableSchema = TableHelper.getCachedTableSchema(entity);
		StringBuilder superTableTagValue = null;
		List<Object> superTableTagParams = new ArrayList<>();
		if(tableSchema.isSuperTable()) {
			sql.append(" USING ").append(tableSchema.getTableName()).append("(");
			superTableTagValue = new StringBuilder(" TAGS (");
			superTableTagParams = new LinkedList<>();
		}
		
		List<ColumnSchema> mappingColumnList = TableHelper.getCachedTableSchema(entity).getMappingColumnList();
		StringBuilder columns = new StringBuilder("(");
		StringBuilder values = new StringBuilder(" VALUES (");
		List<Object> params = new LinkedList<>();
		for (ColumnSchema columnSchema : mappingColumnList) {
			//只会插入 Id Column两种，tag不是插入的
			if(columnSchema.isId() || columnSchema.isColumn()){
				columns.append("").append(columnSchema.getColumnName()).append(", ");
				values.append("?, ");
				params.add(ReflectionUtil.invokeMethod(entity, columnSchema.getColumnSchema().getMethod()));
			}else if(columnSchema.isTag()){
				sql.append("").append(columnSchema.getColumnName()).append(", ");
				superTableTagValue.append("?, ");
				superTableTagParams.add(ReflectionUtil.invokeMethod(entity, columnSchema.getColumnSchema().getMethod()));
			}
		}
		if(superTableTagValue != null){
			sql.replace(sql.lastIndexOf(", "), sql.length(), ") ");
			superTableTagValue.replace(superTableTagValue.lastIndexOf(", "), superTableTagValue.length(), ") ");
			sql.append(superTableTagValue);
			params.addAll(0, superTableTagParams);
		}
		columns.replace(columns.lastIndexOf(", "), columns.length(), ")");
		values.replace(values.lastIndexOf(", "), values.length(), ")");
		sql.append(columns).append(values);
		return new SqlPackage(sql.toString(), params.toArray(), null);
	}

	/**
	 * 转换占位符 ?1 转换pageConfig占位符
	 */
	public static SqlPackage convertGetFlag(String sql, Object[] params) {
		// #转换pageConfig
		SqlPackage sqlPackage = convertPagConfig(sql, params);
		return CommonSqlUtil.convertGetFlag(sqlPackage);
	}

	/**
	 * 转换 分页查询条件 转换有条件，如果params里包含约定位置(末尾)的pageConfig，就转换，如果没有，就不作处理
	 * 但是，如果有pageConfig但是书写方式不符合约定，会报异常
	 */
	public static SqlPackage convertPagConfig(String sql, Object[] params) {
		// #检测占位符是否都符合格式
		// ?后面跟1~9,如果两位数或者更多位,则十位开始可以0~9
		// 但是只用检测个位就好
		boolean[] getFlagModeAry = CommonSqlUtil.analysisGetFlagMode(sql);
		boolean getFlagComm = getFlagModeAry[0];// 普通模式 就是?不带数字
		boolean getFlagSpec = getFlagModeAry[1];// ?带数字模式

		// 不可以两种模式都并存，只能选一种，要么?都带数字，要么?都不带数字
		if (getFlagComm && getFlagSpec)
			throw new RuntimeException("invalid sql statement with ?+number and only ?: " + sql);

		// #params检测是否包含pageConfig，包含的位置
		do {
			PageConfig pageConfig = CommonSqlUtil.getPageConfigFromParams(params);
			if (pageConfig == null)
				break;
			
			//非union模式的外部分页，就是limit n1,n2
			sql = sql + " limit "+pageConfig.getLimitParam1()+","+pageConfig.getLimitParam2();
		} while (false);
		return new SqlPackage(sql, params, new boolean[] { getFlagComm, getFlagSpec });
	}

	public static Map<String, String> getJAVA_TYPE_2_TDENGINE_TYPE_MAP() {
		return JAVA_TYPE_2_TDENGINE_TYPE_MAP;
	}
}
