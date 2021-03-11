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

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.axe.interface_.base.Helper;
import org.axe.util.PropsUtil;

import com.tdengine4axe.bean.TableSchema;
import com.tdengine4axe.constant.TDengineConfigConstant;
import com.tdengine4axe.util.TDengineUtil;

/**
 * @author CaiDongyu
 * 数据库Schema 助手类
 */
public final class SchemaHelper implements Helper{
	@Override
	public void init() throws Exception {}

	@Override
	public void onStartUp() throws Exception {
		//在框架的Helper都初始化后，同步表结构，（为了支持多数据源，借鉴了Rose框架）
		Map<String, TableSchema> ENTITY_TABLE_MAP = TableHelper.getEntityTableSchemaCachedMap();
		//默认按@Table里的来
		for(TableSchema tableSchema:ENTITY_TABLE_MAP.values()){
			Properties configProps = TDengineConfigHelper.getCONFIG_PROPS();
			//TODO 这块应该放在全局初始化时候，放在DataSourceHelper，这里应该直接获取，不应该再拼字符串
			Boolean autoCreateTable = PropsUtil.getBoolean(configProps,TDengineConfigConstant.DATASOURCE + "." + tableSchema.getDataSourceName() + "." + TDengineConfigConstant.AUTO_CREATE_TABLE,null);
			if(autoCreateTable == null){
				//全局未配置，不创建
			}else if(autoCreateTable){
				//按@Table里的来
				if(tableSchema.getAutoCreate()){
					createTable(tableSchema);
				}
			}else{
				//全局关闭了，优先级也最高，直接不创建
			}
		}
	}
	
	public static void createTable(TableSchema tableSchema) throws SQLException{
		String tableCreateSql = TDengineUtil.getTableCreateSql(tableSchema.getDataSourceName(),tableSchema);
		
		try {
			DataBaseHelper.executeUpdate(tableCreateSql, new Object[]{}, tableSchema.getDataSourceName());
		} catch (Exception e) {
			//如果是表重复，则不做处理，这种情况
			if(e != null && e.getMessage().toUpperCase().contains("DUPLICATE")){
				//不做处理
			}else{
				throw e;
			}
		}
	}
	
}
