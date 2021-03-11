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
package com.tdengine4axe.datasource;

import java.sql.Connection;
import java.sql.SQLException;

import org.axe.util.LogUtil;

import com.tdengine4axe.annotation.DataSource;
import com.tdengine4axe.helper.TDengineConfigHelper;
import com.tdengine4axe.interface_.BaseDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@DataSource("hikaricp-datasource")
public final class HikariCPDatasource implements BaseDataSource{
	
    //#数据库
    private HikariDataSource ds;
    
	public HikariCPDatasource() {
		//#初始化jdbc配置
		try {
			HikariConfig config = new HikariConfig();
			// jdbc properties
			config.setDriverClassName(TDengineConfigHelper.getDriver());
			config.setJdbcUrl(TDengineConfigHelper.getUrl());
			config.setUsername(TDengineConfigHelper.getUsername());
			config.setPassword(TDengineConfigHelper.getPassword());
			// connection pool configurations
			config.setMinimumIdle(10);           //minimum number of idle connection
			config.setMaximumPoolSize(10);      //maximum number of connection in the pool
			config.setConnectionTimeout(30000); //maximum wait milliseconds for get connection from pool
			config.setMaxLifetime(0);       // maximum life time for each connection
			config.setIdleTimeout(0);       // max idle time for recycle idle connection
			config.setConnectionTestQuery("select server_status()"); //validation query
			ds = new HikariDataSource(config); //create datasource
		} catch (Exception e) {
			LogUtil.error(e);
		}
	}
	
	@Override
	public Connection getConnection() throws SQLException {
		return ds.getConnection(); // get connection
	}
	
	@Override
	public void closeConnection(Connection con) throws SQLException{
		con.close();
	}
	
}
