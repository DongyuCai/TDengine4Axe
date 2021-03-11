package com.tdengine4axe;

import java.util.List;
import java.util.Properties;

import org.axe.constant.ConfigConstant;
import org.axe.helper.HelperLoader;
import org.axe.helper.base.ConfigHelper;
import org.axe.helper.ioc.IocHelper;
import org.axe.interface_.base.Helper;
import org.axe.interface_.mvc.AfterConfigLoaded;
import org.axe.interface_.mvc.AfterHelperLoaded;

import com.tdengine4axe.helper.DataBaseHelper;
import com.tdengine4axe.helper.DataSourceHelper;
import com.tdengine4axe.helper.SchemaHelper;
import com.tdengine4axe.helper.TDengineConfigHelper;
import com.tdengine4axe.helper.TableHelper;

/**
 * TDengine的初始化类
 * 负责结合axe框架的初始化工作，省去了读配置文件
 */
public final class TDengine {
	
	public static void config(){
		//添加框架扫描包路径
		//必须在axe框架读取配置后，初始化之前
		ConfigHelper.addAfterConfigLoadedCallback(new AfterConfigLoaded() {
			@Override
			public void doSomething(Properties config) {
				String value = config.getProperty(ConfigConstant.APP_BASE_PACKAGE).toString();
				if(!value.contains("com.tdengine4axe")){
					value = value+",com.tdengine4axe";
				}
				config.setProperty(ConfigConstant.APP_BASE_PACKAGE, value);
			}
		});
		
		//添加Helper到axe框架
		HelperLoader.addAfterHelperLoadedCallback(new AfterHelperLoaded() {
			@Override
			public void doSomething(List<Helper> helperList) {
				int iocHelperIndex = 0;
				for(Helper helper:helperList){
					if(helper.getClass().equals(IocHelper.class)){
						break;
					}
					iocHelperIndex++;
				}
				//在ioc之前加入helper
				helperList.add(iocHelperIndex,new SchemaHelper());//最先添加的会排在后面
				helperList.add(iocHelperIndex,new TableHelper());
				helperList.add(iocHelperIndex,new DataBaseHelper());
				helperList.add(iocHelperIndex,new DataSourceHelper());
				helperList.add(iocHelperIndex,new TDengineConfigHelper());
			}
		});
	}
	
}
