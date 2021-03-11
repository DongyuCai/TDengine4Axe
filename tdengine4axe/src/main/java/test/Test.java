package test;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.axe.Axe;
import org.axe.annotation.mvc.Interceptor;
import org.axe.constant.ConfigConstant;
import org.axe.extra.timer.TimerTask;
import org.axe.helper.base.ConfigHelper;
import org.axe.helper.ioc.BeanHelper;
import org.axe.helper.ioc.ClassHelper;
import org.axe.interface_.mvc.AfterClassLoaded;
import org.axe.interface_.mvc.AfterConfigLoaded;
import org.axe.interface_.mvc.Filter;
import org.axe.interface_.mvc.Listener;
import org.axe.util.JsonUtil;
import org.axe.util.LogUtil;
import org.axe.util.StringUtil;

import com.tdengine4axe.TDengine;

public class Test {
	public static void main(String[] args) {
		try {
			TDengine.config();
			
			ConfigHelper.addAfterConfigLoadedCallback(new AfterConfigLoaded() {
				@Override
				public void doSomething(Properties prop) {
					//增加test包扫描路径
					prop.put(ConfigConstant.APP_BASE_PACKAGE, ConfigHelper.getCONFIG_PROPS().get(ConfigConstant.APP_BASE_PACKAGE)+",test");
				}
			});
			
			ClassHelper.addAfterClassLoadedCallback(new AfterClassLoaded() {
				@Override
				public void doSomething(Set<Class<?>> classSet) {
					Set<Class<?>> classesRemove = new HashSet<>();
					for(Class<?> cls:classSet){
						if(Listener.class.isAssignableFrom(cls)){
							classesRemove.add(cls);
						}
						if(TimerTask.class.isAssignableFrom(cls)){
							classesRemove.add(cls);
						}
						if(Filter.class.isAssignableFrom(cls)){
							classesRemove.add(cls);
						}
						if(Interceptor.class.isAssignableFrom(cls)){
							classesRemove.add(cls);
						}
					}
					classSet.removeAll(classesRemove);
				}
			});
			
			Axe.init();
			
			//执行插入一条数据
			//value
			Meters meters = new Meters();
			meters.setId(1L);//设备1
			meters.setCurrent((float)2.1);//电流
			meters.setVoltage(5);//电压
			//tag
			meters.setLocation("Beijing.chaoyang");//区域
			meters.setGroupdId(2);//设备2组
			
			TestDao dao = BeanHelper.getBean(TestDao.class);
			
			//增
			for(int i=0;i<10;i++){
				meters.setTs(new Date());
				meters.setVoltage(Integer.parseInt(StringUtil.getRandomString(1,"3456")));
				dao.insertEntity(meters);
			}
			
			//查
			List<Meters> logList = dao.getLogList(1);
			for(Meters log:logList){
				System.out.println(JsonUtil.toJson(log));
			}

		} catch (Exception e) {
			LogUtil.error(e);
		}
	}
}
