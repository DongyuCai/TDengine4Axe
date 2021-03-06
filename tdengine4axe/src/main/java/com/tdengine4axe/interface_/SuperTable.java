package com.tdengine4axe.interface_;

/**
 * 表名修改器
 * 凡是实现此接口的Table实体类，都拥有在sql执行前，对表名最终确定的能力
 * @author CaiDongyu 2018/12/28
 */
public interface SuperTable {
	
	/**
	 * 最终的真实表名，可以是实体类类名
	 */
	public String subTableName()throws Exception;
	
}
