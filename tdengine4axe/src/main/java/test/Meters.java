package test;

import java.util.Date;

import org.axe.annotation.common.Comment;

import com.tdengine4axe.annotation.Column;
import com.tdengine4axe.annotation.Id;
import com.tdengine4axe.annotation.Table;
import com.tdengine4axe.annotation.Tag;
import com.tdengine4axe.interface_.SuperTable;

//带上数据库名称
@Table(tableName="test4axe.meters", comment = "电表")
public class Meters implements SuperTable{
	@Id
	@Comment("时间")
	private Date ts;
	
	//这个字段会创建到表结构中
	@Comment("设备id")
	private Long id;
	
	@Column
	@Comment("电流")
	private float current;
	
	@Column
	@Comment("电压")
	private int voltage;
	
	@Tag
	@Comment("区域")
	private String location;
	
	@Tag
	@Comment("所属设备组id")
	private int groupdId;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Date getTs() {
		return ts;
	}

	public void setTs(Date ts) {
		this.ts = ts;
	}

	public float getCurrent() {
		return current;
	}

	public void setCurrent(float current) {
		this.current = current;
	}

	public int getVoltage() {
		return voltage;
	}

	public void setVoltage(int voltage) {
		this.voltage = voltage;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public int getGroupdId() {
		return groupdId;
	}

	public void setGroupdId(int groupdId) {
		this.groupdId = groupdId;
	}

	@Override
	public String subTableName() throws Exception {
		return "test4axe.device_"+id;//子表的名称
	}
	
}
