package test;

import java.util.Date;

import org.axe.annotation.common.Comment;

import com.tdengine4axe.annotation.Column;
import com.tdengine4axe.annotation.Id;
import com.tdengine4axe.annotation.Table;

//表字段如果需要创建到表结构里，必须有@Id、@Column、@Tag中的一个
//@Id字段有且只有一个
@Table(tableName="user_log",comment = "用户操作记录")
public class UserLog{
	//表结构里不会有这个字段
	private Long userId;
	
	@Id //必须标注主键，必须用在时间上
	@Comment("记录产生时间") //注释虽然不会创建到表结构，但是对结构可视化还是有好处
	private Date createTime;
	
	@Column//普通列字段
	@Comment("记录内容")
	private String log;
	
	public Long getUserId() {
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public String getLog() {
		return log;
	}

	public void setLog(String log) {
		this.log = log;
	}
}
