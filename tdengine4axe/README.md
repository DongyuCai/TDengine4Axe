1.建立普通表的POJO映射
//表字段如果需要创建到表结构里，必须有@Id、@Column、@Tag中的一个
//@Id字段有且只有一个
//*表名最好带上dbname，测试发现，如果不带，并发查询可能会报DataBase 未指定的错误，这可能是tdengine的bug
@Table(tableName="test4axe.user_log",comment = "用户操作记录")
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

2.建立超级表可以这样
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

3.写dao的sql来操作或查询数据
@Dao
public interface TestDao extends BaseRepository{

	@Sql("select * from test4axe.device_?1")
	public List<Meters> getLogList(long deviceId);
}

public static void main(){
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
}

