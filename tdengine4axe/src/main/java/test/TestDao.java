package test;

import java.util.List;

import com.tdengine4axe.annotation.Dao;
import com.tdengine4axe.annotation.Sql;
import com.tdengine4axe.interface_.BaseRepository;

@Dao
public interface TestDao extends BaseRepository{

	@Sql("select * from device_?1")
	public List<Meters> getLogList(long deviceId);
}
