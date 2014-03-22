package com.jz100.knn;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jz100.db.JDBCUtil;


public class LoadDataSet {
	
	public static void main(String[] args) throws Exception {
		
		
		
		//获取数据库名
		String sql = "SELECT DATABASE() AS name";
		JDBCUtil ju = new JDBCUtil();
		String prefix = ju.getPrefix();
		ResultSet rs = ju.getSingleResult(sql);
		String dbname = null;
		rs.next();
		dbname = rs.getString("name");
		//获取所有post分表
		List<String> postTables = new ArrayList<String>();
		sql = "SHOW TABLES WHERE Tables_in_"+dbname+" REGEXP '^pre_forum_post[_]{0,1}[0-9]*$'";
		rs = ju.getResultSet(sql);
		while(rs.next()) {
			postTables.add(rs.getString("Tables_in_"+dbname));
		}
		
		//取出用户的统计数据
		Map<String, String> userMap = new HashMap<String, String>();
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT a.credits,")
			.append("b.extcredits1,b.extcredits2,b.extcredits3,b.extcredits4,b.extcredits5,b.extcredits6,b.extcredits7,b.extcredits8,b.extcredits9,")
			.append("b.friends,b.posts,b.threads,b.digestposts,b.blogs,b.attachsize,b.views,b.oltime,b.feeds,b.follower,b.following,b.newfollower,")
			.append("b.doings,b.albums,b.sharings,")
			.append("a.uid,a.avatarstatus,a.regdate,a.groupid,a.adminid,")
			.append("c.medals,d.regip,d.lastip,d.lastvisit,d.lastactivity,d.lastpost,d.profileprogress ")
			.append("FROM "+prefix+"common_member AS a ")
			.append("LEFT JOIN "+prefix+"common_member_count AS b ON a.uid=b.uid ")
			.append("LEFT JOIN "+prefix+"common_member_field_forum AS c ON a.uid=c.uid ")
			.append("LEFT JOIN "+prefix+"common_member_status AS d ON a.uid=d.uid ")
			.append("WHERE 1 ORDER BY a.uid ASC LIMIT 5");
		sql = sb.toString();
		rs = ju.getResultSet(sql);
		Map<String,String> resultMap = ju.getResultMap();
		System.out.println(resultMap);
		/*
		while(rs.next()) {
			for (int i = 1; i < columnCount + 1; i++ ) {
				userMap.put(columnNames[i-1], rs.getString(i));
			}
			
			
			
			
			
			
			
			System.out.println(userMap);
			
			
			
			
			
			
			
			
			
			
			
			
			
			
		}*/
		
		
		ju.closed();
	}
	
	
	
	
	
}
