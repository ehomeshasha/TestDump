package com.jz100.dump;

import java.io.FileWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.jz100.db.JDBCUtil;
import com.jz100.dump.DumpUserData;
import com.jz100.util.CommonUtil;

public class DumpUserData {
	
	public static void main(String[] args) throws Exception {
		long startTime=System.currentTimeMillis();   //获取开始时间  
		DumpUserData loadDataSet = new DumpUserData();
		loadDataSet.run();
		long endTime=System.currentTimeMillis(); //获取结束时间
		
		System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
	}
	CommonUtil cu = new CommonUtil();
	private StringBuilder sb = new StringBuilder();
	private StringBuilder recordSb = new StringBuilder();
	
	public void run() throws Exception {	
		
		
		
		//设置输出路径
		FileWriter fw = new FileWriter("data.txt");
		FileWriter fw2 = new FileWriter("title.txt");
		sb.append("send_flower_count\t").append("send_egg_count\t").append("receive_flower_count\t").append("receive_egg_count\t")
			.append("thread_length\t").append("reply_length\t").append("thread_rate\t")
			.append("thread_views\t").append("thread_replies\t").append("thread_highlight\t")
			.append("thread_digest\t").append("thread_recommends\t").append("thread_heats\t")
			.append("thread_favtimes\t").append("thread_sharetimes\t")
			.append("postcomment\t")
			.append("creditsCommonUtil\t").append("extcredits1\t").append("extcredits2\t").append("extcredits3\t").append("extcredits4\t").append("extcredits5\t").append("extcredits6\t").append("extcredits7\t").append("extcredits8\t").append("extcredits9\t")
			.append("friends\t").append("posts\t").append("threads\t").append("digestposts\t").append("blogs\t")
			.append("attachsize\t").append("views\t").append("oltime\t")
			.append("feeds\t").append("follower\t").append("following\t").append("newfollower\t")
			.append("doings\t").append("albums\t").append("sharings\t")
			.append("uid\t").append("avatarstatus\t").append("regdate\t").append("groupid\t").append("adminid\t")
			.append("medals\t").append("regip\t").append("lastip\t")
			.append("lastvisit\t").append("lastactivity\t").append("lastpost\t").append("profileprogress");
		fw2.write(sb.toString());
		fw2.close();
		
		//获取数据库名
		String sql = "SELECT DATABASE() AS name";
		JDBCUtil ju = new JDBCUtil();
		String prefix = ju.getPrefix();
		ResultSet rs = ju.getSingleResult(sql);
		String dbname = null;
		rs.next();
		dbname = rs.getString("name");
		rs.close();
		ju.getStmt().close();
		
		//获取所有post分表
		List<String> postTables = new ArrayList<String>();
		sql = "SHOW TABLES WHERE Tables_in_"+dbname+" REGEXP '^pre_forum_post[_]{0,1}[0-9]*$'";
		rs = ju.getResultSet(sql);
		while(rs.next()) {
			postTables.add(rs.getString("Tables_in_"+dbname));
		}
		rs.close();
		ju.getStmt().close();
		
		//取出用户的统计数据
		sb.setLength(0);
		sb.append("SELECT a.credits,")
			.append("b.extcredits1,b.extcredits2,b.extcredits3,b.extcredits4,b.extcredits5,b.extcredits6,b.extcredits7,b.extcredits8,b.extcredits9,")
			.append("b.friends,b.posts,b.threads,b.digestposts,b.blogs,b.attachsize,b.views,b.oltime,b.feeds,b.follower,b.following,b.newfollower,")
			.append("b.doings,b.albums,b.sharings,")
			.append("a.uid,a.avatarstatus,a.regdate,a.groupid,a.adminid,")
			.append("c.medals,d.regip,d.lastip,d.lastvisit,d.lastactivity,d.lastpost,d.profileprogress ")
			.append("FROM ").append(prefix).append("common_member AS a ")
			.append("LEFT JOIN ").append(prefix).append("common_member_count AS b ON a.uid=b.uid ")
			.append("LEFT JOIN ").append(prefix).append("common_member_field_forum AS c ON a.uid=c.uid ")
			.append("LEFT JOIN ").append(prefix).append("common_member_status AS d ON a.uid=d.uid ")
			.append("WHERE 1 ORDER BY a.uid ASC");
		sql = sb.toString();
		rs = ju.getResultSet(sql, true);
		String[] columnNameArray = ju.getColumnNameArray();
		String columnName = null;
		String value = null;
		
		String where = null;
		ResultSet rs2;
		int columnCount2;
		String line = null;
		while(rs.next()) {
			List<String> record = new ArrayList<String>();
			recordSb.setLength(0);
			where = recordSb.append("authorid='").append(rs.getString("uid")).append("'").toString();
			
			//关联表获取额外统计数据
			//送出鲜花数,鸡蛋数,收到鲜花数,鸡蛋数
			recordSb.setLength(0);
			recordSb.append("SELECT AVG(num) AS send_flower_count FROM ").append(prefix).append("common_plugin_fegglog WHERE fromuid='").append(rs.getString("uid")).append("' AND `type`=0");
			rs2 = ju.getSingleResult(recordSb.toString());
			rs2.next();
			record.add(cu.setEmpty2Zero(rs2.getString(1)));
			rs2.close();
			ju.getStmt().close();
			
			recordSb.setLength(0);
			recordSb.append("SELECT AVG(num) AS send_egg_count FROM ").append(prefix).append("common_plugin_fegglog WHERE fromuid='").append(rs.getString("uid")).append("' AND `type`=1");
			rs2 = ju.getSingleResult(recordSb.toString());
			rs2.next();
			record.add(cu.setEmpty2Zero(rs2.getString(1)));
			rs2.close();
			ju.getStmt().close();
			
			recordSb.setLength(0);
			recordSb.append("SELECT AVG(num) AS receive_flower_count FROM ").append(prefix).append("common_plugin_fegglog WHERE touid='").append(rs.getString("uid")).append("' AND `type`=0");
			rs2 = ju.getSingleResult(recordSb.toString());
			rs2.next();
			record.add(cu.setEmpty2Zero(rs2.getString(1)));
			rs2.close();
			ju.getStmt().close();
			
			recordSb.setLength(0);
			recordSb.append("SELECT AVG(num) AS receive_egg_count FROM ").append(prefix).append("common_plugin_fegglog WHERE touid='").append(rs.getString("uid")).append("' AND `type`=1");
			rs2 = ju.getSingleResult(recordSb.toString());
			rs2.next();
			record.add(cu.setEmpty2Zero(rs2.getString(1)));
			rs2.close();
			ju.getStmt().close();
			
			double thread_length = 0, reply_length = 0, thread_rate = 0; 
			for(String postTable : postTables) {
				//主题字数
				recordSb.setLength(0);
				recordSb.append("SELECT ")
					.append("AVG(CHAR_LENGTH(TRIM(subject))+CHAR_LENGTH(TRIM(message))) AS thread_length ")
					.append("FROM ").append(postTable)
					.append(" WHERE ").append(where).append(" AND position=1");
				rs2 = ju.getSingleResult(recordSb.toString());
				rs2.next();
				thread_length += rs2.getDouble(1);
				rs2.close();
				ju.getStmt().close();
				
				//回复字数
				recordSb.setLength(0);
				recordSb.append("SELECT ")
				.append("AVG(CHAR_LENGTH(TRIM(subject))+CHAR_LENGTH(TRIM(message))) AS reply_length ")
				.append("FROM ").append(postTable)
				.append(" WHERE ").append(where).append(" AND position!=1");
				rs2 = ju.getSingleResult(recordSb.toString());
				rs2.next();
				reply_length += rs2.getDouble(1);
				rs2.close();
				ju.getStmt().close();
				
				//评分值
				recordSb.setLength(0);
				recordSb.append("SELECT ")
				.append("AVG(rate/ratetimes) AS thread_rate  ")
				.append("FROM ").append(postTable)
				.append(" WHERE ").append(where);
				rs2 = ju.getSingleResult(recordSb.toString());
				rs2.next();
				thread_rate += rs2.getDouble(1);
				rs2.close();
				ju.getStmt().close();
				
			}
			record.add(String.valueOf(thread_length));
			record.add(String.valueOf(reply_length));
			record.add(String.valueOf(thread_rate));
			
			
			//主题统计信息,排除特殊主题
			recordSb.setLength(0);
			recordSb.append("SELECT ")
				.append("AVG(views) AS thread_views,")
				.append("AVG(replies) AS thread_replies,")
				.append("AVG(highlight) AS thread_highlight,")
				.append("AVG(digest) AS thread_digest,")
				.append("AVG(recommends) AS thread_recommends,")
				.append("AVG(heats) AS thread_heats,")
				.append("AVG(favtimes) AS thread_favtimes,")
				.append("AVG(sharetimes) AS thread_sharetimes ")
				.append("FROM ").append(prefix).append("forum_thread ")
				.append("WHERE ").append(where).append(" AND special=0");
			rs2 = ju.getSingleResult(recordSb.toString(), true);
			rs2.next();
			columnCount2 = ju.getColumnCount();
			for(int i=1;i<columnCount2+1;i++) {
				record.add(rs2.getString(i));
			}
			rs2.close();
			ju.getStmt().close();
			
			//获取点评数
			recordSb.setLength(0);
			recordSb.append("SELECT COUNT(*) AS count FROM ").append(prefix).append("forum_postcomment WHERE ").append(where);
			rs2 = ju.getSingleResult(recordSb.toString());
			rs2.next();
			record.add(rs2.getString(1));
			rs2.close();
			ju.getStmt().close();
			
			//获取用户的统计数据
			//recordSb.setLength(0);
			for(int i=1;i<columnNameArray.length+1;i++) {
				columnName = columnNameArray[i-1];
				value = rs.getString(i);
				if(columnName.equals("medals")) {
					value = cleanMedals(value, recordSb);
				}
				if(columnName.equals("regip") || columnName.equals("lastip")) {
					value = cleanIP(value);
				}
				record.add(value);
			}
			
			
			
			
			
			
			
			recordSb.setLength(0);
			for(String r : record) {
				recordSb.append("\t").append(r);
			}
			
			line = recordSb.append("\n").toString().substring(1);
			fw.write(line);
			
			
		}
		rs.close();
		ju.getStmt().close();
		fw.close();
		ju.closed();
	}

	private String cleanIP(String value) {
		if(value == null || value.equals(CommonUtil.EMPTY_STRING) || value.equals(CommonUtil.NULL)) {
			return CommonUtil.EMPTY_STRING;
		}
		if(value.equals("hidden") || value.equals("Manual Acting")) {
			return CommonUtil.EMPTY_STRING;
		}
		return value;
	}

	private String cleanMedals(String value, StringBuilder recordSb) {
		recordSb.setLength(0);
		if(value == null || value.equals(CommonUtil.EMPTY_STRING) || value.equals(CommonUtil.NULL)) {
			return CommonUtil.EMPTY_STRING;
		}
		String[] terms = CommonUtil.EMPTY.split(value);
		for(int i=0;i<terms.length;i++) {
			recordSb.append(",").append(CommonUtil.SHU.split(terms[i])[0]);
		}
		return recordSb.toString().substring(1);
	}
	
	
	
	
	
}
