package com.jz100.knn;

import java.sql.ResultSet;

import com.jz100.db.JDBCUtil;

public class LoadDataSet {
	
	public static void main(String[] args) throws Exception {
		String sql = "SELECT DATABASE() AS name";
		JDBCUtil ju = new JDBCUtil();
		ResultSet rs = ju.getSingleResult(sql);
		String dbname = null;
		rs.next();
		dbname = rs.getString("name");
		
		
		
		
		
		

	}
	
	
	
	
	
}
