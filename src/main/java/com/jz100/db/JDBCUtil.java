package com.jz100.db;

import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.ResultSet;  
import java.sql.ResultSetMetaData;
import java.sql.SQLException;  
import java.sql.Statement;  
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jz100.util.Props;
/**
 * JDBC封装类
 * 
 * @author fish, zzy
 */
public class JDBCUtil {

	private String propsfilename = "mysql.connection.props";
	private String className = "com.mysql.jdbc.Driver";
	
	private ResultSet rs;
	private Statement stmt;
	private Connection conn;
	private String url;
	private String username;
	private String password;
	private String prefix = "";

	private boolean isSetVariables = false;
	private ResultSetMetaData rsmd;
	private int columnCount;
	String[] columnNameArray;
	
	private void setVariables() {
		setRsmd();
		setColumnCount();
		setColumnNameArray();
	}
	
	private void setRsmd() {
		try {
			rsmd = rs.getMetaData();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void setColumnCount() {
		try {
			columnCount = rsmd.getColumnCount();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void setColumnNameArray() {
		columnNameArray = new String[columnCount];
		for (int i = 1; i < columnCount + 1; i++ ) {
			try {
				columnNameArray[i-1] = rsmd.getColumnName(i);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public ResultSetMetaData getRsmd() {
		return rsmd;
	}

	public int getColumnCount() {
		return columnCount;
	}
	
	public String[] getColumnNameArray() {
		return columnNameArray;
	}
	
	public List<Map<String, String>> returnResultArray() {
		if(!isSetVariables) {	//如果类还没有set返回记录的meta信息,那么执行seVariables进行set
			setVariables();
		}
		List<Map<String, String>> resultArray = new ArrayList<Map<String, String>>();
		try {
			while(rs.next()) {
				Map<String, String> resultMap = new HashMap<String, String>();
				for (int i = 1; i < columnCount + 1; i++ ) {
					resultMap.put(columnNameArray[i-1], rs.getString(i));
				}
				resultArray.add(resultMap);
				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return resultArray;
	}
	
	


	/**
	 * 构造函数
	 */
	public JDBCUtil() {
		try {
			Class.forName(className);
			
			Props props = new Props(propsfilename);
			Map<String, String> propMap = props.getProps();
			url = propMap.get("url");
			username = propMap.get("username");
			password = propMap.get("password");
			if(propMap.containsKey("prefix")) {
				prefix = propMap.get("prefix");
			}
			 
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	

	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * 创建数据库连接
	 */
	
	public Connection getConn() {
		try {
			conn = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * 获取Statement记录
	 */
	public Statement getStmt() {
		try {
			conn = getConn();
			stmt = conn.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return stmt;
	}

	/**
	 * 调法上面的方法，查询数据库，返回单个结果 其他类调用过程如下： 
	 * JDBCUtil ju=new JDBCUtil(); 
	 * ResultSet rs=ju.getSingleResult(sql);
	 * while(rs.next()){
	 * 		String s1 = rs.getInt(1); 
	 * }
	 */
	public ResultSet getSingleResult(String sql) {
		if (sql == null)
			sql = "";
		try {
			stmt = getStmt();
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}
	public ResultSet getSingleResult(String sql, boolean flag) {
		if (sql == null)
			sql = "";
		try {
			stmt = getStmt();
			rs = stmt.executeQuery(sql);
			isSetVariables = flag;
			if(isSetVariables) {
				setVariables();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	/**
	 * 获取Statement记录集
	 */
	public Statement getStmed() {
		try {
			conn = getConn();
			//stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
			//		ResultSet.CONCUR_UPDATABLE);
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return stmt;
	}

	/**
	 * 调法上面的方法，查询数据库，返回一个结果集 其他类调用过程如下： JDBCUtil ju=new JDBCUtil(); 
	 * ResultSet rs=ju.getResultSet(sql); 
	 * while(rs.next()){ 
	 * 		String s1 = r.getInt(1); 
	 * 		String s2 = r.getInt(2); 
	 * }
	 */
	public ResultSet getResultSet(String sql) {
		if (sql == null)
			sql = "";
		try {
			stmt = getStmed();
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}
	public ResultSet getResultSet(String sql, boolean flag) {
		if (sql == null)
			sql = "";
		try {
			stmt = getStmed();
			rs = stmt.executeQuery(sql);
			isSetVariables = flag;
			if(isSetVariables) {
				setVariables();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	/**
	 * 对数据库进行更新操作，适合SQL的insert语句和update语句 返回一个int值，表示更新的记录数 若返回为0,表示更新失败
	 * 其他类调用过程如下： 
	 * JDBCUtil ju=new JDBCUtil(); 
	 * int i=ju.update(sql); 
	 * if(i==0){
	 * 		System.out.println("failed") 
	 * } else {
	 * 		System.out.println("success")
	 * }
	 */
	public int update(String sql) {
		int flag = 0;
		if (sql == null)
			sql = "";
		try {
			stmt = getStmed();
			flag = stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			flag = 0;
		}
		return flag;
	}

	/**
	 * 删除一条数据 其他类调用过程如下： 
	 * JDBCUtil ju=new JDBCUtil(); 
	 * boolean flag = ju.delete(sql);
	 */
	public boolean delete(String sql) {
		boolean flag = false;
		if (sql == null)
			sql = "";
		try {
			stmt = getStmed();
			flag = stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return flag;
	}

	/**
	 * 插入一条数据 其他类调用过程如下： 
	 * JDBCUtil ju=new JDBCUtil(); 
	 * ju.insert(sql);
	 */
	public boolean insert(String sql) {
		boolean flag = false;
		if (sql == null)
			sql = "";
		try {
			stmt = getStmed();
			flag = stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return flag;
	}

	/**
	 * 断开数据库连接 其他类调用过程如下： 
	 * JDBCUtil ju=new JDBCUtil(); 
	 * ju.closed();
	 */
	public void closed() {
		try {
			if (rs != null)
				rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if (stmt != null)
				stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if (conn != null)
				conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}