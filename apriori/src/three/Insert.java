package three;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Insert {

	/**
	 * @param args
	 */
	// JDBC DRIVER and DB
	static final String DRIVER = "com.mysql.cj.jdbc.Driver";
	static final String DB = "jdbc:mysql://localhost/freq_item";
	// Database auth
	static final String USER = "user1";
	static final String PASSWD = "1";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// 加载驱动程序
			Class.forName(DRIVER);
			System.out.println("Connecting to a selected database...");
			// 打开一个连接
			conn = DriverManager.getConnection(DB, USER, PASSWD);
			// 执行一个查询
			stmt = conn.createStatement();
			String sql = null;
			// 获得结果集
			
			String testFile = "/home/hadoop/rate";
			FileInputStream in = new FileInputStream(testFile);
			BufferedReader d = new BufferedReader(new InputStreamReader(in));
			String line = null;
			
			while ((line = d.readLine()) != null) {
				
				String[] parts = line.split("\t");
				
				float num = (float)Double.parseDouble(parts[1]);
				
				sql = "update `BX-Books` set Rate = " + num + " where ISBN = '" + parts[0] + "';";
				
				stmt.executeUpdate(sql);
			}
			
			System.out.println("Success!");

			d.close();
			in.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
}
