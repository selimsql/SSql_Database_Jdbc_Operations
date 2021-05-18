package selimsql.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class NewSSqlDb {

	private static final String SELIMSQL_JDBC_CLASS              = "selimsql.jdbc.DbDriver";
	private static final String SELIMSQL_JDBC_URL_HEADER         = "jdbc:selimsql:";
	private static final String SELIMSQL_DB_TEST_NAME            = "TestDb";
	private static final String SELIMSQL_DB_TYPE_FILENEWDATABASE = "fileNewDatabase";

	private static final String TESTDB_USER_ADMIN                = "admin";
	private static final String TESTDB_PASS_NONE                 = null;

	public Connection newDb(String dbName) {
		System.out.println("-SelimSql NewDB(" + dbName + ") Connection Testing ------");
		try {
			Class.forName(SELIMSQL_JDBC_CLASS);
		}
		catch(ClassNotFoundException e) {
			System.out.println("No driver for SelimSql?");
			return null;
		}
		System.out.println("SelimSql JDBC Driver Registered!");

		Connection con = null;
		try {
			String url = SELIMSQL_JDBC_URL_HEADER + SELIMSQL_DB_TYPE_FILENEWDATABASE + ":" + dbName;
			con = DriverManager.getConnection(url, TESTDB_USER_ADMIN, TESTDB_PASS_NONE);
		}
		catch(SQLException e) {
			System.out.println("Error while connetion:" + e.getMessage());
		}
 
		if (con != null)
			System.out.println("Connection succesfull.");
		else
			System.out.println("Connection failed!");

		return con;
	}

	public boolean closeDb(Connection con) {
		System.out.println("-SelimSql closing ------");
		if (con == null) {
			System.out.println("No connection for SelimSql");
			return false;
		}

		try {
			if (con.isClosed()) {
				System.out.println("Connection is already closed!");
				return false;
			}

			con.close();
			System.out.println("Connection closed.");
			return true;
		}
		catch(SQLException e) {
			return false;
		}
	}

	public static void main(String[] args) {
		NewSSqlDb jdbcTest = new NewSSqlDb();
		Connection con = jdbcTest.newDb(SELIMSQL_DB_TEST_NAME);
		//...
		jdbcTest.closeDb(con);
	}		 
}
