package selimsql.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DmlOpSSqlDb {

	private static final String SELIMSQL_JDBC_CLASS      = "selimsql.jdbc.DbDriver";
	private static final String SELIMSQL_JDBC_URL_HEADER = "jdbc:selimsql:";
	private static final String SELIMSQL_DB_TYPE_FILE    = "file";
	private static final String SELIMSQL_DB_TEST_NAME    = "TestDb";

	private static final String TESTDB_USER_ADMIN        = "admin";
	private static final String TESTDB_PASS_NONE         = null;

	public Connection connectDb(String dbName) {
		System.out.println("-SelimSql Connection Testing ------");
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
			String url = SELIMSQL_JDBC_URL_HEADER + SELIMSQL_DB_TYPE_FILE + ":" + dbName;
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

	public boolean closeDb(Connection connection) {
		System.out.println("-SelimSql closing ------");
		if (connection == null) {
			System.out.println("No connection for SelimSql");
			return false;
		}

		try {
			if (connection.isClosed()) {
				System.out.println("Connection is already closed!");
				return false;
			}

			connection.close();
			System.out.println("Connection closed.");
			return true;
		}
		catch(SQLException e) {
			return false;
		}
	}

	private int statementExecuteUpdate(Statement statement, String sql) {
		try {
			int number = statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed(number:" + number + ")");
			System.out.println(sql);
			System.out.println();
			return number;
		}
		catch(SQLException ex) {
			System.out.println("Error:" + ex.getMessage());
			return 0;
		}
	}

	public int insertUpdateDeleteRecords(Connection connection) throws SQLException {
		System.out.println("-SelimSql insertUpdateDelete operations ------");
		Statement statement = null;
		int count = 0;
		try {
			statement = connection.createStatement();
			System.out.println("connection.statement okay.");
			//statement.setMaxRows(100);

			//Insert Product rows ---------------
			String sql = "INSERT INTO Product(Id, Name, Price, ProductDate, Status, Comment, Photo)"
						+ " VALUES(1, 'Product1', 12.35, today(), 'A', null, null)";
			count += statementExecuteUpdate(statement, sql);

			//---------------------------
			sql = "INSERT INTO Product"
				+ " VALUES(2, 'Product2', 23.68, TODAY(-1), 'P', null, null)";
			count += statementExecuteUpdate(statement, sql);

			//---------------------------
			sql = "INSERT INTO Product"
				+ " VALUES(3, 'Product3', 80.79, TODAY(-2), 'A', null, null)";
			count += statementExecuteUpdate(statement, sql);

			//---------------------------
			sql = "INSERT INTO Product"
				+ " VALUES(5, 'Product5', 12.69, TODAY(), 'P', null, null)";
			count += statementExecuteUpdate(statement, sql);

			//---------------------------
			sql = "update Product"
				+ " set Comment = Name + ' Comment Description Long.'"
				+ " where Id <= 3";
			statementExecuteUpdate(statement, sql);

			//---------------------------
			sql = "update Product"
				+ " set Photo = BinaryFromFile('./Notebook.png')"
				//set Photo = BinaryFromFile('http://www.selimsql.com/imageTest/Notebook.png')"
				+ " where Id = 2";
			statementExecuteUpdate(statement, sql);

			//---------------------------
			sql = "delete from Product"
				+ " where Name = 'Product5'";
			count -= statementExecuteUpdate(statement, sql);


			//Insert Customer rows ---------------
			sql = "insert into Customer"
				+ " values(11, datefromstr('28.08.2014','dd.MM.yyyy'), 'Fatih', 'Songul', 'M', null, 'abc@yahoo.com')";
			count += statementExecuteUpdate(statement, sql);

			sql = "insert into Customer"
				+ " values(12, today(-10), 'Selim', 'Eren', 'M', null, 'selim@yahoo.com')";
			count += statementExecuteUpdate(statement, sql);

			sql = "insert into Customer"
				+ " values(13, today(-10), 'Habibe', 'G�ven', 'F', null, 'hguven@yahoo.com')";
			count += statementExecuteUpdate(statement, sql);


			//Insert Order rows  ---------------
			sql = "insert into Order"
				+ " values(101, 11, 1, today(), 2, 16.45, 'CASH', 'A')";
			count += statementExecuteUpdate(statement, sql);

			sql = "insert into Order"
				+ " values(102, 11, 2, today(-3), 3, 5, 'CREDIT', 'A')";
			count += statementExecuteUpdate(statement, sql);

			sql = "insert into Order"
				+ " values(103, 11, 3, today(-1), 1, 7.65, 'CREDIT', 'A')";
			count += statementExecuteUpdate(statement, sql);

			sql = "insert into Order"
				+ " values(104, 12, 1, today(-3), 2, 16.45, 'CASH', 'A')";
			count += statementExecuteUpdate(statement, sql);

			sql = "insert into Order"
				+ " values(105, 12, 2, today(-7), 5, 5, 'CASH', 'A')";
			count += statementExecuteUpdate(statement, sql);

			sql = "insert into Order"
				+ " values(106, 13, 3, today(-6), 6, 7.65, 'CREDIT', 'A')";
			count += statementExecuteUpdate(statement, sql);

			sql = "insert into Order"
				+ " values(107, 13, 2, today(-5), 2, 7.65, 'CASH', 'A')";
			count += statementExecuteUpdate(statement, sql);
		}
		finally {
			if (statement != null) {
				statement.close();
				System.out.println("connection.statement closed.");
			}
		}

		System.out.println("-insertUpdateDelete operations count: " + count);
		return count;
	}

	public static void main(String[] args) {
		DmlOpSSqlDb jdbcTest = new DmlOpSSqlDb();
		Connection dbConnection = null;
		try {
			dbConnection = jdbcTest.connectDb(SELIMSQL_DB_TEST_NAME);
			jdbcTest.insertUpdateDeleteRecords(dbConnection);
		}
		catch(SQLException e) {
			System.out.println("Error:" + e.getMessage());
		}
		finally {
			jdbcTest.closeDb(dbConnection);
		}
	}
}
