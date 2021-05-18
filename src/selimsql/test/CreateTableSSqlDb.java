package selimsql.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateTableSSqlDb {

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

	public boolean createTablesAndIndexes(Connection connection) throws SQLException {
		System.out.println("-SelimSql createTablesAndIndexes ------");
		Statement statement = null;
		try {
			statement = connection.createStatement();
			System.out.println("connection.statement okay.");

			String sql = "CREATE TABLE Product"
						+ " (Id INTEGER NOT NULL,"
						+ " Name VARCHAR(20) NOT NULL,"
						+ " Price DECIMAL(15, 2) NOT NULL,"
						+ " ProductDate DATE NOT NULL,"
						+ " Status CHAR NOT NULL,"
						+ " Comment LONGTEXT,"
						+ " Photo BLOB)";

			//statement.executeUpdate(sql);
			statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed:");
			System.out.println(sql);
			System.out.println();

			sql = "CREATE UNIQUE INDEX ProductPk ON Product(Id)";
			statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed:");
			System.out.println(sql);
			System.out.println();


			sql = "Create Table Customer"
				+ " (Id INTEGER NOT NULL,"
				+ " EntryDate DATE NOT NULL,"
				+ " Name VARCHAR(20) NOT NULL,"
				+ " Surname VARCHAR(20) NOT NULL,"
				+ " Sex CHAR NOT NULL,"
				+ " Job VARCHAR(30),"
				+ " Email VARCHAR(30))";
			statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed:");
			System.out.println(sql);
			System.out.println();

			sql = "CREATE UNIQUE INDEX CustomerPk ON Customer(Id)";
			statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed:");
			System.out.println(sql);
			System.out.println();

			sql = "CREATE INDEX CustomerNameIdx ON Customer(Name, Surname)";
			statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed:");
			System.out.println(sql);
			System.out.println();


			sql = "Create Table Order"
				+ " (Id INTEGER NOT NULL,"
				+ " CustomerId INTEGER NOT NULL,"
				+ " ProductId INTEGER NOT NULL,"
				+ " OrderDate DATE NOT NULL,"
				+ " OrderAmount SMALLINT NOT NULL,"
				+ " UnitPrice DECIMAL(15, 2) NOT NULL,"
				+ " PaymentType VARCHAR(10) NOT NULL,"
				+ " Status VARCHAR(10) NOT NULL)";
			statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed:");
			System.out.println(sql);
			System.out.println();

			sql = "CREATE UNIQUE INDEX OrderPk ON Order(Id)";
			statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed:");
			System.out.println(sql);
			System.out.println();

			sql = "CREATE INDEX OrderCusIdx ON Order(CustomerId)";
			statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed:");
			System.out.println(sql);
			System.out.println();

			sql = "CREATE INDEX OrderDateIdx ON Order(OrderDate)";
			statement.executeUpdate(sql);
			System.out.println("-Statement Sql executed:");
			System.out.println(sql);
			System.out.println();
		}
		finally {
			if (statement != null) {
				statement.close();
				System.out.println("connection.statement closed.");
			}
		}
		return (statement != null);
	}

	public static void main(String[] args) {
		CreateTableSSqlDb jdbcTest = new CreateTableSSqlDb();
		Connection dbConnection = null;
		try {
			dbConnection = jdbcTest.connectDb(SELIMSQL_DB_TEST_NAME);
			jdbcTest.createTablesAndIndexes(dbConnection);
		}
		catch(SQLException e) {
			System.out.println("Error:" + e.getMessage());
		}
		finally {
			jdbcTest.closeDb(dbConnection);
		}
	}		 
}
