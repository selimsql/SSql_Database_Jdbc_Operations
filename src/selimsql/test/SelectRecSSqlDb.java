package selimsql.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SelectRecSSqlDb {

	private static final String SELIMSQL_JDBC_CLASS      = "selimsql.jdbc.DbDriver";
	private static final String SELIMSQL_JDBC_URL_HEADER = "jdbc:selimsql:";
	private static final String SELIMSQL_DB_TYPE_FILE    = "file";
	protected static final String SELIMSQL_DB_TEST_NAME  = "TestDb";

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

	private boolean writeFileBinary(String fileFullName, InputStream inputStream)
	{
		if (fileFullName == null || inputStream == null)
			return false;

		FileOutputStream fileOutputStream = null;
		try
		{
			fileOutputStream = new FileOutputStream(fileFullName);

			int lenRead;
			byte[] byteBuf = new byte[1000];
			while((lenRead = inputStream.read(byteBuf)) != -1) {
				fileOutputStream.write(byteBuf, 0, lenRead);
			}

			return true;
		}
		catch(Exception ex) {
			System.out.println("WriteFileError:" + ex.getMessage());
			return false;
		}
		finally
		{
			try {
				if (inputStream != null) inputStream.close();
				if (fileOutputStream != null) fileOutputStream.close();
			}
			catch(IOException ex) {
				System.out.println("Error:" + ex.getMessage());
			}
		}
	}


	public int selectProductRecords(Connection connection) throws SQLException {
		System.out.println();
		System.out.println("-SelimSql selectProductRecords operations ------");
		Statement statement = null;
		int row = 0;
		try {
			statement = connection.createStatement();
			System.out.println("connection.statement okay.");

			String sql = "select * from Product where Id > 0";
			ResultSet resultSet = statement.executeQuery(sql);
			while(resultSet.next())
			{
				row++;
				int recId = resultSet.getInt("Id");
				System.out.print(row + ". ProductId:" + recId);
				System.out.print(", Name:" + resultSet.getString("Name"));
				System.out.print(", Price:" + resultSet.getBigDecimal("Price"));
				System.out.print(", ProductDate:" + resultSet.getDate("ProductDate"));
				System.out.print(", Status:" + resultSet.getString("Status"));
				System.out.print(", Comment:" + resultSet.getString("Comment"));

				InputStream inputStreamPhoto = resultSet.getBinaryStream("Photo");
				System.out.println(", Photo:" + inputStreamPhoto);

				String fileFullName = "./MyPhoto" + recId + ".png";
				boolean status = writeFileBinary(fileFullName, inputStreamPhoto);
				if (status)
					System.out.println("Write file:" + fileFullName + " okay");

				System.out.println();
			}
		}
		finally {
			if (statement != null) {
				statement.close();
				System.out.println("connection.statement closed.");
			}
		}
		return row;
	}//select_ProductRecords

	public int selectOrderRecords(Connection connection) throws SQLException {
		System.out.println();
		System.out.println("-SelimSql selectOrderRecords operations ------");
		Statement statement = null;
		int row = 0;
		try {
			statement = connection.createStatement();
			System.out.println("connection.statement okay.");

			String sql = "select * from Order where OrderAmount>=3 and Status='A'";
			ResultSet resultSet = statement.executeQuery(sql);
			while(resultSet.next())
			{
				row++;
				System.out.print(row + ". OrderId:" + resultSet.getInt(1));
				System.out.print(", CustomerId:" + resultSet.getInt(2));
				System.out.print(", ProductId:" + resultSet.getInt(3));
				System.out.print(", OrderDate:" + resultSet.getDate(4));
				System.out.print(", OrderAmount:" + resultSet.getShort(5));
				System.out.print(", UnitPrice:" + resultSet.getDouble(6));
				System.out.println(", PaymentType:" + resultSet.getString(7));
				System.out.println();
			}
		}
		finally {
			if (statement != null) {
				statement.close();
				System.out.println("connection.statement closed.");
			}
		}
		return row;
	}//select_OrderRecords

	public static void main(String[] args) {
		SelectRecSSqlDb jdbcTest = new SelectRecSSqlDb();
		Connection dbConnection = null;
		try {
			dbConnection = jdbcTest.connectDb(SELIMSQL_DB_TEST_NAME);
			jdbcTest.selectProductRecords(dbConnection);
			jdbcTest.selectOrderRecords(dbConnection);
		}
		catch(SQLException e) {
			System.out.println("Error:" + e.getMessage());
		}
		finally {
			jdbcTest.closeDb(dbConnection);
		}
	}		 
}
