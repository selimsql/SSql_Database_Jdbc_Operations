package selimsql.test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DmlPreparedOpSSqlDb {

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

	protected int preparedStatementExecute(PreparedStatement preparedStatement, Object[] colValues) {
		try {
			int lenCol = (colValues == null ? 0 : colValues.length);
			for(int i=0; i < lenCol; i++) {
				preparedStatement.setObject(i + 1, colValues[i]);
			}

			int number = preparedStatement.executeUpdate();
			System.out.println("-PreparedStatement executed values: " + number);
			System.out.println();

			return number;
		}
		catch(SQLException ex) {
			System.out.println("Error:" + ex.getMessage());
			return 0;
		}
	}

	private byte[] readFileBinaryAsBytes(String fileFullName)
	{
		FileInputStream fileInputStream = null;
		ByteArrayOutputStream baos = null;
		try
		{
			fileInputStream = new FileInputStream(fileFullName);
			baos = new ByteArrayOutputStream();

			int sizeTemp;
			byte[] bytesTemp = new byte[1000];
			while((sizeTemp = fileInputStream.read(bytesTemp)) != -1) {
				baos.write(bytesTemp, 0, sizeTemp);
			}

			return baos.toByteArray();
		}
		catch(Exception ex) {
			System.out.println("ReadFileError:" + ex.getMessage());
			return null;
		}
		finally
		{
			try {
				if (fileInputStream != null) fileInputStream.close();
				if (baos != null) baos.close();
			}
			catch(IOException ex) {
				System.out.println("Error:" + ex.getMessage());
			}
		}
	}

	public boolean insertUpdateRecords(Connection connection) throws SQLException {
		System.out.println("-SelimSql insertUpdate operations ------");
		PreparedStatement preparedStatement = null;
		try {
			connection.setAutoCommit(false);
			System.out.println("connection.AutoCommit:false");

			String sql = "Insert into Product(Id, Name, Price, ProductDate, Status, Comment, Photo) VALUES(?, ?, ?, ?, ?, ?, ?)";
			preparedStatement = connection.prepareStatement(sql);
			//statement.setMaxRows(100);
			System.out.println("connection.preparedStatement(sql:" + sql + ") okay.");
			System.out.println();

			java.util.Date dateToday = new java.util.Date();
			Object[] recArray = new Object[]{new Object[]{new Integer(11), "Product11", new Double(22.35), dateToday, "A", null, null},
											 new Object[]{new Integer(12), "Product12", new Double(32.68), dateToday, "P", null, null},
											 new Object[]{new Integer(13), "Product13", new Double(97), dateToday, "A", null, null},
											};

			//Insert Product rows ---------------
			int countArray = recArray.length;
			for(int i=0; i < countArray; i++) {
				Object[] colValues = (Object[])recArray[i];
				preparedStatementExecute(preparedStatement, colValues);
			}

			connection.commit();
			System.out.println("connection.commit okay for inserts");

			//------------------
			sql = "Update Product set Comment = ?, Photo = ? where Id = ?";
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, "Update Comment Long Description..");

			byte[] imageBytes = readFileBinaryAsBytes("./Notebook.png");
			preparedStatement.setBytes(2, imageBytes);

			preparedStatement.setInt(3, 13);

			int number = preparedStatement.executeUpdate();
			System.out.println("-PreparedStatement executed for update. Affected row:" + number);
			System.out.println();

			//------------------
			connection.commit();
			System.out.println("connection.commit okay for update");
		}
		finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				System.out.println("connection.preparedStatement closed.");
			}
		}

		return (preparedStatement != null);
	}

	public static void main(String[] args) {
		DmlPreparedOpSSqlDb jdbcTest = new DmlPreparedOpSSqlDb();
		Connection dbConnection = null;
		try {
			dbConnection = jdbcTest.connectDb(SELIMSQL_DB_TEST_NAME);
			jdbcTest.insertUpdateRecords(dbConnection);
		}
		catch(SQLException e) {
			System.out.println("Error:" + e.getMessage());
		}
		finally {
			jdbcTest.closeDb(dbConnection);
		}
	}		 
}
