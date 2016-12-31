package com.rvalenz.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.commons.dbutils.*;

/**
 * Purpose: This is a database utility class intended to simplify connections and queries
 * 
 * @author Robert Valenzuela
 * @version 1.0 
 */

public class DbConnector {
	 private String url;      // Example: "jdbc:mysql://localhost:3306/test";
	 private String driver;   // Example: "com.mysql.jdbc.Driver";
     private String user;     // Example: "root";
     private String password; // Example: "password";
     
 	/**
	 *  Designate how ResultSets are handled.
	 *  In this implementation the ResultSet is a two dimensional ArrayList
	 */
     private final ResultSetHandler<ArrayList<ArrayList<Object>>> handler = new ResultSetHandler<ArrayList<ArrayList<Object>>>(){ 		
  	
  		@Override
  		public ArrayList<ArrayList<Object>> handle(ResultSet rs) throws SQLException {
  			
  			if (!rs.next()){
  			return null;
  			}  			
  			ResultSetMetaData meta = rs.getMetaData();
  			int cols = meta.getColumnCount();
  			ArrayList<ArrayList<Object>> results = new ArrayList<ArrayList <Object>>(); 			
  			do{
  				ArrayList<Object> row = new ArrayList<Object>();
  				for(int i=0; i<cols; i++){					
  			         row.add(rs.getObject(i + 1));
  			        }
  				results.add(row);
  				
  			  }while(rs.next());
  			return results;
  		}
  	};
/**
 *  Empty Constructor
 */
public DbConnector(){
	
}
     
/**
 * @param url
 * @param driver
 * @param user
 * @param password
 */
public DbConnector (String url, String driver, String user, String password){
	this.url = url;
	this.driver = driver;
	this.user = user;
	this.password = password;
	
}
/**
 * Accessors and Mutators 
 */
public String getUrl() {
	return url;
}

public void setUrl(String url) {
	this.url = url;
}

public String getDriver() {
	return driver;
}

public void setDriver(String driver) {
	this.driver = driver;
}

public String getUser() {
	return user;
}

public void setUser(String user) {
	this.user = user;
}

public String getPassword() {
	return password;
}

public void setPassword(String password) {
	this.password = password;
}


/**
 * This method parses the user inputted SQL with no replacement parameters and executes the appropriate action on the designated database
 * @param statement
 */
public void execute(String statement) {
	DbUtils.loadDriver(driver);
	String cleaned = statement.toLowerCase().trim();
	String[] type = cleaned.split(" ");
	Connection conn = null;
	ArrayList<ArrayList<Object>> result = null;
	switch(type[0]){
	
	case "select":
		
		
		try{
		conn = DriverManager.getConnection(url, user, password);
		QueryRunner run = new QueryRunner();
		result = run.query(conn,  statement , handler);
		for(ArrayList<Object> innerList : result) {
		    for(Object col : innerList) {
		        System.out.print(col + "\t");
		    }
		    System.out.println();
		}
		}catch (SQLException e) {
			e.printStackTrace();
			return;
		}finally{
			try {
				DbUtils.close(conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		

		break;
	case "insert":
		
		try{
		conn = DriverManager.getConnection(url, user, password);
		QueryRunner run = new QueryRunner();
	    result = run.insert(conn,  statement , handler);
		System.out.println("Row:" +result + "added");
		}catch (SQLException e) {
			e.printStackTrace();
			return;
		}finally{
			try {
				DbUtils.close(conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		break;
	case "update":
		
		try{
		conn = DriverManager.getConnection(url, user, password);
		QueryRunner run = new QueryRunner();
		int numberUpdated = run.update(conn,  statement);
		System.out.println(numberUpdated + " row(s) updated");
		}catch (SQLException e) {
			e.printStackTrace();
			return;
		}finally{
			try {
				DbUtils.close(conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		break;
	case "delete":
	
		try{
			conn = DriverManager.getConnection(url, user, password);
			QueryRunner run = new QueryRunner();
			int numberUpdated = run.update(conn,  statement);
			System.out.println(numberUpdated + " row(s) deleted");
			}catch (SQLException e) {
				e.printStackTrace();
				return;
			}finally{
				try {
					DbUtils.close(conn);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		break;
	
	}
	
}
/**
 * This method parses the user inputed SQL and replacement parameters and executes the appropriate action on the designated database
 * @param statement
 * @param replacement params
 */
public void execute(String statement, Object...params) {
	DbUtils.loadDriver(driver);
	String cleaned = statement.toLowerCase().trim();
	String[] type = cleaned.split(" ");
	Connection conn = null;
	ArrayList<ArrayList<Object>> result = null;
	switch(type[0]){
	case "select":
		
		try{
		conn = DriverManager.getConnection(url, user, password);
		QueryRunner run = new QueryRunner();
		result = run.query(conn,  statement , handler,params);
		for(ArrayList<Object> innerList : result) {
		    for(Object col : innerList) {
		        System.out.print(col + "\t");
		    }
		    System.out.println();
		}
		}catch (SQLException e) {
			e.printStackTrace();
			return;
		}finally{
			try {
				DbUtils.close(conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}

		break;
	case "insert":
		
		try{
		conn = DriverManager.getConnection(url, user, password);
		QueryRunner run = new QueryRunner();
	    result = run.insert(conn,  statement , handler,params);
		System.out.println("Row:" +result + "added");
		}catch (SQLException e) {
			e.printStackTrace();
			return;
		}finally{
			try {
				DbUtils.close(conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		break;
	case "update":
		
		try{
		conn = DriverManager.getConnection(url, user, password);
		QueryRunner run = new QueryRunner();
		int numberUpdated = run.update(conn,  statement,params);
		System.out.println(numberUpdated + " row(s) updated");
		}catch (SQLException e) {
			e.printStackTrace();
			return;
		}finally{
			try {
				DbUtils.close(conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		break;
	case "delete":
	
		try{
			conn = DriverManager.getConnection(url, user, password);
			QueryRunner run = new QueryRunner();
			int numberUpdated = run.update(conn,  statement,params);
			System.out.println(numberUpdated + " row(s) deleted");
			}catch (SQLException e) {
				e.printStackTrace();
				return;
			}finally{
				try {
					DbUtils.close(conn);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		break;
	
	}
	
}

}
	
	