/**
 * 
 */
package com.founder.enp.dayoo.DB;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.founder.enp.dayoo.util.FileUtils;


/**
 * @author huangwenjie
 *
 */
public class DBUtil {
	private static Log log = LogFactory.getLog(DBUtil.class);
	
	public static String SCHEMA = "CMSUSER";
	public static int OLDDB = 1;
	public static int NEWDB = 2;

	static{
		String driverClasses = "oracle.jdbc.driver.OracleDriver";
		try{Class.forName(driverClasses);}catch (Exception e){}
	}

	public static boolean valid(String name){
		Connection newcon = null;
		Connection oldcon = null;
		boolean b = false;
		try{
			newcon = ConnectionManager.getConn(DBUtil.NEWDB);;
			oldcon = ConnectionManager.getConn(DBUtil.OLDDB);
			b = valid(name,oldcon,newcon);
		}finally{
			ConnectionManager.freeConn(newcon, DBUtil.NEWDB);
			ConnectionManager.freeConn(oldcon, DBUtil.OLDDB);
		}
		return b;
	}
	/**
	 * 检查两表结构是否相同
	 * @param name
	 * @param oldcon
	 * @param newcon
	 * @return
	 */
	public static boolean valid(String name,Connection oldcon,Connection newcon){
		List<String> oldcols = new ArrayList<String>();
		List<String> newcols = new ArrayList<String>();
		Statement newst = null;
		ResultSet newrs = null;
		Statement oldst = null;
		ResultSet oldrs = null;
		try{
			String sql = "SELECT COLUMN_NAME,DATA_TYPE,DATA_LENGTH FROM USER_TAB_COLUMNS WHERE TABLE_NAME=UPPER('"+name+"')";
			
			newst = newcon.createStatement();
			newrs = newst.executeQuery(sql);
			
			oldst = oldcon.createStatement();
			oldrs = oldst.executeQuery(sql);
			while (oldrs.next()) {
				oldcols.add(oldrs.getString(1));
				oldcols.add(oldrs.getString(2));
				oldcols.add(oldrs.getString(3));
			}
			while (newrs.next()) {
				newcols.add(newrs.getString(1));
				newcols.add(newrs.getString(2));
				newcols.add(newrs.getString(3));
			}
			int count = 0;
			do{
				count = 0;
				for (int i = 0; i < oldcols.size(); i += 3) {
					String colname = (String) oldcols.get(i);
					String coltype = (String) oldcols.get(i + 1);
					String colsize = (String) oldcols.get(i + 2);
					for (int j = 0; j < newcols.size(); j += 3) {
						if (colname.equals(newcols.get(j))
								&& coltype.equals(newcols.get(j + 1))
								&& colsize.equals(newcols.get(j + 2))) {
							oldcols.remove(i + 2);
							oldcols.remove(i + 1);
							oldcols.remove(i);
							newcols.remove(j + 2);
							newcols.remove(j + 1);
							newcols.remove(j);
							count++;
							break;
						}
					}
				}
			}while(count!=0);
			
//			newcon = ConnectionManager.getConn(DBUtil.NEWDB);
//			newst = newcon.createStatement();
//			newrs = newst.executeQuery("SELECT * FROM "+name+" WHERE ROWNUM=1");
//			ResultSetMetaData newrsmd = newrs.getMetaData();
//			int newcount = newrsmd.getColumnCount();
//			
//			oldcon = ConnectionManager.getConn(DBUtil.OLDDB);
//			oldst = oldcon.createStatement();
//			oldrs = newst.executeQuery("SELECT * FROM "+name+" WHERE ROWNUM=1");
//			ResultSetMetaData oldrsmd = oldrs.getMetaData();
//			int oldcount = oldrsmd.getColumnCount();
//			if(newcount != oldcount){
//				log.info("新旧库中表 "+name+"　字段不同");
//			}
//			
//			if(oldrs.next()){
//				for(int i=0;i<oldcount;i++){
//					oldcols.add(oldrsmd.getColumnName(i+1));
//					oldcols.add(String.valueOf(oldrsmd.getColumnDisplaySize(i+1)));
//				}
//			}
//			if(newrs.next()){
//				for(int i=0;i<oldcount;i++){
//					oldcols.add(oldrsmd.getColumnName(i+1));
//					oldcols.add(String.valueOf(oldrsmd.getColumnDisplaySize(i+1)));
//				}
//			}
//			for(int i=0;i<oldcols.size();i+=2){
//				String colname = (String)oldcols.get(i);
//				String colsize = (String)oldcols.get(i+1);
//				for(int j=0;j<newcols.size();j+=2){
//					if(colname.equals(newcols.get(j)) &&
//						colsize.equals(newcols.get(j+1))){
//						oldcols.remove(i+1);
//						oldcols.remove(i);
//						newcols.remove(j+1);
//						newcols.remove(j);
//					}
//				}
//			}
		}catch(SQLException e){
			log.error("检查表"+name+"出错");
			log.fatal(e.getMessage(),e);
			return false;
		}catch (Exception e) {
			log.error("检查表结构时错误. 表:"+name);
			log.error(e.getMessage(),e);
			return false;
		}finally{
			DBUtil.close(oldrs, oldst);
			DBUtil.close(newrs, newst);
		}

		File curdir = new File("tbinfo\\diff");
		if(!curdir.isDirectory()){
			curdir.mkdirs();
		}
		File file = new File(curdir.getAbsolutePath()+File.separator+name+".txt");
		if (oldcols.size() == 0 && newcols.size() == 0) {
			//FileUtils.string2File(name+" the same table", file);
			System.out.println(" the same table : "+name);
			return true;
		} else {
			String str = "old:\n";
			str+=oldcols.toString();
			str+="\n\nnew:\n";
			str+=newcols.toString();
			FileUtils.string2File(str, file);
			System.out.println(oldcols);
			System.out.println(newcols);
			return false;
		}
		
		/*
		if(oldcols.size()==0 && newcols.size()==0){
			return true;
		}else{
			log.info("不同的字段"+oldcols);
			log.info("不同的字段"+newcols);
			return false;
		}*/
	}

	/**
	 * 检查要导入的新库中是否有这个表
	 * 有的话再
	 * @param name
	 * @param list
	 * @return
	 */
	public static boolean exist(String name,List<String> list){
		boolean r = false;
		for(int i=0;i<list.size();i++){
			if(list.get(i).equals(name)){
				r = true;
				list.remove(i);
				return r;
			}
		}
		log.info("原库中 "+name+" 表在新库中没有对应表，跳过");
		return r;
	}
	/**
	 * 
	 * @param flag 1:old 2:new
	 * @return
	 */
	public static Connection getConn(int flag) {
		
		Connection con = null;
		try {
			if(flag == OLDDB){
				con = DriverManager.getConnection(DBInfo.getFromDb(),DBInfo.getFromUsername(),DBInfo.getFromPwd()); 
			}else if(flag == NEWDB){
				con = DriverManager.getConnection(DBInfo.getToDb(),DBInfo.getToUsername(),DBInfo.getToPwd()); 
			}
		} catch (SQLException e) {
			log.info("得到连接出错");
			log.fatal(e.getMessage(),e);
		}
		return con;
	}
	
	public static void closeConn(Connection c){
		String name = null;
		try{
			if(c != null){
				name = c.getMetaData().getURL();
				c.close();
				log.info("关闭连接成功:"+name);
			}
		}catch (Exception e) {
			log.info("关闭连接出错:"+name);
		}
	}
	
	public static void close(ResultSet rs ,Statement st){
		try {
			rs.close();
		} catch (Exception e) {}
		try {
			st.close();
		} catch (Exception e) {}
	}
	
	public static List<String> getTables(Connection con){

		/*
		 * 一种方法,java的方法
		DatabaseMetaData dm = con.getMetaData();
		rs = dm.getTables(con.getCatalog(), SCHEMA, null, new String[]{"TABLE"});
        ResultSetMetaData rsmd = rs.getMetaData();
        int count = rsmd.getColumnCount();
        while(rs.next()){
//        	for(int i=0;i<count;i++){
//        		System.out.print(rs.getObject(i+1)+"--");
//        	}
//        	System.out.println();
        	String tablename = rs.getString(3);
        	if(tablename.indexOf("$")==-1){
        		tablelist.add(tablename);
        	}
        }
        */
		
		//通过oracle数据库自己的表得到
		Statement st = null;
		ResultSet tablers = null;
		List<String> list = null; 
		try {
			st = con.createStatement();
			tablers = st.executeQuery("select table_name from user_tables where table_name not like '%'||'$'||'%' ");
			list = new ArrayList<String>();
			while(tablers.next()){
				list.add(tablers.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			close(tablers, st);
		}
		return list;
	}

	/**
	 * 用getTables()方法得到所有表
	 */
	public static void expAllTables2File(List<String> tables, File file){
		FileUtils.string2File(tables.toString(), file);
	}
	
	public static void main(String args[]){
	}
}
