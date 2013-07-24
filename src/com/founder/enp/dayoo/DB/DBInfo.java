/**
 * 
 */
package com.founder.enp.dayoo.DB;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author huangwenjie
 *
 */
public class DBInfo {
	/**
	 * 存放每个表的主健,<(String)tablename,(ArrayList)primarykey>
	 */
	public static HashMap<String, ArrayList<String>> pkmap = new HashMap<String, ArrayList<String>>();
	
	/**
	 * 导一个表可用的最大连接数
	 */
	private static int conlimit = 15;
	private static int recordlimit = 3000;

	private static String JDBC = "jdbc:oracle:thin:@";
	private static String fromip = "";
	private static String fromport = "";
	private static String fromservicename = "";
	private static String fromusernmae = "";
	private static String frompwd = "";
	

	private static String toip = "";
	private static String toport = "";
	private static String toservicename = "";
	private static String tousernmae = "";
	private static String topwd = "";
	
	public static long totaldatacount = 0;

	private static String getURL(String ip,String port,String sid){
		return JDBC + ip + ":" + port + ":" + sid;
	}
	
	public static String getFromDb(){
		return getURL(fromip, fromport, fromservicename);		
	}
	
	public static String getToDb(){
		return getURL(toip,toport,toservicename);		
	}
	
	public static String getFromUsername(){
		return fromusernmae;
	}
	
	public static String getToUsername(){
		return tousernmae;
	}
	
	public static String getFromPwd(){
		return frompwd;
	}
	
	public static String getToPwd(){
		return topwd;
	}

	public static void setFromip(String fromip) {
		DBInfo.fromip = fromip;
	}

	public static void setFromport(String fromport) {
		DBInfo.fromport = fromport;
	}

	public static void setFromservicename(String fromservicename) {
		DBInfo.fromservicename = fromservicename;
	}

	public static void setFromusernmae(String fromusernmae) {
		DBInfo.fromusernmae = fromusernmae;
	}

	public static void setFrompwd(String frompwd) {
		DBInfo.frompwd = frompwd;
	}

	public static void setToip(String toip) {
		DBInfo.toip = toip;
	}

	public static void setToport(String toport) {
		DBInfo.toport = toport;
	}

	public static void setToservicename(String toservicename) {
		DBInfo.toservicename = toservicename;
	}

	public static void setTousernmae(String tousernmae) {
		DBInfo.tousernmae = tousernmae;
	}

	public static void setTopwd(String topwd) {
		DBInfo.topwd = topwd;
	}
	
	public static void setConlimit(int limit){
		conlimit = limit;
	}
	
	public static int getConlimit(){
		return conlimit;
	}

	public static void setRecordlimit(int limit){
		recordlimit = limit;
	}
	
	public static int getRecordlimit(){
		return recordlimit;
	}
}
