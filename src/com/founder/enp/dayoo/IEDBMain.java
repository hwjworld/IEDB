/**
 * 
 */
package com.founder.enp.dayoo;

import java.io.File;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.JDOMException;

import com.founder.enp.dayoo.DB.ConnectionManager;
import com.founder.enp.dayoo.DB.DBInfo;
import com.founder.enp.dayoo.DB.DBUtil;
import com.founder.enp.dayoo.Exception.ConfigException;
import com.founder.enp.dayoo.config.ConfigManager;
import com.founder.enp.dayoo.util.FileUtils;


/**
 * @author huangwenjie
 *z
 */
public class IEDBMain {
	
	private static final Log log = LogFactory.getLog(IEDBMain.class);
	private static ConfigManager config = null;
	
	public static int count = 0;
	/**
	 * 控制一次最多只能导5个表
	 */
	private static int limit = 5;
	private static String sql = "select count(1) from ";
	
	public static List<String> oldtables = null;
	public static List<String> newtables = null;
	public static List<String> imptables = new ArrayList<String>();
	
	public static HashMap<String, String> arguments = new HashMap<String, String>();
	public static String configxml = "";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length >= 1) {
			configxml = args[0];
			ConfigManager.setConfigPath(configxml);
		}else{
			System.out.println("parameter: [config file] [exptables [check] ] [exppk]");
			System.out.println("config:指定配置文件,如config.xml");
			System.out.println("exptables:输出表信息");
			System.out.println("  check:验证表结构(将按照目标库的表进行验证)");
			System.out.println("exppk:导出表主键信息");
			System.exit(0);
		}
		getArgs(args);
		Connection oldcon = null;
		long time1 = System.currentTimeMillis();
		try{
			config = ConfigManager.getInstance();
			
			if(arguments.containsKey("exptables")) {
					Connection newcon = null;
					oldcon = null;
					try{
						//System.getProperty("user.dir")						
						
						File curdir = new File("tbinfo");
						if(!curdir.isDirectory()){
							curdir.mkdirs();
						}
						File newinfo = new File(curdir+File.separator+"newtb.txt");
						File oldinfo = new File(curdir+File.separator+"oldtb.txt");
						newcon = DBUtil.getConn(DBUtil.NEWDB);
						oldcon = DBUtil.getConn(DBUtil.OLDDB);
						List<String> newtables = DBUtil.getTables(newcon);
						List<String> oldtables = DBUtil.getTables(oldcon);
						if(arguments.containsKey("check")){
							for(int i=0;i<newtables.size();i++){
								DBUtil.valid((String)newtables.get(i),oldcon,newcon);
							}
						}
						DBUtil.expAllTables2File(newtables,newinfo);
						DBUtil.expAllTables2File(oldtables,oldinfo);
						//复制一遍新的列表
						List<String> newnew = new ArrayList<String>();
						newnew.addAll(newtables);
						
						newtables.removeAll(oldtables);
						DBUtil.expAllTables2File(newtables,new File(curdir+File.separator+"newtbown.txt"));
						oldtables.removeAll(newnew);
						DBUtil.expAllTables2File(oldtables,new File(curdir+File.separator+"oldtbown.txt"));
						log.info("已导出表信息文件");
						log.info("tbinfo\\newtb.txt为目标库的所有表");
						log.info("tbinfo\\oldtb.txt为源库的所有表");
						log.info("tbinfo\\newtbnew.txt为目标库的相比源库特有的表");
						log.info("tbinfo\\oldtbown.txt为源库的相比目标库特有的表");
						
					}catch(Exception e){
						e.printStackTrace();
						log.info("导出表信息文件失败");
					}finally{
						DBUtil.closeConn(newcon);
						DBUtil.closeConn(oldcon);
					}
					System.exit(0);
			}
			if(arguments.containsKey("exppk")){
				Connection newcon = null;
				File curdir = new File("tbinfo");
				if(!curdir.isDirectory()){
					curdir.mkdirs();
				}
				File newinfo = new File(curdir+File.separator+"newtbpk.txt");
				File oldinfo = new File(curdir+File.separator+"oldtbpk.txt");
				try{
					newcon = DBUtil.getConn(DBUtil.NEWDB);
					oldcon = DBUtil.getConn(DBUtil.OLDDB);
					FileUtils.string2File(ExportInfo.exppk(newcon),newinfo);
					FileUtils.string2File(ExportInfo.exppk(oldcon),oldinfo);
					log.info("已导出表主键信息文件");
					log.info("tbinfo\\newtbpk.txt为目标库的主键信息");
					log.info("tbinfo\\oldtbpk.txt为源库的主键信息");
				}catch(Exception e){
					log.error("导出主键信息文件失败");
				}finally{
					DBUtil.closeConn(newcon);
					DBUtil.closeConn(oldcon);
				}
				System.exit(0);
			}
			
			
			log.info("*******************************");
			log.info("     版本:"+config.getVersion());
			log.info("  导库日期:"+new Date(System.currentTimeMillis()));
			log.info("  要导的表:"+(config.getTables().size()==0?"所有表":config.getTables().toString()));
			log.info("*******************************");
				oldcon = DBUtil.getConn(DBUtil.OLDDB);
				init(oldcon);
				long time2 = System.currentTimeMillis();
				log.info("导表准备工作完毕,花费"+(time2-time1)/1000.0+"秒");
				
				Statement st = null;
				ResultSet rs = null;
				for(int i=0;i<oldtables.size();i++){
					String tablename = (String)oldtables.get(i);
					log.info("开始导"+tablename);
					if(DBUtil.exist(tablename,newtables) && DBUtil.valid(tablename)){
						imptables.add(tablename);
						try{
							st = oldcon.createStatement();
							rs = st.executeQuery(sql+tablename);
							long num = 0;
							if(rs.next()){
								num = rs.getLong(1);
							}
							log.info(tablename+" 表数据量 "+num);
							while(true){
								if(count>limit){
									Thread.sleep(2000);
								}else{
									if(num > 0){										
										new SubIE(num,tablename).start();
										count++;
										log.info("此时同正导 " +count+" 个表");
									}
									break;
								}
							}
						}catch(SQLException e){
							log.fatal(e.getMessage(),e);
						}catch(InterruptedException ie){
							log.fatal(ie.getMessage(),ie);
							log.error(tablename+"　表取数量失败");
						}finally{
							DBUtil.close(rs, st);
						}					
					}
				}
		} catch (JDOMException e) {
			log.fatal(e.getMessage(),e);
		} catch (ConfigException e) {
			log.fatal(e.getMessage(),e);
		}finally{
			DBUtil.closeConn(oldcon);
		}
		long time3 = System.currentTimeMillis();
		log.info("所有表分配完毕，等待剩余数据导完，距开始导花费"+(time3-time1)/1000.0+"秒");
		log.info("共导了 "+imptables.size()+" 个表,分别是 "+imptables);
		oldtables.removeAll(imptables);
		log.info("原库中有 "+oldtables.size()+" 个表没有导出，分别是"+oldtables);
		log.info("新库中有 "+newtables.size()+" 个表没有导入，分别是"+newtables);
		while(true){
			if(Thread.activeCount()>1){
				log.info("还有"+(Thread.activeCount()-1)+"个连接在导,等待...");
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}else{
				log.info("导表完毕,关闭所有打开的连接");
				ConnectionManager.closeAllConn();
				break;
			}
		}
		long time4 = System.currentTimeMillis();
		log.info("导表结束，程序关闭，程序运行时间"+(time4-time1)/1000.0+"秒");
	}
	
	private static void getArgs(String[] args){
		for(int i=0;i<args.length;i++){
			arguments.put(args[i], args[i]);
		}
	}
	
	/**
	 * 准备好要导的表
	 */
	private static void init(Connection con){
		oldtables = config.getTables();
		Connection c = null;
		if(oldtables.size() == 0){
			oldtables = DBUtil.getTables(con);
			try{
				while(true){
					c = ConnectionManager.getConn(DBUtil.NEWDB);
					if(c!= null){
						break;
					}else{
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							log.fatal(e.getMessage(),e);
						}
					}
				}
				newtables = DBUtil.getTables(c);
			}finally{
				ConnectionManager.freeConn(c, DBUtil.NEWDB);
//				DBUtil.closeConn(c);
			}
		}else{
			if(newtables == null){
				newtables = new ArrayList<String>();
			}
			newtables.addAll(oldtables);
		}
	}
}

class SubIE extends Thread{
	
	private static final Log log = LogFactory.getLog(SubIE.class);
	public static String ACTIVE = "Y";
	
	private long number = 0;
	private String tablename = "";
	
	private static int limit = DBInfo.getConlimit();
	
	/**
	 * 为后面的正常关闭准备 
	 */
	Statement st = null;
	
	private int count = 1;
	private int num = DBInfo.getRecordlimit();
	
	/**
	 * 记录每个出去的线程的状态,管理线程
	 */
	public HashMap<String, String> threadmap = new HashMap<String, String>();
	
	public SubIE(long number,String tablename){
		this.number = number;
		this.tablename = tablename;
	}
	
	public void run(){
		ResultSet rs = null;
		while(true){
			if(threadmap.size()<=limit){
				Connection con = null;
				while(true){
					con = ConnectionManager.getConn(DBUtil.OLDDB);
					if(con != null){
						break;
					}else{
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				if(number>=num){
					rs = getData(con, tablename, count, num);
					Thread t = new ExeIns(this,con,st,rs,tablename);
					threadmap.put(String.valueOf(t.getId()), SubIE.ACTIVE);
					t.start();
					number-=num;
					count++;
				}else{
					rs = getData(con, tablename, count, num);
					Thread t = new ExeIns(this,con,st,rs,tablename);
					threadmap.put(String.valueOf(t.getId()), SubIE.ACTIVE);
					t.start();
					break;
				}
			}else{
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		IEDBMain.count--;
	}
	
	/**
	 * 
	 * @param con
	 * @param count 表示取第几次了
	 * @param num 表示一次取的条数
	 * @return
	 * @throws SQLException 
	 */
	private ResultSet getData(Connection con,String tablename, int count,int num){
		log.info(this.getId()+" 开始从数据库取数据");
		long time1 = System.currentTimeMillis();
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT * FROM (SELECT t.*, row_number() over(");
		sql.append(getOrderSql(con,tablename));
		sql.append(") row_num FROM ").append(tablename).append(" t) WHERE 1=1");
		sql.append(" and row_num>").append((count-1)*num);
		sql.append(" and row_num<=").append(count*num);
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql.toString());
		} catch (SQLException e) {
			log.error("取 "+tablename+" 表时,第 "+count+" 次数据时出错,一次取 "+num+" 条");
			log.info("错误SQL:"+sql);
		}finally{
			this.st = st;
		}
		long time2 = System.currentTimeMillis();
		log.info(this.getId()+" 从数据库中取数据完毕,花费 "+(time2-time1)/1000.0+" 秒");
		return rs;
		
	}
	
	private String getOrderSql(Connection con,String tablename){
		ArrayList<String> pklist = null;
		StringBuffer ordersql = null;
		try {
			if(DBInfo.pkmap.get(tablename) == null){
				pklist = new ArrayList<String>();
				DatabaseMetaData dmd = con.getMetaData();
				ResultSet pk = dmd.getPrimaryKeys(con.getCatalog(), DBUtil.SCHEMA, tablename);
				while(pk.next()){
					pklist.add(pk.getString("COLUMN_NAME"));
				}
				DBInfo.pkmap.put(tablename, pklist);
			}else{
				pklist = (ArrayList<String>)DBInfo.pkmap.get(tablename);
			}
			if(pklist.size() == 0){
				ordersql = new StringBuffer(" ORDER BY ROWID ASC ");
			}else{
				ordersql = new StringBuffer(" ORDER BY ");
				for(int i=0;i<pklist.size();i++){
					ordersql.append(pklist.get(i)).append(" ASC ,");
				}
				ordersql.deleteCharAt(ordersql.length()-1);	
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ordersql.toString();
	}
}

class ExeIns extends Thread{
	private static final Log log = LogFactory.getLog(ExeIns.class);
	
	ResultSet rs = null;
	SubIE subie = null;
	String tablename = null;
	Connection oldcon = null;
	Statement st = null;
	
	public ExeIns(SubIE subie,Connection con , Statement st, ResultSet rs, String tablename){
		this.rs = rs;
		this.subie = subie;
		this.tablename = tablename;
		this.oldcon = con;
		this.st = st;
	}
	
	public void run(){
		long start = System.currentTimeMillis();
		Connection newcon = null;
		String sqlstr = null;
		try {
			int successnum = 0;
			int failnum = 0;
			while(true){
				newcon = ConnectionManager.getConn(DBUtil.NEWDB);
				if(newcon != null){
					break;
				}else{
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						log.fatal(e.getMessage(),e);
					}
				}
			}
			
			ResultSetMetaData rsmd = rs.getMetaData();
			
			int colcount = rsmd.getColumnCount();
			int coltype[] = new int[colcount];
			String colname[]  = new String[colcount];
			for(int i=0;i<colcount;i++){
				coltype[i] = rsmd.getColumnType(i+1);
				colname[i] = rsmd.getColumnName(i+1);
			}
			
			StringBuffer sql = new StringBuffer();
			sql.append("INSERT INTO ").append(tablename).append("(");
			for(int i=0;i<colcount;i++){
				if(!colname[i].equalsIgnoreCase("row_num"))
					sql.append(colname[i]).append(",");
			}
			sql.deleteCharAt(sql.length()-1);
			sql.append(") values(");
			for(int i=0;i<colcount;i++){
				if(!colname[i].equalsIgnoreCase("row_num"))
					sql.append("?,");
			}
			sql.deleteCharAt(sql.length()-1);
			sql.append(")");
			sqlstr = sql.toString();
			PreparedStatement ps = newcon.prepareStatement(sqlstr);
			while(rs.next()){
				ps.clearParameters();
				for(int i=0;i<colcount;i++){
					if(!colname[i].equalsIgnoreCase("row_num")){
						if(coltype[i]==Types.DATE){
//							tmp = rs.getString(colname[i]);
//							if(tmp != null){
//								if(tmp.indexOf("00:00:00")==-1){
//									sql.append("to_date('");
//									sql.append(tmp.substring(0, tmp.length()-2));
//									sql.append("','yyyy-mm-dd hh24:mi:ss'),");
//								}else{
//									sql.append("to_date('");
//									sql.append(tmp.substring(0, 10));
//									sql.append("','yyyy-mm-dd'),");
//								}
//							}else{
//								sql.append("null,");
//							}
							Date date = rs.getDate(colname[i]);
							if(date != null){
								ps.setDate(i+1, date);
							}else{
								ps.setString(i+1, null);
							}						
						}else if(coltype[i]==Types.NUMERIC){
//							tmp = rs.getString(colname[i]);
//							sql.append(tmp).append(",");

							String num = rs.getString(colname[i]);
							if(num != null){
								ps.setLong(i+1, rs.getLong(colname[i]));
							}else{
								ps.setString(i+1, null);
							}
						
							
						}else if(coltype[i]==Types.VARCHAR){
//							tmp = rs.getString(colname[i]);
//							if(tmp != null){
//								if(tmp.indexOf('\'')!=-1){
//									tmp = tmp.replaceAll("'", "''");
//								}
//								sql.append("'").append(tmp).append("',");
//							}else{
//								sql.append("'',");
//							}

							ps.setString(i+1, rs.getString(colname[i]));
							
						}else if(coltype[i]==Types.CLOB){
							Clob clob = rs.getClob(colname[i]);
//							ps.setClob(i+1, rs.getClob(colname[i]));
							
							if(clob != null){
								ps.setCharacterStream(i+1, clob.getCharacterStream(), (int)clob.length());
							}else{
								ps.setString(i+1, null);
							}
							
						}else if(coltype[i]==Types.BLOB){
							Blob blob = rs.getBlob(colname[i]);
							if(blob != null){
								ps.setBlob(i+1, blob);
							}else{
								ps.setString(i+1, null);
							}
						}else if(coltype[i]==Types.CHAR){
							if(rs.getString(colname[i]) != null){
								ps.setString(i+1, rs.getString(colname[i]));
							}else{
								ps.setString(i+1, null);
							}			
						}else if(coltype[i]==Types.LONGVARCHAR){
							if(rs.getString(colname[i]) != null){
								ps.setAsciiStream(i+1, rs.getAsciiStream(colname[i]), rs.getString(colname[i]).length());
							}else{
								ps.setString(i+1, null);
							}
						}
						
					}
				}
				try{
					ps.executeUpdate();
				}catch(SQLException ee) {
					log.fatal("表:"+tablename+"  "+ee.getMessage());
					log.fatal("记录:"+tablename+"  "+ee.getMessage());
					successnum--;
					failnum++;
				}
				successnum++;
			}
			newcon.commit();
			ps.close();
			long end = System.currentTimeMillis();
			log.info(tablename +" 表成功导入 "+successnum+" 条,失败 "+failnum+" 条，花费"+(end-start)/1000.0+" 秒");
			log.info("总数据量 "+(DBInfo.totaldatacount+=successnum)+" 条");
		} catch (SQLException e) {
			log.fatal(e.getMessage(),e);
			log.error(sqlstr);
		}finally{
			DBUtil.close(rs, st);
			ConnectionManager.freeConn(oldcon, DBUtil.OLDDB);
			ConnectionManager.freeConn(newcon, DBUtil.NEWDB);
			subie.threadmap.remove(String.valueOf(this.getId()));
		}
	}
}

/*
 * 得到所有的类型
		HashMap hm = new HashMap();
		HashMap hm2 = new HashMap();
try {
	//PreparedStatement ps = oldcon.prepareStatement("select * from ? where rownum=1");
	Statement st = oldcon.createStatement();
	for(int i=0;i<oldtables.size();i++){
//		ps.setString(1, (String)oldtables.get(i));
		ResultSet rs1 = st.executeQuery("select * from "+(String)oldtables.get(i)+" where rownum=1");
		ResultSetMetaData rsmd = rs1.getMetaData();
		int count = rsmd.getColumnCount();
		for(int j=0;j<count;j++){
			if(hm.get(rsmd.getColumnTypeName(j+1)) == null){
				hm.put(rsmd.getColumnTypeName(j+1), rsmd.getColumnType(j+1));
			}
		}
	}
	st = newcon.createStatement();
	for(int i=0;i<newtables.size();i++){
		ResultSet rs1 = st.executeQuery("select * from "+(String)newtables.get(i)+" where rownum=1");
		ResultSetMetaData rsmd = rs1.getMetaData();
		int count = rsmd.getColumnCount();
		for(int j=0;j<count;j++){
			if(hm2.get(rsmd.getColumnTypeName(j+1)) == null){
				hm2.put(rsmd.getColumnTypeName(j+1), rsmd.getColumnType(j+1));
			}
			if(rsmd.getColumnType(j+1) == -3){
				System.out.println();
			}
		}
	}
} catch (SQLException e) {
	e.printStackTrace();
}
*/



/*
ResultSet rs1 = dm.getPrimaryKeys(con.getCatalog(), "CMSUSER", "RELEASELIB");

ResultSetMetaData rsmd1 = rs1.getMetaData();
while(rs1.next()){
	for(int i=0;i<rsmd1.getColumnCount();i++){
    	System.out.println(rs1.getString(i+1));
	}
}

int colcount = rsmd.getColumnCount();
int coltype[] = new int[colcount];
String colname[]  = new String[colcount];
String a[] = new String[colcount];
for(int i=0;i<colcount;i++){
	coltype[i] = rsmd.getColumnType(i+1);
	colname[i] = rsmd.getColumnTypeName(i+1);
	a[i] = rsmd.getSchemaName(i+1);
	System.out.println(colname[i]+"---"+coltype[i]+"---"+a[i]);
}
*/