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
	 * ����һ�����ֻ�ܵ�5����
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
			System.out.println("config:ָ�������ļ�,��config.xml");
			System.out.println("exptables:�������Ϣ");
			System.out.println("  check:��֤��ṹ(������Ŀ���ı������֤)");
			System.out.println("exppk:������������Ϣ");
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
						//����һ���µ��б�
						List<String> newnew = new ArrayList<String>();
						newnew.addAll(newtables);
						
						newtables.removeAll(oldtables);
						DBUtil.expAllTables2File(newtables,new File(curdir+File.separator+"newtbown.txt"));
						oldtables.removeAll(newnew);
						DBUtil.expAllTables2File(oldtables,new File(curdir+File.separator+"oldtbown.txt"));
						log.info("�ѵ�������Ϣ�ļ�");
						log.info("tbinfo\\newtb.txtΪĿ�������б�");
						log.info("tbinfo\\oldtb.txtΪԴ������б�");
						log.info("tbinfo\\newtbnew.txtΪĿ�������Դ�����еı�");
						log.info("tbinfo\\oldtbown.txtΪԴ������Ŀ������еı�");
						
					}catch(Exception e){
						e.printStackTrace();
						log.info("��������Ϣ�ļ�ʧ��");
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
					log.info("�ѵ�����������Ϣ�ļ�");
					log.info("tbinfo\\newtbpk.txtΪĿ����������Ϣ");
					log.info("tbinfo\\oldtbpk.txtΪԴ���������Ϣ");
				}catch(Exception e){
					log.error("����������Ϣ�ļ�ʧ��");
				}finally{
					DBUtil.closeConn(newcon);
					DBUtil.closeConn(oldcon);
				}
				System.exit(0);
			}
			
			
			log.info("*******************************");
			log.info("     �汾:"+config.getVersion());
			log.info("  ��������:"+new Date(System.currentTimeMillis()));
			log.info("  Ҫ���ı�:"+(config.getTables().size()==0?"���б�":config.getTables().toString()));
			log.info("*******************************");
				oldcon = DBUtil.getConn(DBUtil.OLDDB);
				init(oldcon);
				long time2 = System.currentTimeMillis();
				log.info("����׼���������,����"+(time2-time1)/1000.0+"��");
				
				Statement st = null;
				ResultSet rs = null;
				for(int i=0;i<oldtables.size();i++){
					String tablename = (String)oldtables.get(i);
					log.info("��ʼ��"+tablename);
					if(DBUtil.exist(tablename,newtables) && DBUtil.valid(tablename)){
						imptables.add(tablename);
						try{
							st = oldcon.createStatement();
							rs = st.executeQuery(sql+tablename);
							long num = 0;
							if(rs.next()){
								num = rs.getLong(1);
							}
							log.info(tablename+" �������� "+num);
							while(true){
								if(count>limit){
									Thread.sleep(2000);
								}else{
									if(num > 0){										
										new SubIE(num,tablename).start();
										count++;
										log.info("��ʱͬ���� " +count+" ����");
									}
									break;
								}
							}
						}catch(SQLException e){
							log.fatal(e.getMessage(),e);
						}catch(InterruptedException ie){
							log.fatal(ie.getMessage(),ie);
							log.error(tablename+"����ȡ����ʧ��");
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
		log.info("���б������ϣ��ȴ�ʣ�����ݵ��꣬�࿪ʼ������"+(time3-time1)/1000.0+"��");
		log.info("������ "+imptables.size()+" ����,�ֱ��� "+imptables);
		oldtables.removeAll(imptables);
		log.info("ԭ������ "+oldtables.size()+" ����û�е������ֱ���"+oldtables);
		log.info("�¿����� "+newtables.size()+" ����û�е��룬�ֱ���"+newtables);
		while(true){
			if(Thread.activeCount()>1){
				log.info("����"+(Thread.activeCount()-1)+"�������ڵ�,�ȴ�...");
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}else{
				log.info("�������,�ر����д򿪵�����");
				ConnectionManager.closeAllConn();
				break;
			}
		}
		long time4 = System.currentTimeMillis();
		log.info("�������������رգ���������ʱ��"+(time4-time1)/1000.0+"��");
	}
	
	private static void getArgs(String[] args){
		for(int i=0;i<args.length;i++){
			arguments.put(args[i], args[i]);
		}
	}
	
	/**
	 * ׼����Ҫ���ı�
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
	 * Ϊ����������ر�׼�� 
	 */
	Statement st = null;
	
	private int count = 1;
	private int num = DBInfo.getRecordlimit();
	
	/**
	 * ��¼ÿ����ȥ���̵߳�״̬,�����߳�
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
	 * @param count ��ʾȡ�ڼ�����
	 * @param num ��ʾһ��ȡ������
	 * @return
	 * @throws SQLException 
	 */
	private ResultSet getData(Connection con,String tablename, int count,int num){
		log.info(this.getId()+" ��ʼ�����ݿ�ȡ����");
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
			log.error("ȡ "+tablename+" ��ʱ,�� "+count+" ������ʱ����,һ��ȡ "+num+" ��");
			log.info("����SQL:"+sql);
		}finally{
			this.st = st;
		}
		long time2 = System.currentTimeMillis();
		log.info(this.getId()+" �����ݿ���ȡ�������,���� "+(time2-time1)/1000.0+" ��");
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
					log.fatal("��:"+tablename+"  "+ee.getMessage());
					log.fatal("��¼:"+tablename+"  "+ee.getMessage());
					successnum--;
					failnum++;
				}
				successnum++;
			}
			newcon.commit();
			ps.close();
			long end = System.currentTimeMillis();
			log.info(tablename +" ��ɹ����� "+successnum+" ��,ʧ�� "+failnum+" ��������"+(end-start)/1000.0+" ��");
			log.info("�������� "+(DBInfo.totaldatacount+=successnum)+" ��");
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
 * �õ����е�����
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