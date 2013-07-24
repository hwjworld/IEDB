/**
 * 
 */
package com.founder.enp.dayoo;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.JDOMException;

import com.founder.enp.dayoo.DB.ConnectionManager;
import com.founder.enp.dayoo.DB.DBUtil;
import com.founder.enp.dayoo.Exception.ConfigException;
import com.founder.enp.dayoo.config.ConfigManager;

/**
 * @author huangwenjie
 * 
 */
public class IncreIE {
	private static final Log log = LogFactory.getLog(IncreIE.class);
	public static ConfigManager config = null;
	public static String configxml = "";
	public static List<String> oldtables = null;
	public static List<String> newtables = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length >= 1) {
			configxml = args[0];
		} else {
			log.error("û��ָ�������ļ�");
			System.exit(-1);
		}
		ConfigManager.setConfigPath(configxml);
		Connection oldcon = null;
		Connection newcon = null;
		try {
			config = ConfigManager.getInstance();
			oldcon = DBUtil.getConn(DBUtil.OLDDB);
			log.info("*******************************");
			log.info("������������������");
			log.info("     �汾:" + config.getVersion());
			log.info("  ��������:" + new Date(System.currentTimeMillis()));
			log.info("  Ҫ���ı�:" + config.getTables());
			log.info("*******************************");

			init(oldcon);
			for (int i = 0; i < oldtables.size(); i++) {
				String tablename = (String) oldtables.get(i);
				log.info("��ʼ��" + tablename);
				if (DBUtil.exist(tablename, newtables)
						&& DBUtil.valid(tablename)) {
					String sql = getMaxIdSql(tablename, config.getPK());
					Statement st = null;
					ResultSet rs = null;
					newcon = ConnectionManager.getConn(DBUtil.NEWDB);
					st = newcon.createStatement();
					rs = st.executeQuery(sql);
					long id = 0;
					if (rs.next()) {
						id = rs.getLong(1);
					}
					log.info("�ӱ� "+tablename+" ��¼ "+id+" ��ʼ��");
					
					DBUtil.close(rs, st);
					while (true) {
						if (Thread.activeCount() <= 11) {
							new SubIncreIE(tablename, config.getPK(), id).start();
							break;
						} else {
							try {
								Thread.sleep(2000);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}

				}
			}
		} catch (JDOMException e) {
			e.printStackTrace();
		} catch (ConfigException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBUtil.closeConn(oldcon);
			ConnectionManager.freeConn(newcon, DBUtil.NEWDB);
		}
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
	}

	private static void init(Connection con) {
		oldtables = config.getTables();
		Connection c = null;
		if (oldtables.size() == 0) {
			oldtables = DBUtil.getTables(con);
			try {
				c = ConnectionManager.getConn(DBUtil.NEWDB);
				newtables = DBUtil.getTables(c);
			} finally {
				ConnectionManager.freeConn(c, DBUtil.NEWDB);
				// DBUtil.closeConn(c);
			}
		} else {
			if (newtables == null) {
				newtables = new ArrayList<String>();
			}
			newtables.addAll(oldtables);
		}
	}

	private static String getMaxIdSql(String tablename, String pk) {
		String select = "SELECT ";
		String from = " FROM ( SELECT ";
		String from2 = " FROM ";
		String orderby = " ORDER BY ";
		String desc = " DESC)WHERE ROWNUM=1 ";
		return (select + pk + from + pk + from2 + tablename + orderby + pk + desc);
	}

}

class SubIncreIE extends Thread {
	
	private static final Log log = LogFactory.getLog(SubIncreIE.class);
	
	public static String tablename = "";
	public static String pk = "";
	public static long lastid = 0;
	
	private Statement st = null; 
	
	public SubIncreIE(String tbn, String pk,long id){
		tablename = tbn;
		SubIncreIE.pk = pk;
		lastid = id;
	}
	
	public void run() {
		Connection oldcon = null;
		Connection newcon = null; 
		try{
			oldcon = ConnectionManager.getConn(DBUtil.OLDDB);
			while(true){
				if(oldcon != null){
					break;
				}else{
					try{
						Thread.sleep(2000);
					}catch(InterruptedException e){
						log.fatal(e.getMessage(),e);
					}
				}
			}
			while(true){
				ResultSet rs = getData(oldcon);
				if(rs.next()){
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
						sql.append(colname[i]).append(",");
					}
					sql.deleteCharAt(sql.length()-1);
					sql.append(") values(");
					for(int i=0;i<colcount;i++){
						sql.append("?,");
					}
					sql.deleteCharAt(sql.length()-1);
					sql.append(")");
					
					
					newcon = ConnectionManager.getConn(DBUtil.NEWDB);
					while(true){
						if(newcon != null){
							break;
						}else{
							try{
								Thread.sleep(2000);
							}catch(InterruptedException e){
								log.fatal(e.getMessage(),e);
							}
						}
					}			
					PreparedStatement ps = newcon.prepareStatement(sql.toString());
					int insertcount = 0;
					rs.beforeFirst();
					while(rs.next()){
						long id = rs.getLong(pk);
						for(int i=0;i<colcount;i++){
							if(coltype[i]==Types.DATE){
								Date date = rs.getDate(colname[i]);
								if(date != null){
									ps.setDate(i+1, date);
								}else{
									ps.setString(i+1, null);
								}
							}else if(coltype[i]==Types.NUMERIC){
								String num = rs.getString(colname[i]);
								if(num != null){
									ps.setInt(i+1, rs.getInt(colname[i]));
								}else{
									ps.setString(i+1, null);
								}					
							}else if(coltype[i]==Types.VARCHAR){
								ps.setString(i+1, rs.getString(colname[i]));						
							}else if(coltype[i]==Types.CLOB){
								Clob clob = rs.getClob(colname[i]);
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
								ps.setString(i+1, rs.getString(colname[i]));			
							}else if(coltype[i]==Types.LONGVARCHAR){
								ps.setAsciiStream(i+1, rs.getAsciiStream(colname[i]), rs.getString(colname[i]).length());
							}
						}
						try{
							ps.executeUpdate();
							insertcount++;
							lastid = id;
							log.info("���� "+id+" �ɹ� ���ɹ����� "+insertcount+" ��");
						}catch(SQLException e){
							log.error("���� "+id+" ����");
							log.error(e.getMessage());
						}
					}
					try{
						ps.close();
					}catch(SQLException e){
						log.info("�ر�һ��cursor����");
					}
					log.info("����1000��,�� "+lastid+" Ϊֹ�ɹ�,�˴γɹ����� "+insertcount+" ��");
					DBUtil.close(rs, st);
				}else{
					break;
				}
			}
		} catch (SQLException e) {
			
		}finally{
			ConnectionManager.freeConn(oldcon, DBUtil.OLDDB);
			ConnectionManager.freeConn(newcon, DBUtil.NEWDB);
		}
		
	}
	
	public ResultSet getData(Connection oldcon){
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT * FROM (SELECT * FROM ");
		sql.append(tablename).append(" WHERE ").append(pk);
		sql.append(">").append(lastid);
		sql.append(" ORDER BY ").append(pk);
		sql.append(" ASC) WHERE ROWNUM<1001");
		ResultSet rs = null;
		try{
			st = oldcon.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
			rs = st.executeQuery(sql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}
}