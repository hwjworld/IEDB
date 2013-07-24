/**
 * 
 */
package com.founder.enp.dayoo.DB;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Vector.get(0)=checkOut<p>
 * ����Ԫ��Ϊ����
 * @author huangwenjie
 * 
 */
public class ConnectionManager {
	
	private static final Log log = LogFactory.getLog(ConnectionManager.class);
	/**
	 * Connection���Ĺ���
	 */
	private static Vector<Object> oldtablev = new Vector<Object>();
	private static Vector<Object> newtablev = new Vector<Object>();
//	private static HashMap oldtablehm = new HashMap();
//	private static HashMap newtablehm = new HashMap();
	
	/*
	public static Connection getConn(String tablename,int flag){
		Object obj = tablehm.get(tablename);
		if(obj == null){
			
		}else{
			Vector v = (Vector)obj;
			int checkOut = ((Integer)v.firstElement()).intValue();
			if(checkOut > DBInfo.tbconlimit){
				return null;
			}else{
				Connection c = null;
				if(v.size()>1){
					c = (Connection)v.lastElement();
					v.remove(0);
					return c;
				}else{
					c = DBUtil.getConn(flag);
				}
			}
		}
	}
	*/
	public synchronized static Connection getConn(int flag){
		Vector<Object> v = null;
		if(flag == DBUtil.OLDDB){
			v = oldtablev;
		}else if(flag == DBUtil.NEWDB){
			v = newtablev;
		}
		Connection c = null;
		if(v.size()<1){
			int checkOut = 1;
			c = DBUtil.getConn(flag);
			v.add( new Integer(checkOut));
		}else{
			int checkOut = ((Integer)v.firstElement()).intValue();
			if(checkOut>DBInfo.getConlimit()){
				if(flag == DBUtil.OLDDB){
					log.info("ԭ ���ݿ�����ʹ���ѹ�����"+DBInfo.getConlimit());
				}else if(flag == DBUtil.NEWDB){
					log.info("�� ���ݿ�����ʹ���ѹ�����"+DBInfo.getConlimit());
				}
				return null;
			}else{
				if(v.size() > 1){
					c = (Connection)v.remove(v.size()-1);
					checkOut++;
					v.set(0, new Integer(checkOut));
					try {
						if(c instanceof Connection){
							if(c.isClosed()){
								c = DBUtil.getConn(flag);
							}
						}
					} catch (SQLException e) {
						log.error(e);
					}
					
				}else{
					c = DBUtil.getConn(flag);
				}
			}
		}
		if(flag == DBUtil.OLDDB){
			log.info("�õ� ԭ ���ݿ����ӳɹ�");
		}else if(flag == DBUtil.NEWDB){
			log.info("�õ� �� ���ݿ����ӳɹ�");
		}
		return c;
	}

	public synchronized static void freeConn(Connection con,int flag){
		Vector<Object> v = null;
		if(flag == DBUtil.OLDDB){
			v = oldtablev;
		}else if(flag == DBUtil.NEWDB){
			v = newtablev;
		}
		if(v.size()!=0){
			int checkOut = ((Integer)v.firstElement()).intValue();
			checkOut--;
			v.add(con);
			v.set(0, new Integer(checkOut));
			if(flag == DBUtil.OLDDB){
				log.info("�ͷ� ԭ ���ݿ����ӳɹ�");
			}else if(flag == DBUtil.NEWDB){
				log.info("�ͷ� �� ���ݿ����ӳɹ�");
			}			
		}
	}
	
	public static synchronized void closeAllConn(){
		Connection c = null;
		Object o = null;
		for(int i=1;i<oldtablev.size();i++){
			
			o = oldtablev.get(i);
			if(o instanceof Connection){
				c = (Connection)o;
				DBUtil.closeConn(c);
			}
		}

		for(int i=1;i<newtablev.size();i++){
			o = newtablev.get(i);
			if(o instanceof Connection){
				c = (Connection)o;
				DBUtil.closeConn(c);
			}
		}
	}
}
