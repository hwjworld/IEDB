package com.founder.enp.dayoo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.founder.enp.dayoo.DB.DBInfo;
import com.founder.enp.dayoo.DB.DBUtil;

public class ExportInfo {
	
	public static String exppk(Connection con){
		String pkstr = null;
		try {
			List<String> tablelist = DBUtil.getTables(con);
			DatabaseMetaData dmd = con.getMetaData();
			for(int i=0;i<tablelist.size();i++){
				List<String> pklist = new ArrayList<String>();
				ResultSet pk = dmd.getPrimaryKeys(con.getCatalog(), DBUtil.SCHEMA, (String) tablelist.get(i));
				while(pk.next()){
					pklist.add(pk.getString("COLUMN_NAME"));
				}
				DBInfo.pkmap.put((String) tablelist.get(i), (ArrayList<String>) pklist);
				DBUtil.close(pk, null);
			}
			pkstr = DBInfo.pkmap.toString();
			pkstr = pkstr.replaceAll("],", "]\n");
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return pkstr;
	}

}
