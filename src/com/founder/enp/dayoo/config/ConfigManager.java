/**
 * 
 */
package com.founder.enp.dayoo.config;

import java.util.ArrayList;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.founder.enp.dayoo.DB.DBInfo;
import com.founder.enp.dayoo.Exception.ConfigException;

/**
 * @author huangwenjie
 * 
 */
public class ConfigManager {
//	private static final Log log = LogFactory.getLog(ConfigManager.class);

	private static ConfigManager config = null;
	private static String CONFIG_PATH = "config.xml";
	
	private List<String> tables = new ArrayList<String>();
	private String primarykey = "";
//	private List extables = new ArrayList();
	
	private String version = null;

	protected void loadConfig() throws JDOMException, ConfigException {
		SAXBuilder builder = new SAXBuilder();
		Document doc = builder.build(getClass().getClassLoader().getResourceAsStream(CONFIG_PATH));
		Element root = doc.getRootElement();
		String tmp = null;
		
		tmp = root.getChildText("version");
		if(tmp != null && !tmp.equals("")){
			this.version = tmp;
		}else{
			version = "2008-03-14";
		}
		
		Element ele = root.getChild("table");
		tmp = ele.getChildText("table-name");
		String tables[] = null;
		if(tmp != null && !tmp.equals("")){
			tables = tmp.split(";");			
		}
		if(tables != null){
			for(int i=0;i<tables.length;i++){
				this.tables.add(tables[i]);
			}
		}
		primarykey = ele.getChildText("primarykey");

		/*
		String extablestr = root.getChildText("ex-table");
		String extables[] = null;
		if(extablestr != null && extablestr.equals("")){
			extables = extablestr.split(";");			
		}
		for(int i=0;i<extables.length;i++){
			this.extables.add(extables[i]);
		}
		*/
		DBInfo.setConlimit(Integer.parseInt(load(root,"conn-limit")));
		DBInfo.setConlimit(Integer.parseInt(load(root,"record-limit")));
		
		Element fromdb = root.getChild("fromdb");
		DBInfo.setFromip(load(fromdb,"ip"));
		DBInfo.setFromport(load(fromdb,"port"));
		DBInfo.setFromservicename(load(fromdb,"service-name"));
		DBInfo.setFromusernmae(load(fromdb,"username"));
		DBInfo.setFrompwd(load(fromdb,"pwd"));
		
		Element todb = root.getChild("todb");
		DBInfo.setToip(load(todb,"ip"));
		DBInfo.setToport(load(todb,"port"));
		DBInfo.setToservicename(load(todb,"service-name"));
		DBInfo.setTousernmae(load(todb,"username"));
		DBInfo.setTopwd(load(todb,"pwd"));
	}
	protected String load(Element e ,String name) throws ConfigException{
		String tmp = null;
		tmp = e.getChildText(name);
		if(tmp != null && !tmp.equals("")){
			return tmp;
		}else{
			throw new ConfigException(e.getName()+":"+name +" ÓÐÎó");			
		}
	}

	protected ConfigManager() throws JDOMException, ConfigException {
		loadConfig();
	}

	public static ConfigManager getInstance() throws JDOMException, ConfigException {
		if (config == null)
			config = new ConfigManager();

		return config;
	}

	public List<String> getTables(){
		return tables;
	}
	
	/*
	public List getExTables(){
		return extables;
	}
	*/
	
	public String getVersion(){
		return version;
	}
	
	public String getPK(){
		return primarykey;
	}
	
	public static void setConfigPath(String config){
		CONFIG_PATH = config;
	}
	
	
}
