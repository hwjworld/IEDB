/**
 * 
 */
package com.founder.enp.dayoo.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author huangwenjie
 *
 */
public class FileUtils {

	private static final Log log = LogFactory.getLog(FileUtils.class);
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		File file = new File("d:\\hwj.txt");
		String str = "hwjjj";
		string2File(str, file);

	}

	public static void string2File(String str, File file){
		try {
			if(!file.exists()){
				file.createNewFile();
			}else{
				for(int i=1;;i++){
					String fp = file.getAbsolutePath();
					int lastindex = fp.lastIndexOf(".");
					if(lastindex == -1)
						lastindex = fp.length();
					file = new File(fp.substring(0,lastindex)+i+fp.substring(lastindex));
					if(!file.exists()){
						file.createNewFile();
						break;
					}
				}
			}
			if(file.canWrite()){
				FileOutputStream fos = new FileOutputStream(file);
				fos.write(str.getBytes());
				fos.close();
			}else{
				log.info(file.getName()+"²»¿ÉÐ´");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
