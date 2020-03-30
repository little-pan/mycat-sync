package org.opencloudb.config.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.util.IoUtil;
import org.slf4j.*;

/**
 * 
 * @author yanglixue
 *
 */
public class DnPropertyUtil {

	private static final Logger log = LoggerFactory.getLogger(DnPropertyUtil.class);
	
	/**
	 * 加载dnindex.properties属性文件
	 *
	 * @return 属性文件
	 * @throws IllegalStateException if load properties failed
	 */
	public static Properties loadDnIndexProps() throws IllegalStateException {
		Properties prop = new Properties();
		File file = new File(SystemConfig.getHomePath(),
				"conf" + File.separator + "dnindex.properties");
		if (!file.isFile()) {
			return prop;
		}

		FileInputStream in = null;
		try {
			in = new FileInputStream(file);
			prop.load(in);
		} catch (FileNotFoundException e) {
			log.debug("No file '{}'", file);
		} catch (IOException e) {
			throw new IllegalStateException("Fatal: access file '"+file+"'", e);
		} finally {
			IoUtil.close(in);
		}

		return prop;
	}
	
}
