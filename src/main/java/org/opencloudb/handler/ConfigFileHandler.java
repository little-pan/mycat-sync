/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.handler;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.ParserConfigurationException;

import org.opencloudb.MycatServer;
import org.opencloudb.config.Fields;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.config.util.ConfigUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.IoUtil;
import org.opencloudb.util.StringUtil;
import org.slf4j.*;
import org.xml.sax.SAXException;

/**
 * Mycat config file related handler
 * 
 * @author wuzh
 */
public final class ConfigFileHandler {

	private static final Logger log = LoggerFactory.getLogger(ConfigFileHandler.class);

	private static final String CHARSET = "UTF-8";
	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	private static final String UPLOAD_CMD = "FILE @@UPLOAD";
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("DATA", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void handle( String stmt,ManagerConnection c) {
		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c,true);

		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c,true);
		}

		// write eof
		buffer = eof.write(buffer, c,true);
		// write rows
		byte packetId = eof.packetId;
		final String command = stmt.toUpperCase().trim();
		PackageBufINf bufInf;
		// Commands:
		// 1) file @@list
		// 2) file @@show FILENAME
		// 3) file @@upload FILENAME CONTENT
		// 4) file @@delete FILENAME
		if (command.equals("FILE @@LIST")) {
			bufInf = listConfigFiles(c, buffer, packetId);
		} else if (command.startsWith("FILE @@SHOW")) {
			int index = stmt.lastIndexOf(' ');
			String fileName = stmt.substring(index + 1);
			bufInf = showConfigFile(c, buffer, packetId, fileName);
		} else if (command.startsWith(UPLOAD_CMD)) {
			int index = stmt.indexOf(' ', UPLOAD_CMD.length());
			int index2 = stmt.indexOf(' ', index + 1);
			if (index <= 0 || index2 <= 0
					|| index + 1 > stmt.length() || index2 + 1 > stmt.length()) {
				bufInf = showInfo(c, buffer, packetId, "Invalid parameter");
			} else {
				String fileName = stmt.substring(index + 1, index2);
				String content = stmt.substring(index2 + 1).trim();
				bufInf = uploadConfigFile(c, buffer, packetId, fileName, content);
			}
		} else if (command.startsWith("FILE @@DELETE")) {
			int index = stmt.lastIndexOf(' ');
			String fileName = stmt.substring(index + 1);
			bufInf = deleteConfigFile(c, buffer, packetId, fileName);
		} else {
			bufInf = showInfo(c, buffer, packetId, "Invalid command");
		}

		packetId = bufInf.packetId;
		buffer = bufInf.buffer;

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c,true);

		// write buffer
		c.write(buffer);
	}

	private static void checkXMLFile(String xmlFileName, byte[] data)
			throws ParserConfigurationException, SAXException, IOException {
		InputStream dtdStream;
		File confDir = new File(MycatServer.getDirectory("conf"));

		if (xmlFileName.equals("schema.xml")) {
			File dtdFile = new File(confDir, "schema.dtd");
			dtdStream = new ByteArrayInputStream(readFileByBytes(dtdFile));
		} else if (xmlFileName.equals("server.xml")) {
			File dtdFile = new File(confDir, "server.dtd");
			dtdStream = new ByteArrayInputStream(readFileByBytes(dtdFile));
		} else if (xmlFileName.equals("rule.xml")) {
			File dtdFile = new File(confDir, "rule.dtd");
			dtdStream = new ByteArrayInputStream(readFileByBytes(dtdFile));
		} else {
			throw new IllegalArgumentException("Unknown xml file: " + xmlFileName);
		}

		ConfigUtil.getDocument(dtdStream, new ByteArrayInputStream(data));
	}

	/**
	 * 以字节为单位读取文件，常用于读二进制文件，如图片、声音、影像等文件。
	 */
	private static byte[] readFileByBytes(File fileName) throws IOException {
		InputStream in = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try { // 一次读多个字节
			byte[] temp = new byte[4096];
			int n = 0;
			in = new FileInputStream(fileName);
			// 读入多个字节到字节数组中，byteread为一次读入的字节数
			while ((n = in.read(temp)) != -1) {
				out.write(temp, 0, n);
			}
		} finally {
			IoUtil.close(in);
		}

		return out.toByteArray();
	}

	private static PackageBufINf uploadConfigFile(final ManagerConnection c, final ByteBuffer buffer,
												  byte packetId, final String fileName, String content) {
		log.debug("Upload config file '{}', content: {}", fileName, content);

		final String timeFormat = "yyyyMMddHHmmssSSS";
		DateFormat df = new SimpleDateFormat(timeFormat);
		final String tempFileName = df.format(new Date()) + "_" + fileName;
		File tempFile = MycatServer.getConfigFile(tempFileName);
		BufferedOutputStream buff = null;
		OutputStream out = null;
		try {
			byte[] fileData = content.getBytes(CHARSET);
			if (fileName.endsWith(".xml")) {
				checkXMLFile(fileName, fileData);
			}
			out = new FileOutputStream(tempFile);
			buff = new BufferedOutputStream(out);
			buff.write(fileData);
			buff.flush();
		} catch (Exception e) {
			log.warn("Write file error", e);
			deleteFile(tempFile);
			return showInfo(c, buffer, packetId, "Write file error: " + e);
		} finally {
			IoUtil.close(buff);
			IoUtil.close(out);
		}

		// First backup: file -> file_TS_auto
		File oldFile = MycatServer.getConfigFile(fileName);
		final String backName = fileName + "_" + df.format(new Date()) + "_auto";
		if (oldFile.isFile()) {
			File backUP = MycatServer.getConfigFile(backName);
			if (!oldFile.renameTo(backUP)) {
				deleteFile(tempFile);
				String msg = "rename old file failed";
				log.warn("{} for upload file '{}'", msg, oldFile.getAbsolutePath());
				return showInfo(c, buffer, packetId, msg);
			}
		}
		// temp file -> file
		File dest = MycatServer.getConfigFile(fileName);
		if (!tempFile.renameTo(dest)) {
			deleteFile(tempFile);
			// Restore from backup
			File backUP = MycatServer.getConfigFile(backName);
			File source = MycatServer.getConfigFile(fileName);
			if (!backUP.renameTo(source)) {
				String msg = "Restore config file from backup failed";
				return showInfo(c, buffer, packetId, msg);
			}
			String msg = "rename file failed";
			log.warn("{} for upload file '{}'", msg, tempFile.getAbsolutePath());
			return showInfo(c, buffer, packetId, msg);
		}
		deleteFile(tempFile);
		File backUP = MycatServer.getConfigFile(backName);
		deleteFile(backUP);

		return showInfo(c, buffer, packetId, "Success: save file '" + fileName+"'");
	}

	static void deleteFile(File file) {
		deleteFile(file, true);
	}

	static void deleteFile(File file, boolean silent) throws ConfigException {
		if (!file.isFile()) {
			if (silent) {
				log.warn("File not found: '{}'", file);
			} else {
				throw new ConfigException("File not found: '"+file+"'");
			}
		}
		if (!file.delete()) {
			if (silent) {
				log.warn("Delete file failed: '{}'", file);
				return;
			} else {
				throw new ConfigException("Delete file failed: '"+file+"'");
			}
		}
		log.debug("File deleted: '{}'", file);
	}

	private static PackageBufINf showInfo(ManagerConnection c,
			ByteBuffer buffer, byte packetId, String string) {
		PackageBufINf bufINf = new PackageBufINf();
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(string, c.getCharset()));
		row.packetId = ++packetId;
		buffer = row.write(buffer, c,true);
		bufINf.packetId = packetId;
		bufINf.buffer = buffer;
		return bufINf;
	}

	private static PackageBufINf showConfigFile(ManagerConnection c,
			ByteBuffer buffer, byte packetId, String fileName) {
		File file = MycatServer.getConfigFile(fileName);
		BufferedReader br = null;
		PackageBufINf bufINf = new PackageBufINf();
		InputStream in = null;
		try {
			in = new FileInputStream(file);
			InputStreamReader ir = new InputStreamReader(in, CHARSET);
			br = new BufferedReader(ir);
			for (;;) {
				final String line = br.readLine();
				if (line == null) {
					break;
				}
				if (line.isEmpty()) {
					continue;
				}
				RowDataPacket row = new RowDataPacket(FIELD_COUNT);
				row.add(StringUtil.encode(line, c.getCharset()));
				row.packetId = ++packetId;
				buffer = row.write(buffer, c,true);
			}
			bufINf.buffer = buffer;
			bufINf.packetId = packetId;
			return bufINf;
		} catch (IOException e) {
            log.warn("showConfigFile error", e);
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(e.toString(), c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
			bufINf.buffer = buffer;
			return bufINf;
		} finally {
			IoUtil.close(br);
			IoUtil.close(in);
		}
	}

	private static PackageBufINf deleteConfigFile(ManagerConnection c, ByteBuffer buffer,
												  byte packetId, String fileName) {
		File file = MycatServer.getConfigFile(fileName);
		PackageBufINf bufINf = new PackageBufINf();
		try {
			deleteFile(file, false);
			return showInfo(c, buffer, packetId, "Success: delete file '" + fileName+"'");
		} catch (ConfigException e) {
			log.warn("Delete file error", e);
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(e.toString(), c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
			bufINf.buffer = buffer;
			return bufINf;
		}
	}

	private static PackageBufINf listConfigFiles(ManagerConnection c,
			ByteBuffer buffer, byte packetId) {
		PackageBufINf bufINf = new PackageBufINf();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		try {
			int i = 1;
			String configDir = MycatServer.getDirectory("conf");
			File[] files = new File(configDir).listFiles();
			if (files == null) {
				throw new IOException("Access directory '"+configDir+"' failed");
			}
			for (File f : files) {
				if (f.isFile()) {
					RowDataPacket row = new RowDataPacket(FIELD_COUNT);
					String time = df.format(new Date(f.lastModified()));
					String s = String.format("%s: %s '%s'", i++, time, f.getName());
					row.add(StringUtil.encode(s, c.getCharset()));
					row.packetId = ++packetId;
					buffer = row.write(buffer, c,true);
				}
			}

			bufINf.buffer = buffer;
			bufINf.packetId = packetId;
			return bufINf;
		} catch (IOException e) {
            log.error("listConfigFiles error", e);
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(e.toString(), c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
			bufINf.buffer = buffer;
		}
		bufINf.packetId = packetId;
		return bufINf;
	}

}