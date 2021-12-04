package com.transglobe.tglminer.rest.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;


public class DbUtils {

	
	public static void executeScript(Connection conn, String script) throws Exception {

		Statement stmt = null;
		try {

			stmt = conn.createStatement();
			stmt.executeUpdate(script);
			stmt.close();

		} finally {
			if (stmt != null) stmt.close();
		}

	}
	public static void executeSqlScriptFromFile(Connection conn, String file) throws Exception {
		
		Statement stmt = null;
		try {

			ClassLoader loader = Thread.currentThread().getContextClassLoader();	
			try (InputStream inputStream = loader.getResourceAsStream(file)) {
				String createScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
				stmt = conn.createStatement();
				stmt.executeUpdate(createScript);
				stmt.close();
			} catch (SQLException | IOException e) {
				if (stmt != null) stmt.close();
				throw e;
			}

		
		} finally {
			if (stmt != null) stmt.close();
		}
	}
}
