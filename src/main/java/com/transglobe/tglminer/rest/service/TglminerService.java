package com.transglobe.tglminer.rest.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.tglminer.rest.bean.EtlNameBo.ConsumerStatusEnum;
import com.transglobe.tglminer.rest.bean.EtlNameBo.WithSyncEnum;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.tglminer.rest.bean.TglminerSPEnum;
import com.transglobe.tglminer.rest.bean.TglminerTableEnum;


@Service
public class TglminerService {
	static final Logger LOG = LoggerFactory.getLogger(TglminerService.class);

	@Value("${tglminer.db.driver}")
	private String tglminerDbDriver;

	@Value("${tglminer.db.url}")
	private String tglminerDbUrl;

	@Value("${tglminer.db.username}")
	private String tglminerDbUsername;

	@Value("${tglminer.db.password}")
	private String tglminerDbPassword;

	@Value("${kafka.rest.url}")
	private String kafkaRestUrl;

	@Value("${health.rest.url}")
	private String healthRestUrl;

	@Value("${connector.name}")
	private String connectorName;

	@Value("${health.etl.name}")
	private String healthEtlName;

	@Autowired
	KafkaService kafkaService;

	public void cleanup() throws Exception{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			// drop user SP
			Set<String> spSet = new HashSet<>();
			for (TglminerSPEnum e : TglminerSPEnum.values()) {
				spSet.add(e.getSpName());
			}

			sql = "select OBJECT_NAME from dba_objects where object_type = 'PROCEDURE' and owner = 'TGLMINER'";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				String sp = rs.getString("OBJECT_NAME");
				if (spSet.contains(sp)) {
					executeScript(conn, "DROP PROCEDURE " + sp);
					LOG.info(">>> SP={} dropped", sp);
				}
			}
			rs.close();
			pstmt.close();

			// drop user tables
			Set<String> tbSet = new HashSet<>();
			for (TglminerTableEnum tableEnum : TglminerTableEnum.values()) {
				tbSet.add(tableEnum.getTableName());
			}

			// drop user tables
			sql = "select TABLE_NAME from USER_TABLES";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				String table = rs.getString("TABLE_NAME");
				if (tbSet.contains(table)) {
					executeScript(conn, "DROP TABLE " + table);
					LOG.info(">>> table={} dropped", table); 
				}
			}
			pstmt.close();

		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}

	}
	public void initialize() throws Exception{
		Connection conn = null;

		CallableStatement cstmt = null;
		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);
			conn.setAutoCommit(false);

			for (TglminerTableEnum e : TglminerTableEnum.values()) {
				LOG.info(">>>>>>> create TABLE file {}",e.getScriptFile());
				executeSqlScriptFromFile(conn, e.getScriptFile());
			}
			conn.commit();

			for (TglminerSPEnum e : TglminerSPEnum.values()) {
				LOG.info(">>>>>>> create SP file {}",e.getScriptFile());
				executeSqlScriptFromFile(conn, e.getScriptFile());
			}
			conn.commit();


		} finally {
			if (cstmt != null) cstmt.close();
			if (conn != null) conn.close();
		}

	}
	public void runHealthService() throws Exception{
		LOG.info(">>>>>>>>>>>> runHealthService running ....");

		String urlStr = healthRestUrl + "/startHealthConsumer";
		LOG.info(">>>> start health consumer url={}", urlStr);
		HttpURLConnection httpConn = null;
		URL url = null;
		try {
			url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("POST");
			int responseCode = httpConn.getResponseCode();
			//			LOG.info(">>>>>  responseCode={}",responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
			StringBuffer response = new StringBuffer();
			String readLine = null;
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> kafkaRestService responsecode={}, response={}", responseCode, response.toString());

		} finally {
			if (httpConn != null ) httpConn.disconnect();
		}

		Thread.sleep(20000);

		// restart connector
		LOG.info(">>>> resetOffset add sync table");
		String status = restartConnector(healthEtlName, Boolean.TRUE, 1, ConsumerStatusEnum.STARTED);
		LOG.info(">>>> connector status={}", status);

		//		LOG.info(">>>> start scheduler ...");
		//		startScheduler();


		LOG.info(">>>>>>>>>>>> runHealth running end");
	}
	/**
	 * 
	 * @param etlName
	 * @param resetOffset
	 * @param syncTableOption, 0: no change, 1 add sync tables, -1 remove sync tables
	 * @return
	 * @throws Exception
	 */
	public String restartConnector(String etlName, Boolean resetOffset, Integer syncTableOption, ConsumerStatusEnum consumerStatus) throws Exception {
		Map<String,String> configmap = kafkaService.getConnectorConfig(connectorName);
		LOG.info(">>>> configmap={}", configmap);

		if (Boolean.TRUE.equals(resetOffset)) {
			configmap.put("reset.offset", "true");
		} else {
			configmap.put("reset.offset", "false");
		}

		Connection conn = null;

		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl,tglminerDbUsername, tglminerDbPassword);

			final Set<String> tableSet = getLogminerTables(conn, etlName);

			if (syncTableOption == 1) {

				String newsyncTables = String.join(",", tableSet);	
				LOG.info(">>>> add sync table:{}", newsyncTables);

				// "reset.offset", "table.whitelist"
				String newtableWhitelist = configmap.get("table.whitelist") + "," + newsyncTables;
				configmap.put("table.whitelist", newtableWhitelist);

				updateETLName(conn, etlName, WithSyncEnum.SYNC, consumerStatus);


			} else if (syncTableOption == -1) {
				LOG.info(">>>> remove sync tableSet:{}", String.join(",", tableSet));

				String[] tableArr = configmap.get("table.whitelist").split(",");
				List<String> tableList = Arrays.asList(tableArr);
				LOG.info(">>>> existing sync tableList:{}", String.join(",", tableList));

				String newtableWhitelist = tableList.stream().filter(s -> !tableSet.contains(s)).collect(Collectors.joining(","));
				LOG.info(">>>> new newtableWhitelist={}", newtableWhitelist);

				configmap.put("table.whitelist", newtableWhitelist);

				updateETLName(conn, etlName, WithSyncEnum.NO_SYNC, consumerStatus);

			} 

			LOG.info(">>>> new configmap={}", configmap);

			String status = doRestartConnector(connectorName, configmap);

			updateLogminerConnectorStatus(conn,status);		

			return status;
			
		} finally {
			if (conn != null) conn.close();
		}


	}
	public void updateETLName(Connection conn, String etlName, WithSyncEnum withSyncEnum, ConsumerStatusEnum consumerStatus) throws Exception {
		PreparedStatement pstmt = null;
		String sql = null;
		try {

			sql = "update TM_ETL_NAME set WITH_SYNC=?, CONSUMER_STATUS=? where ETL_NAME=?";
			pstmt = conn.prepareStatement(sql);

			pstmt.setInt(1,  (WithSyncEnum.SYNC == withSyncEnum)? 1 : 0);
			pstmt.setString(2,  consumerStatus.name());
			pstmt.setString(3,  etlName);
			pstmt.executeUpdate();
			pstmt.close();

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	public Long sendHeartbeat() throws Exception{
		Connection conn = null;
		CallableStatement cstmt = null;

		try {	
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			cstmt = conn.prepareCall("{call SP_INS_HEALTH_HEARTBEAT(?)}");

			long currMillis = System.currentTimeMillis();
			cstmt.setTimestamp(1, new Timestamp(currMillis));
			cstmt.execute();

			return currMillis;

		} catch (Exception e1) {
			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));
			throw e1;
		} finally {
			if (cstmt != null) cstmt.close();
			if (conn != null) conn.close();
		}

	}
	//
	private String doRestartConnector(String connectorName, Map<String,String> configmap) throws Exception {
		LOG.info(">>>> restartConnector...");

		LOG.info(">>>> pause connector");
		kafkaService.pauseConnector(connectorName);

		LOG.info(">>>> delete connector");
		kafkaService.deleteConnector(connectorName);

		LOG.info(">>>> add sync table to config's whitelist");

		LOG.info(">>>> create connector");
		boolean result = kafkaService.createConnector(connectorName, configmap);
		LOG.info(">>>> create connector result={}", result);

		String status = kafkaService.getConnectorStatus(connectorName);

		LOG.info(">>>> connector status={}", status);

		return status;
	}

	private void executeSqlScriptFromFile(Connection conn, String file) throws Exception {
		LOG.info(">>>>>>>>>>>> executeSqlScriptFromFile file={}", file);

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

			LOG.info(">>>>>>>>>>>> executeSqlScriptFromFile Done!!!");

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();
		}
	}
	private void executeScript(Connection conn, String script) throws Exception {

		Statement stmt = null;
		try {

			stmt = conn.createStatement();
			stmt.executeUpdate(script);
			stmt.close();

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();

		}

	}
	public void updateLogminerConnectorStatus(Connection conn, String status) throws Exception {

		PreparedStatement pstmt = null;
		String sql = null;

		try {

			sql = "update TM_LOGMINER_OFFSET SET STATUS=? where start_time = \n" +
					" (select start_time from TM_LOGMINER_OFFSET order by start_time desc \n" +
					" fetch next 1 row only)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, status);
			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	public Set<String> getLogminerTables(Connection conn, String etlName) throws Exception {

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		Set<String> tableSet = new HashSet<>();
		try {

			sql = "select TABLE_NAME from TM_LOGMINER_TABLE where ETL_NAME=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, etlName);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				tableSet.add(rs.getString("TABLE_NAME"));
			}
			pstmt.close();

			return tableSet;
		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}

	}
}
