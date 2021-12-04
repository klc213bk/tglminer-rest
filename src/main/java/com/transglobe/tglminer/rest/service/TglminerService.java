package com.transglobe.tglminer.rest.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.tglminer.rest.bean.EtlNameBo.ConsumerStatusEnum;
import com.transglobe.tglminer.rest.bean.EtlNameBo.WithSyncEnum;
import com.transglobe.tglminer.rest.bean.HealthTopicEnum;


@Service
public class TglminerService {
	static final Logger LOG = LoggerFactory.getLogger(TglminerService.class);

	private static final String CONSUMER_GROUP = "health";

	public static final String CLIENT_ID = "health-1";

	@Value("${tglminer.db.driver}")
	private String tglminerDbDriver;

	@Value("${tglminer.db.url}")
	private String tglminerDbUrl;

	@Value("${tglminer.db.username}")
	private String tglminerDbUsername;

	@Value("${tglminer.db.password}")
	private String tglminerDbPassword;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstapServer;


	//	@Value("${kafka.rest.url}")
	//	private String kafkaRestUrl;
	//
	@Value("${connector.name}")
	private String connectorName;
	//
	@Value("${health.etl.name}")
	private String healthEtlName;

	@Autowired
	KafkaService kafkaService;

	@Autowired
	HealthService healthService;


	public void runHealthService() throws Exception{
		LOG.info(">>>>>>>>>>>> runHealthService running ....");

		Connection conn = null;

		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl,tglminerDbUsername, tglminerDbPassword);
			//
			LOG.info(">>>> start health consumer ");
			healthService.startHealthConsumer();
			LOG.info(">>>> health consumer started!!!");

			// restart connector
			LOG.info(">>>> resetOffset add sync table");
			Set<String> tableSet = getLogminerTables(conn, healthEtlName);
			Map<String,String> configmap = kafkaService.getUpdatedConnectorConfigMap(connectorName, Boolean.TRUE, 1, tableSet) ;
				
			kafkaService.restartConnector(connectorName, configmap);
			
			
			//		LOG.info(">>>> connector status={}", status);

			//		LOG.info(">>>> start scheduler ...");
			//		startScheduler();
		} finally {
			if (conn != null) conn.close();
		}

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
	//	private String restartConnector(String etlName, Boolean resetOffset, Integer syncTableOption, ConsumerStatusEnum consumerStatus) throws Exception {
	//		Map<String,String> configmap = kafkaService.getConnectorConfig(connectorName);
	//		LOG.info(">>>> configmap={}", configmap);
	//
	//		if (Boolean.TRUE.equals(resetOffset)) {
	//			configmap.put("reset.offset", "true");
	//		} else {
	//			configmap.put("reset.offset", "false");
	//		}
	//
	//		Connection conn = null;
	//
	//		try {
	//			Class.forName(tglminerDbDriver);
	//			conn = DriverManager.getConnection(tglminerDbUrl,tglminerDbUsername, tglminerDbPassword);
	//
	//			final Set<String> tableSet = getLogminerTables(conn, etlName);
	//
	//			if (syncTableOption == 1) {
	//
	//				String newsyncTables = String.join(",", tableSet);	
	//				LOG.info(">>>> add sync table:{}", newsyncTables);
	//
	//				// "reset.offset", "table.whitelist"
	//				String newtableWhitelist = configmap.get("table.whitelist") + "," + newsyncTables;
	//				configmap.put("table.whitelist", newtableWhitelist);
	//
	//				updateETLName(conn, etlName, WithSyncEnum.SYNC, consumerStatus);
	//
	//
	//			} else if (syncTableOption == -1) {
	//				LOG.info(">>>> remove sync tableSet:{}", String.join(",", tableSet));
	//
	//				String[] tableArr = configmap.get("table.whitelist").split(",");
	//				List<String> tableList = Arrays.asList(tableArr);
	//				LOG.info(">>>> existing sync tableList:{}", String.join(",", tableList));
	//
	//				String newtableWhitelist = tableList.stream().filter(s -> !tableSet.contains(s)).collect(Collectors.joining(","));
	//				LOG.info(">>>> new newtableWhitelist={}", newtableWhitelist);
	//
	//				configmap.put("table.whitelist", newtableWhitelist);
	//
	//				updateETLName(conn, etlName, WithSyncEnum.NO_SYNC, consumerStatus);
	//
	//			} 
	//
	//			LOG.info(">>>> new configmap={}", configmap);
	//
	//			String status = doRestartConnector(connectorName, configmap);
	//
	//			updateLogminerConnectorStatus(conn,status);		
	//
	//			return status;
	//
	//		} finally {
	//			if (conn != null) conn.close();
	//		}
	//
	//
	//	}
	private void updateETLName(Connection conn, String etlName, WithSyncEnum withSyncEnum, ConsumerStatusEnum consumerStatus) throws Exception {
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


	//




	private void updateLogminerConnectorStatus(Connection conn, String status) throws Exception {

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
	private Set<String> getLogminerTables(Connection conn, String etlName) throws Exception {

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
