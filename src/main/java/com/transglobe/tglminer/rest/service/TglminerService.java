package com.transglobe.tglminer.rest.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.tglminer.rest.bean.EtlNameBo.ConsumerStatusEnum;
import com.transglobe.tglminer.rest.bean.EtlNameBo.WithSyncEnum;
import com.transglobe.tglminer.rest.util.HttpUtils;


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

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstapServer;

	@Value("${logminer.rest.url}")
	private String logminerResturl;

	@Value("${partycontact.rest.url}")
	private String partycontactRestUrl;
	
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
			if (healthService.isConsumerClosed()) {
				healthService.startHealthConsumer();
				LOG.info(">>>> health consumer is started!!!");
			} else {
				LOG.info(">>>> health consumer IS ALREADY STARTED.!!!");
			}

			LOG.info(">>>> applyLogminerSync etlname={}", healthEtlName);
			applyLogminerSync(healthEtlName);

			LOG.info(">>>> start heratbeat ");
			healthService.startHeartbeat();
			LOG.info(">>>> heratbeat started!!!");

		} finally {
			if (conn != null) conn.close();
		}

		LOG.info(">>>>>>>>>>>> runHealth running end");
	}
	public void stopHealthService() throws Exception{
		LOG.info(">>>>>>>>>>>> runHealthService running ....");

		Connection conn = null;

		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl,tglminerDbUsername, tglminerDbPassword);
			
			LOG.info(">>>> stop heratbeat ");
			healthService.stopHeartbeat();
			LOG.info(">>>> stop heratbeat end!!!");
			
			LOG.info(">>>> dropLogminerSync etlname={}", healthEtlName);
			dropLogminerSync(healthEtlName);
			
			//
			LOG.info(">>>> stop health consumer ");
			if (!healthService.isConsumerClosed()) {
				healthService.stopHealthConsumer();
				LOG.info(">>>> health consumer is closed!!!");
			} else {
				LOG.info(">>>> health consumer IS ALREADY CLOSED.!!!");
			}

			

			

		} finally {
			if (conn != null) conn.close();
		}

		LOG.info(">>>>>>>>>>>> runHealth running end");
	}
	public String applyLogminerSync(String etlName) throws Exception {
		Connection conn = null;

		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl,tglminerDbUsername, tglminerDbPassword);

			// restart connector
			LOG.info(">>>> resetOffset add sync table");
			Set<String> tableSet = getLogminerTables(conn, etlName);
			LOG.info(">>>> tableSet={}", String.join(",", tableSet));
			Map<String,String> configmap = kafkaService.getUpdatedConnectorConfigMap(connectorName, Boolean.TRUE, WithSyncEnum.SYNC, tableSet) ;
			String restartConnectorStatus = kafkaService.restartConnector(connectorName, configmap);
			LOG.info(">>>> connector status={}", restartConnectorStatus);

			String tableWhitelist = configmap.get("table.whitelist");
			String kafkaTopics = String.join(",", kafkaService.listTopics());
			LOG.info(">>>> tableWhitelist={}, kafkaTopics={}",tableWhitelist,  kafkaTopics);

			//update connector status
			updateLogminerConnectorStatus(conn, tableWhitelist,kafkaTopics, restartConnectorStatus);

			// update ETL status
			updateETLName(conn, etlName, WithSyncEnum.SYNC, ConsumerStatusEnum.STARTED);
			LOG.info(">>>> update etl name sync={}, consumerstatus={}", WithSyncEnum.SYNC, ConsumerStatusEnum.STARTED);

			return restartConnectorStatus;

		} finally {
			if (conn != null) conn.close();
		}
	}
	public void checkToRestart()  throws Exception {
		LOG.info(">>>> checkToRestart ...");

		Connection conn = null;
		CallableStatement cstmt = null;
		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl,tglminerDbUsername, tglminerDbPassword);

			cstmt = conn.prepareCall("{call GET_STREAMING_ETL_STATE(?,?)}");
			cstmt.setString(1,  "PARTY_CONTACT");
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.execute();
			String state = cstmt.getString(2);
			
			String response = "";
			LOG.info(">>>> health status:{}", state);
			if (StringUtils.equalsIgnoreCase("STANDBY",state)) {
				LOG.info(">>>> stop party contact");
				String stopPartycontactUrl = partycontactRestUrl + "/partycontact/stopPartyContact";
				LOG.info(">>>>>>> stopPartycontactUrl={}", stopPartycontactUrl); 
				response = HttpUtils.restService(stopPartycontactUrl, "POST");
				
				LOG.info(">>>> stop health service");
				stopHealthService();
				
				LOG.info(">>>> stop logminer connector");
				String stopConnectorUrl = logminerResturl + "/logminer/stopConnector";
				HttpUtils.restService(stopConnectorUrl, "POST");
				
				Thread.sleep(20000);
				
				LOG.info(">>>> start logminer connector");
				String startConnectorUrl = logminerResturl + "/logminer/startConnector";
				HttpUtils.restService(startConnectorUrl, "POST");
				

				LOG.info(">>>> runHealthService");
				runHealthService();
				
				LOG.info(">>>> run party contact");
				String runPartycontactUrl = partycontactRestUrl + "/partycontact/runPartyContact";
				LOG.info(">>>>>>> runPartycontactUrl={}", runPartycontactUrl); 
				response = HttpUtils.restService(runPartycontactUrl, "POST");
				
				LOG.info(">>>> checkToRestart .done !!!!..");
			} else {
				LOG.info(">>>> VERY HEATHY !! No need to restart");
			}
		} finally {
			if (conn != null) conn.close();
		}
		
		
		
		
		
		
		
		
		
		
		LOG.info(">>>> restarting .Done!!!!..");
	}
	public String dropLogminerSync(String etlName) throws Exception {
		Connection conn = null;

		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl,tglminerDbUsername, tglminerDbPassword);

			// restart connector
			LOG.info(">>>> resetOffset add sync table");
			Set<String> tableSet = getLogminerTables(conn, etlName);
			LOG.info(">>>> tableSet={}", String.join(",", tableSet));
			Map<String,String> configmap = kafkaService.getUpdatedConnectorConfigMap(connectorName, Boolean.FALSE, WithSyncEnum.DROP_SYNC, tableSet) ;
			String restartConnectorStatus = kafkaService.restartConnector(connectorName, configmap);
			LOG.info(">>>> drop sync connector status={}", restartConnectorStatus);


			String tableWhitelist = configmap.get("table.whitelist");
			String kafkaTopics = String.join(",", kafkaService.listTopics());
			LOG.info(">>>> tableWhitelist={}, kafkaTopics={}",tableWhitelist,  kafkaTopics);

			//update connector status
			updateLogminerConnectorStatus(conn, tableWhitelist,kafkaTopics, restartConnectorStatus);

			// update ETL status
			updateETLName(conn, etlName, WithSyncEnum.DROP_SYNC, ConsumerStatusEnum.STOPPED);
			LOG.info(">>>> update etl name sync={}, consumerstatus={}", WithSyncEnum.SYNC, ConsumerStatusEnum.STARTED);

			return restartConnectorStatus;

		} finally {
			if (conn != null) conn.close();
		}
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




	private void updateLogminerConnectorStatus(Connection conn, String tableWhitelist, String kafkaTopics, String restartConnectorStatus) throws Exception {

		PreparedStatement pstmt = null;
		String sql = null;

		try {

			sql = "update TM_LOGMINER_OFFSET SET TABLE_WHITE_LIST=?,KAFKA_TOPICS=?,STATUS=? where start_time = \n" +
					" (select start_time from TM_LOGMINER_OFFSET order by start_time desc \n" +
					" fetch next 1 row only)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, tableWhitelist);
			pstmt.setString(2, kafkaTopics);
			pstmt.setString(3, restartConnectorStatus);
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
