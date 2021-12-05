package com.transglobe.tglminer.rest.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.tglminer.rest.bean.HealthTopicEnum;



@Service
public class HealthService {
	static final Logger LOG = LoggerFactory.getLogger(HealthService.class);

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
	private String kafkaBootstrapServer;

	private BasicDataSource tglminerConnPool;

	private ExecutorService executor = null;

	private KafkaHealthConsumer consumer = null;
	
	private ExecutorService heartbeatExecutor;

	public boolean startHealthConsumer() throws Exception {
		LOG.info(">>>>>>>>>>>> startHealthConsumer...");
		boolean result = true;

		if (tglminerConnPool == null) {
			tglminerConnPool = getConnectionPool();
		}
		
		List<String> topicList = new ArrayList<>();
		topicList.add(HealthTopicEnum.HEARTBEAT.getTopic());

		executor = Executors.newFixedThreadPool(1);

		//		String groupId1 = config.groupId1;
		consumer = new KafkaHealthConsumer(CLIENT_ID, CONSUMER_GROUP, kafkaBootstrapServer, topicList, tglminerConnPool);
		executor.submit(consumer);

		while (!consumer.consumerStarted()) {
			LOG.info(">>>>>>WAITING 1 sec FOR FINISH");
			Thread.sleep(1000);
		}
		
		LOG.info(">>>>>>>>>>>> startHealthConsumer Done!!!");

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {

				stopHealthConsumer();

			}
		});

		return result;


	}
	public boolean startHeartbeat() throws Exception {
		LOG.info(">>>>>>>>>>>> startHeartbeat...");
		boolean result = true;

		if (tglminerConnPool == null) {
			tglminerConnPool = getConnectionPool();
		}

		heartbeatExecutor = Executors.newSingleThreadExecutor();
		heartbeatExecutor.submit(new Runnable() {

			@Override
			public void run() {
				
				try {
					while (true) {
						sendHeartbeat();
						Thread.sleep(60000);
					}
				} catch (Exception e) {
					LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}
			}
			private Long sendHeartbeat() throws Exception{
				Connection conn = null;
				CallableStatement cstmt = null;

				try {	
					conn = tglminerConnPool.getConnection();
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
		});
		

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {

				stopHeartbeat();

			}
		});

		return result;


	}

	public boolean stopHealthConsumer() {
		LOG.info(">>>>>>>>>>>> stopHealthConsumer ");
		boolean result = true;
		if (executor != null && consumer != null) {
			consumer.shutdown();

			try {
				if (tglminerConnPool != null) tglminerConnPool.close();
			} catch (Exception e) {
				result = false;
				LOG.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}

			executor.shutdown();
			if (!executor.isTerminated()) {
				executor.shutdownNow();

				try {
					executor.awaitTermination(300, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					result = false;
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}
			}
		}
		LOG.info(">>>>>>>>>>>> stopHealthConsumer done !!!");

		return result;
	}
	public boolean stopHeartbeat() {
		LOG.info(">>>>>>>>>>>> stopHeartbeat ");
		boolean result = true;
		if (heartbeatExecutor != null) {

			try {
				if (tglminerConnPool != null) tglminerConnPool.close();
			} catch (Exception e) {
				result = false;
				LOG.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}

			heartbeatExecutor.shutdown();
			if (!heartbeatExecutor.isTerminated()) {
				heartbeatExecutor.shutdownNow();

				try {
					heartbeatExecutor.awaitTermination(300, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					result = false;
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}

		}

		LOG.info(">>>>>>>>>>>> stopHeartbeat done !!!");

		return result;
	}
	
	private BasicDataSource getConnectionPool() {
		BasicDataSource connPool = new BasicDataSource();
		connPool.setUrl(tglminerDbUrl);
		connPool.setDriverClassName(tglminerDbDriver);
		connPool.setUsername(tglminerDbUsername);
		connPool.setPassword(tglminerDbPassword);
		connPool.setMaxTotal(2);
		
		return connPool;
	}
	
	
	
//	public void loadData() throws Exception{
//		Connection conn = null;
//		PreparedStatement pstmt = null;
//		ResultSet rs = null;
//		String sql = null;
//		try {	
//			Class.forName(logminerDbDriver);
//			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
//			
//			sql = "truncate table HE_HEALTH_SINK";
//			pstmt = conn.prepareStatement(sql);
//			pstmt.executeUpdate();
//			pstmt.close();
//			
//			sql = "select HEARTBEAT_TIME,ORA_ROWSCN, ROWID from HE_HEARTBEAT";
//
//			pstmt = conn.prepareStatement(sql);
//			rs= pstmt.executeQuery();
//			List<Timestamp> heartbeatList = new ArrayList<Timestamp>();
//			List<Long> scnList = new ArrayList<Long>();
//			List<String> rowIdList = new ArrayList<String>();
//			while (rs.next()) {
//				heartbeatList.add(rs.getTimestamp("HEARTBEAT_TIME"));
//				scnList.add(rs.getLong("ORA_ROWSCN"));
//				rowIdList.add(rs.getString("ROWID"));
//			}
//			rs.close();
//			pstmt.close();
//
//			for (int i = 0; i < heartbeatList.size(); i++ ) {
//				Timestamp currTs = new Timestamp(System.currentTimeMillis());
//				sql = "insert into HE_HEALTH_SINK (HEARTBEAT_TIME,INSERT_TIMESTAMP,UPDATE_TIMESTAMP,SCN,ROW_ID)\n" + 
//						" values (?,?,?,?,?)\n";
//				
//				pstmt = conn.prepareStatement(sql);
//				pstmt.setTimestamp(1, heartbeatList.get(i));
//				pstmt.setTimestamp(2, currTs);
//				pstmt.setTimestamp(3, currTs);
//				pstmt.setLong(4, scnList.get(i));
//				pstmt.setString(5, rowIdList.get(i));
//				
//				pstmt.execute();
//
//				pstmt.close();
//				
//			}
//			conn.close();
//			
//		} catch (Exception e1) {
//
//			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));
//
//			throw e1;
//		} finally {
//			if (rs != null) rs.close();
//			if (pstmt != null) pstmt.close();
//			if (conn != null) conn.close();
//		}
//	}
	public void logEtlStates(String etlName) throws Exception{

		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		try {	
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			cstmt = conn.prepareCall("{call SP_UPD_ETL_STATES(?,?)}");
			cstmt.setString(1, etlName);
			cstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			cstmt.execute();

			cstmt.close();
			conn.close();


		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (cstmt != null) cstmt.close();
			if (conn != null) conn.close();
		}
	}
	public boolean isConsumerClosed() throws Exception {
		LOG.info(">>>>>>>>>>>> isConsumerClosed ");
		
		if (executor == null || executor.isTerminated()) {
			return true;
		} else {
			if (consumer == null) {
				return true;
			} else {
				return consumer.isConsumerClosed();
			}
		}

	}

}
