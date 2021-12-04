package com.transglobe.tglminer.rest.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.tglminer.rest.service.HealthService;
import com.transglobe.tglminer.rest.service.KafkaService;
import com.transglobe.tglminer.rest.service.TglminerService;
import com.transglobe.tglminer.rest.util.HttpUtils;

@RestController
@RequestMapping("/tglminer")
public class TglminerController {
	static final Logger LOG = LoggerFactory.getLogger(TglminerController.class);

	@Autowired
	private TglminerService tglminerService;
	
	@Autowired
	private HealthService healthService;
	
	@Autowired
	private KafkaService kafkaService;
	
	@Autowired
	private ObjectMapper mapper;
	
	
	
	@PostMapping(path="/runHealthService", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> runHealthService() {
		LOG.info(">>>>controller runHealthService is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			tglminerService.runHealthService();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		LOG.info(">>>>controller runHealthService finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(value="/sendHeartbeat")
	@ResponseBody
	public ResponseEntity<Object> sendHeartbeat() throws Exception{
		
		LOG.info(">>>>controller sendHeartbeat is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		Long hbtime = null;
		try {
			hbtime = healthService.sendHeartbeat();
			objectNode.put("returnCode", "0000");
			objectNode.put("heartbeatTime", hbtime);
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		LOG.info(">>>>controller sendHeartbeat finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	
	}
	@GetMapping(path="/listTopics", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> listTopics() {
		LOG.info(">>>>controller listTopics is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
	
		try {
			Set<String> topics = kafkaService.listTopics();
			List<String> topicList = new ArrayList<>();
			for (String t : topics) {
				topicList.add(t);
			}
			String jsonStr = HttpUtils.writeListToJsonString(topicList);
			
			mapper.createArrayNode().add("ggg");
			objectNode.put("returnCode", "0000");
			objectNode.put("topics", jsonStr);
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		LOG.info(">>>>controller listTopics finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
}
