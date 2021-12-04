package com.transglobe.tglminer.rest.controller;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.tglminer.rest.service.InitService;



@RestController
@RequestMapping("/init")
public class InitController {
	static final Logger LOG = LoggerFactory.getLogger(InitController.class);

	
	@Autowired
	private InitService initService;
	
	@Autowired
	private ObjectMapper mapper;
	
	@PostMapping(path="/cleanup", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> cleanup() {
		LOG.info(">>>>controller cleanup is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			initService.cleanup();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		LOG.info(">>>>controller cleanup finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/initialize", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> initialize() {
		LOG.info(">>>>controller initialize is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			initService.initialize();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>>errMsg:{}, stack trace={}", errMsg,stackTrace);
		}
		
		LOG.info(">>>>controller initialize finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
}
