#!/bin/bash

java -jar -Dspring.profiles.active=tglminer target/tglminer-rest-1.0.jar --spring.config.location=file:config/ 
