#!/bin/bash

java -jar -Dspring.profiles.active=scheduling target/streamingetl-scheduling-1.0.jar --spring.config.location=file:config/
