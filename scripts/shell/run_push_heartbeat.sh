#!/bin/bash
cd /Users/eudis/ths
/usr/bin/python3 -m quant_core.execution.pushplus_tasks heartbeat >> push_heartbeat.log 2>&1
