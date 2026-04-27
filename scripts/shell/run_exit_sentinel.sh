#!/bin/bash
cd /Users/eudis/ths
/usr/bin/python3 -m quant_core.execution.exit_sentinel "$@" >> exit_sentinel.log 2>&1
