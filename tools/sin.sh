#!/bin/bash

while(true); do echo "sin `perl -e 'print int(sin(time()/10.0)*10.0+10)'` `date +%s`" | nc localhost 2003; sleep 0.5; done

