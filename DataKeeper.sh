#!/bin/bash

declare -a Ports
Ports[0]=21210
Ports[1]=21220
Ports[2]=21230

for((i=0 ; i<3 ; i++)); do 

    ./DataKeeper.py $(hostname -I) ${Ports[i]} $((${Ports[i]}+1)) $((${Ports[i]}+2)) &

done