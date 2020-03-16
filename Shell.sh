./DataKeeper.py $(hostname -i) 3000 3002 > /dev/null 2>&1 &
./DataKeeper.py $(hostname -i) 3001 3003 > /dev/null 2>&1 &

./DataKeeper.py $(hostname -I) 6000 6002 > /dev/null 2>&1 &
./DataKeeper.py $(hostname -I) 6001 3003 > /dev/null 2>&1 &

./Alive.py > /dev/null 2>&1 &

./master_tracker.py 9027 > /dev/null 2>&1 &
./master_tracker.py 9028 > /dev/null 2>&1 &
./master_tracker.py 9029 > /dev/null 2>&1 &