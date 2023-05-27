#!/bin/sh

# N f B K
echo "start.sh <N> <F> <B> <K> <P>"

python3 crypto/run_trusted_key_gen.py --N $1 --f $2

killall python3
which python3

i=0
while [ "$i" -lt $1 ]; do
    echo "start node $i..."
    if [ "$5" = "dl" ]; then
      python3 nodes/run_sockets_node.py --sid 'sidA' --id $i --N "$1" --f "$2" --B "$3" --K "$4" --S 100 --P "$5" --D True --O True &
    else
      python3 nodes/run_socket_node.py --sid 'sidA' --id $i --N "$1" --f "$2" --B "$3" --K "$4" --S 50 --T 1 --F 1000000 --P "$5" --D True --O True &
    fi
    i=$(( i + 1 ))
done 
