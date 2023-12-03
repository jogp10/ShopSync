#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <port1> [<port2> <port3> ...]"
    exit 1
fi

# Loop through each port number provided as arguments
for port in "$@"; do
    # Start python node.py with the specified port in a new terminal
    gnome-terminal -- python3 node.py "$port" &
done

# Sleep to allow time for nodes to start (adjust as needed)
sleep 5

# Run server.py in a new terminal
gnome-terminal -- python3 server.py