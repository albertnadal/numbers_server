#!/usr/bin/python3
# ----------------------------------------------------------------------
# File     : client_numbers.py
# Abstract : TCP socket client for sending random numbers of fixed size.
# Author   : Albert Nadal Garriga (anadalg@gmail.com)
# Date     : Thu, 17 Feb 20:26 +0100
# ----------------------------------------------------------------------

import socket
import random

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sock.connect(('localhost', 4000))
    print("Connected. Sending numbers...", flush=True)

    while True:
        try:
            # Generate and send a random generated number with LF escape sequence
            number_str = str(random.randint(0, 999999999)).zfill(9)+"\n"
            sock.sendall(bytearray(number_str.encode()))
        except (ConnectionResetError, BrokenPipeError) as ex:
            print("Connection closed remotely.", flush=True)
            sock.close()
            exit(0)

except ConnectionRefusedError as ex:
    print("Connection refused. Is the server running?", flush=True)
except KeyboardInterrupt as ex:
    print("Disconnecting...", flush=True)
    sock.shutdown(socket.SHUT_WR)
    sock.close()

