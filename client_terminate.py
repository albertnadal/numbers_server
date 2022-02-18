#!/usr/bin/python3
# ----------------------------------------------------------------------
# File     : client_terminate.py
# Abstract : TCP socket client for sending the 'terminate\n' sequence.
# Author   : Albert Nadal Garriga (anadalg@gmail.com)
# Date     : Thu, 17 Feb 20:51 +0100
# ----------------------------------------------------------------------

import socket

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sock.connect(('localhost', 4000))
    print("Connected. Sending 'terminate'...", flush=True)

    try:
        sock.sendall(b'terminate\n')
        while True:
            data = sock.recv(1024)
            if not data:
                print("Connection closed remotely.", flush=True)
                break
    except (ConnectionResetError, BrokenPipeError) as ex:
        print("Connection closed remotely.", flush=True)
    finally:
        sock.close()

except ConnectionRefusedError as ex:
    print("Connection refused. Is the server running?", flush=True)

