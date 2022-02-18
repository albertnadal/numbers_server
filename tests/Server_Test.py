#!/usr/bin/python3
# ----------------------------------------------------------------------
# File     : Server_Test.py
# Abstract : Integration tests for the numbers server application.
# Author   : Albert Nadal Garriga (anadalg@gmail.com)
# Date     : Fri, 18 Feb 04:11 +0100
# ----------------------------------------------------------------------

import unittest
import subprocess
import signal
import sys
import time
import socket
import random
from os.path import dirname, abspath, join
from os import killpg, getpgid, setsid

# Add the project root folder to the Python search module path
server_path = dirname(abspath(join(__file__, "..")))
sys.path.append(server_path)

from server import ADDR, PORT, MAX_CONNECTIONS, LOG_FILENAME, REPORT_PERIODICITY

class Integration(unittest.TestCase):

    server_process = None

    def setUp(self):
        self.start_server_process()

    def tearDown(self):
        self.kill_server_process()

    def start_server_process(self):
        server_script_path = join(server_path, 'server.py')
        cmd = sys.executable + ' ' + server_script_path + ' > /dev/null 2> /dev/null &'
        self.server_process = subprocess.Popen(cmd, shell=True, preexec_fn=setsid)

        # Wait 3 seconds to let the server launch and start listening for incoming connections.
        time.sleep(3)

    def kill_server_process(self):
        if self.server_process is not None:
            killpg(getpgid(self.server_process.pid), signal.SIGTERM)
            self.server_process.wait()
        self.server_process = None

    def test_GivenServerIsRunning_WhenConnectingToTheServerAddress_ThenConnectionSucceeds(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((ADDR, PORT))
            sock.close()
            time.sleep(1)
        except ConnectionRefusedError as ex:
            self.fail("Cannot connect to the server. The server is not accepting connections on %s:%d"%(ADDR, PORT))

    def test_GivenServerIsRunning_ThenTheServerAllowMultipleLiveConnections(self):
        sockets = []

        # Perform the max number of allowed connections to the server.
        for i in range(MAX_CONNECTIONS):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((ADDR, PORT))
                sockets.append(sock)
                time.sleep(1)
            except ConnectionRefusedError as ex:
                self.fail("Cannot connect to the server. The server does not allow the max amount (%d) of live connections on %s:%d"%(MAX_CONNECTIONS, ADDR, PORT))

        # Once we performed the max allowed connections then send a valid number for each active connection
        # to validate that all the connections are still alive and working.
        for sock in sockets:
            try:
                number_str = str(random.randint(0, 999999999)).zfill(9)+"\n"
                sock.sendall(bytearray(number_str.encode()))
            except ConnectionResetError as ex:
                self.fail("Connection closed remotely. The server closed an allowed connection.")
                sock.close()

        # Close all the connections.
        for sock in sockets:
            sock.close()

    def test_GivenServerIsRunning_WhenReachingTheMaxAllowedActiveConnections_ThenNewConnectionsAreAutomaticallyClosed(self):
        sockets = []

        # Perform the max number of allowed connections to the server.
        for i in range(MAX_CONNECTIONS):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((ADDR, PORT))
                sockets.append(sock)
                time.sleep(1)
            except ConnectionRefusedError as ex:
                self.fail("Cannot connect to the server. The server does not allow the max amount (%d) of live connections on %s:%d"%(MAX_CONNECTIONS, ADDR, PORT))

        # Perform an extra connection to exceed the total amount of allowed live connections. This extra connection
        # must be accepted and automatically closed by the server right after the connection has been stablished.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ADDR, PORT))
        sockets.append(sock)
        time.sleep(1)

        # A BrokenPipeError is expected if the server closed the connection
        with self.assertRaises(BrokenPipeError) as context:
            while True:
                number_str = str(random.randint(0, 999999999)).zfill(9)+"\n"
                sock.sendall(bytearray(number_str.encode()))

        # Close all the connections.
        for sock in sockets:
            sock.close()

    def test_GivenServerIsRunning_WhenClientSendAnInvalidNumberSequence_ThenTheServerDisconnectsTheClient(self):
        invalid_sequences = ['1234', '12345678\n', '1234b6789\n', '123456789', ' ab$-6qT:\n']

        for invalid_sequence in invalid_sequences:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ADDR, PORT))
            time.sleep(1)

            # A ConnectionResetError is expected if the server closes the connection due to an invalid number sequence
            with self.assertRaises(ConnectionResetError) as context:
                while True:
                    sock.sendall(bytearray(invalid_sequence.encode()))

            sock.close()

    def test_GivenServerIsRunning_WhenClientSendValidNumberSequences_ThenTheServerKeepsTheClientConnected(self):
        valid_sequences = ['000000001\n', '123456789\n', '037209858\n', '111111111\n', '999999999\n']

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ADDR, PORT))
        time.sleep(1)

        for valid_sequence in valid_sequences:
            try:
                sock.sendall(bytearray(valid_sequence.encode()))
            except (ConnectionResetError, BrokenPipeError) as ex:
                self.fail("Connection closed remotely. A client that sent a valid number sequence (%s) has been disconnected by the server."%(valid_sequence))
                break

        sock.close()


if __name__ == '__main__':
    unittest.main()

