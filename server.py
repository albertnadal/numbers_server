import sys
import asyncio
import uuid
import aiofiles
import bintrees

ADDR = 'localhost'
PORT = 4000
MAX_CONNECTIONS = 5
LOG_FILENAME = "numbers.log"
REPORT_PERIODICITY = 10


class NumbersServer:

    def __init__(self, addr=ADDR, port=PORT, max_connections=MAX_CONNECTIONS, filename=LOG_FILENAME, periodicity=REPORT_PERIODICITY):

        self.addr_ = addr
        self.port_ = port
        self.max_connections_ = max_connections
        self.filename_ = filename
        self.periodicity_ = periodicity

        self.connections = {}
        self.connections_count = 0
        self.server = None
        self.timer = None
        self.bst = bintrees.FastBinaryTree()
        self.bst_lock = asyncio.Lock()
        self.logfile = None
        self.logfile_buffer = []
        self.report_event = asyncio.Event()
        self.current_report = [0,0,0]   # [0] -> count of new unique numbers received.
                                        # [1] -> count of new duplicate numbers.
                                        # [2] -> count of total unique numbers received.

    async def run(self):

        # Open an async file descriptor to write non duplicated numbers
        self.logfile = await aiofiles.open(self.filename_, "w")

        # Create the server task to handle TCP connections at localhost:4000
        self.server = await asyncio.start_server(self.connection_handler, self.addr_, self.port_)

        # Generate the first report to show the initial status
        await self.generate_report()

        # Create the timer task to generate reports every 10 seconds
        self.timer = asyncio.create_task(self.timer_runner(self.periodicity_, self.generate_report))

        # Keep the server running in the event loop
        async with self.server:
            await self.server.serve_forever() # Im noticed this asyncio operation is currently deprecated

    async def timer_runner(self, timeout, operation):
        while True:
            await asyncio.sleep(timeout)
            await operation()

    async def generate_report(self):

        self.report_event.clear()
        print("Received %d unique numbers, %d duplicates. Unique total: %d"%(self.current_report[0], self.current_report[1], self.current_report[2]), file=sys.stdout, flush=True)
        self.current_report[0] = 0
        self.current_report[1] = 0

        # Process and flush buffered logs to disk
        await self.logfile.write('\n'.join(self.logfile_buffer))

        # Clean the buffer
        self.logfile_buffer.clear()

        self.report_event.set()


    async def close_connection(self, connection_id):

        connection_data = self.connections.get(connection_id, None)
        if connection_data is not None:
            connection_data['writer'].close()
            del self.connections[connection_id]
            self.connections_count-=1

    def shutdown(self):

        for connection_id in self.connections.keys():
            self.connections[connection_id]['writer'].close()
        self.connections.clear()

        self.timer.cancel()
        self.server.close()
        self.logfile.close()
        loop = asyncio.get_event_loop()
        loop.stop()

    async def consume_stream(self, reader_: asyncio.StreamReader) -> str:

        remaining_buf = b''

        while True:

            # I set a buffer with the max size allowed (64KiB) by the StreamReader. Im assuming the application will receive a massive
            # amount of data, so use a large buffer should avoid lots of internal memory allocations and overhead when dealing with
            # the TCP stream.
            buf = await reader_.read(65536) #64KiB

            if not buf:
                # The peer has been disconnected
                return "close"

            if remaining_buf is not None:
                buf = remaining_buf + buf

            remaining_buf = None

            # The application expects to receive sequences of numbers with a fixed size, so to get each sequence I get individual
            # slices of the buffer by using an offset.
            # I chose the Unix escape sequence. So each number sequence should contain one extra byte at the end for the \n character
            # So, each number sequence MUST be 10 bytes long.
            for offset in range(0,len(buf),10):
                byte_array = buf[offset:offset+10]
                length = len(byte_array)

                # Data transfered through TCP connections is sent in different chunks of variable size. So, the last number sequence
                # received can be incomplete. An incomplete number has a lenght < 10, so the remaining sequece in the current buffer
                # must be concatenated at the begining of the next chunk received.
                if length < 10:
                    remaining_buf = buf[-length:]
                    break

                # Validate that the escape sequence is valid
                # Unix/Linux escape sequence is \n (LF). 10 in ASCII encoding. ord('\n') == 10
                if byte_array[9] != 10:
                    # Invalid escape sequence
                    return "close"

                # Validate that the first 9 characters are valid numbers acording to their ASCII values.
                # Also check for the presence of the "terminate" sequence.
                for index in range(9):
                    if byte_array[index] < 48 or byte_array[index] > 57:
                        # Check if the first 9 bytes matches with the "terminate" sequence in ASCII
                        if byte_array == b'terminate\n':
                            return "terminate"
                        return "close"

                # Convert the array of bytes to a string and remove the escape sequence
                # The decode() function can trow potential UnicodeDecodeError exceptions. As I
                # previously validated all the characters in the array and Im assuming this is
                # not a production environment so I decided to avoid using a try/catch just for
                # simplicity.
                number_str = byte_array.decode('utf8').strip()

                # Convert the data to a number. Working with numbers as keys in a BST is way faster
                # than using strings when using structures that perform key comparisons intensively.
                number = int(number_str)

                await self.bst_lock.acquire()

                if not self.report_event.is_set():
                    await self.report_event.wait()

                try:
                    # Check if the number is already present in the BST and also check if
                    # the number has been duplicated at least once in the past.
                    is_duplicated = self.bst.get_value(number)

                    # If no KeyError exception has been raised (number not found in the BST)
                    # then this is a duplicated number. Set this number as duplicated only if
                    # the number is not marked as duplicated yet, and decrease the counter of
                    # total unique numbers received.
                    if not is_duplicated:
                        self.bst.insert(number, True) # Update value
                        self.current_report[2]-=1

                    # Increase the counter of duplicated numbers received for the current report.
                    self.current_report[1]+=1

                except KeyError as ex:
                    # Number not found in the BST. So this is a new unique number.
                    # Increase the counter of unique numbers received.
                    self.current_report[0]+=1

                    # Insert the number to the BST (marked as not duplicated).
                    self.bst.insert(number, False)

                    # Store the new number in a memory buffer instead of writing directly to
                    # disk in the log file. Writing small chunks of data massivelly is very ineficient.
                    # Writing a big buffer of data is faster. Buffer will be flushed during the report generation.
                    self.logfile_buffer.append(number_str)

                    # Increase the counter of total unique numbers received.
                    self.current_report[2]+=1

                finally:
                    self.bst_lock.release()


    async def connection_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):

        if self.connections_count >= self.max_connections_:
            writer.close()
            return

        self.connections_count+=1
        connection_id = str(uuid.uuid1())
        self.connections[connection_id] = { 'reader': reader, 'writer': writer }
        consume_task = asyncio.create_task(self.consume_stream(reader))

        try:
            await consume_task
            if consume_task.result() == "terminate":
                self.shutdown()
            else:
                await self.close_connection(connection_id)
        except ConnectionResetError as ex:
            # The connection has been closed due to an unexpected error.
            await self.close_connection(connection_id)


if __name__ == '__main__':

    try:
        server = NumbersServer()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(server.run())
    except KeyboardInterrupt as ex:
        # Ctrl+c interruption
        server.shutdown()
    except:
        pass

