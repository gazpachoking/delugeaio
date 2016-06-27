import asyncio
import functools
import itertools
import logging
import ssl
import struct
import zlib

try:
    import rencode
except ImportError:
    from . import rencode

import delugeaio.error


log = logging.getLogger(__name__)

RPC_RESPONSE = 1
RPC_ERROR = 2
RPC_EVENT = 3

MESSAGE_HEADER_SIZE = 5


class DelugeTransferProtocol(asyncio.Protocol):
    """
    Data messages are transferred using very a simple protocol.
    Data messages are transferred with a header containing
    the length of the data to be transferred (payload).
    """
    def __init__(self, message_received_callback):
        self.transport = None
        self.message_received = message_received_callback
        self._buffer = bytes()
        self._message_length = 0
        self.connected = False
        self.on_disconnect = asyncio.Future()

    def connection_made(self, transport):
        self.transport = transport
        self.connected = True

    def connection_lost(self, exc):
        self.connected = False
        if exc:
            self.on_disconnect.set_exception(exc)
        else:
            self.on_disconnect.done()

    def transfer_message(self, data):
        """
        Transfer the data.
        The data will be serialized and compressed before being sent.
        First a header is sent - containing the length of the compressed payload
        to come as a signed integer. After the header, the payload is transfered.
        :param data: data to be transfered in a data structure serializable by rencode.
        """
        compressed = zlib.compress(rencode.dumps(data))
        size_data = len(compressed)
        # Store length as a signed integer (using 4 bytes). "!" denotes network byte order.
        header = b"D" + struct.pack("!i", size_data)
        self.transport.write(header)
        self.transport.write(compressed)

    def data_received(self, data):
        """
        This method is called whenever data is received.
        :param data: a message as transfered by transfer_message, or a part of such
                     a messsage.
        Global variables:
            _buffer         - contains the data received
            _message_length - the length of the payload of the current message.
        """
        self._buffer += data

        while len(self._buffer) >= MESSAGE_HEADER_SIZE:
            if self._message_length == 0:
                self._handle_new_message()
            # We have a complete packet
            if len(self._buffer) >= self._message_length:
                self._handle_complete_message(self._buffer[:self._message_length])
                # Remove message data from buffer
                self._buffer = self._buffer[self._message_length:]
                self._message_length = 0
            else:
                break

    def _handle_new_message(self):
        """
        Handle the start of a new message. This method is called only when the
        beginning of the buffer contains data from a new message (i.e. the header).
        """
        try:
            # Read the first bytes of the message (MESSAGE_HEADER_SIZE bytes)
            header = self._buffer[:MESSAGE_HEADER_SIZE]

            if not header.startswith(b"D"):
                raise Exception("Invalid header format. First byte is %d" % header[0])
            # Extract the length stored as a signed integer (using 4 bytes)
            self._message_length = struct.unpack("!i", header[1:MESSAGE_HEADER_SIZE])[0]
            if self._message_length < 0:
                raise Exception("Message length is negative: %d" % self._message_length)
            # Remove the header from the buffer
            self._buffer = self._buffer[MESSAGE_HEADER_SIZE:]
        except Exception as ex:
            log.warn("Error occurred when parsing message header: %s.", ex)
            log.warn("This version of Deluge cannot communicate with the sender of this data.")
            self._message_length = 0
            self._buffer = b""
            raise

    def _handle_complete_message(self, data):
        """
        Handles a complete message as it is transfered on the network.
        :param data: a zlib compressed string encoded with rencode.
        """
        try:
            message = rencode.loads(zlib.decompress(data), decode_utf8=True)
        except Exception as ex:
            log.warn("Failed to decompress (%d bytes) and load serialized data with rencode: %s", len(data), ex)
            return
        if type(message) is not tuple:
            log.error("Received invalid message: type is not tuple")
            return
        self.message_received(message)


class Client:
    def __init__(self):
        self._event_handlers = {}
        self._futures = {}
        self._request_counter = itertools.count()
        self.loop = asyncio.get_event_loop()
        self.protocol = None
        self.daemon_info = None
        self.authentication_level = None
        self.auth_levels_mapping = None
        self.auth_levels_mapping_reverse = None

    @property
    def connected(self):
        return self.protocol and self.protocol.connected

    async def connect(self, host="127.0.0.1", port=58846, username="", password=""):
        prot_factory = functools.partial(DelugeTransferProtocol, self.message_received)
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)
        (transport, self.protocol) = await self.loop.create_connection(prot_factory, host, port, ssl=ssl_context)
        self.protocol.on_disconnect.add_done_callback(self.on_disconnect)
        self.daemon_info = await self.call("daemon.info")
        self.authentication_level = await self.call("daemon.login", username, password, client_version="deluge_aio")
        self.username = username
        auth_mappings = await self.call("core.get_auth_levels_mappings")
        self.auth_levels_mapping, self.auth_levels_mapping_reverse = auth_mappings
        if self._event_handlers:
            await self.call("daemon.set_event_interest", list(self._event_handlers))

    def on_disconnect(self, fut):
        log.info("Disconnected from the daemon.")
        if self._futures:
            log.debug("Disconnected while waiting for results")
            for f in self._futures:
                if fut.exception():
                    f.set_exception(fut.exception())
                else:
                    f.set_exception(delugeaio.error.DelugeError("Daemon disconnected while waiting for reply."))

    def message_received(self, request):
        """
        This method is registered with the protocol to receive messages from the daemon.
        """
        if len(request) < 3:
            log.error("Received invalid message: number of items in response is {}".format(len(request)))
            return

        message_type = request[0]

        if message_type == RPC_EVENT:
            event = request[1]
            if event in self._event_handlers:
                for handler in self._event_handlers[event]:
                    self.loop.call_soon(handler, *request[2])
            return

        request_id = request[1]

        f = self._futures.pop(request_id)

        if message_type == RPC_RESPONSE:
            f.set_result(request[2])
        elif message_type == RPC_ERROR:
            try:
                exception_cls = getattr(delugeaio.error, request[2])
                exception = exception_cls(*request[3], **request[4])
            except TypeError as ex:
                log.error("Received invalid RPC_ERROR (Old daemon?): %s", request[2])
                exception = ex
            f.set_exception(exception)

    async def register_event_handler(self, event, handler):
        """
        Registers a handler function to be called when :param:`event` is received
        from the daemon.

        :param event: the name of the event to handle
        :type event: str
        :param handler: the function to be called when `:param:event`
            is emitted from the daemon
        :type handler: function

        """
        if event not in self._event_handlers:
            # This is a new event to handle, so we need to tell the daemon
            # that we're interested in receiving this type of event
            if self.connected:
                await self.call("daemon.set_event_interest", [event])
            self._event_handlers[event] = []

        # Only add the handler if it's not already registered
        if handler not in self._event_handlers[event]:
            self._event_handlers[event].append(handler)
        return True

    async def deregister_event_handler(self, event, handler):
        """
        Deregisters a event handler.

        :param event: the name of the event
        :type event: str
        :param handler: the function registered
        :type handler: function

        """
        if event in self._event_handlers and handler in self._event_handlers[event]:
            self._event_handlers[event].remove(handler)
            return True

    def call(self, method, *args, **kwargs):
        """
        Calls an RPC function and returns a Future representing the result
        """
        request_id = next(self._request_counter)
        log.debug('Calling reqid {} method {!r} with args:{!r} kwargs:{!r}'.format(request_id, method, args, kwargs))

        req = ((request_id, method, args, kwargs), )
        self.protocol.transfer_message(req)

        f = asyncio.Future()
        self._futures[request_id] = f
        return f

    def __getattr__(self, method):
        """
        Allows calling rpc funcions like `client.daemon.info()`
        """
        return DottedObject(self, method)


class DottedObject:
    """
    This is used for dotted name calls to client
    """
    def __init__(self, client, component):
        self.client = client
        self.base = component

    def __call__(self, *args, **kwargs):
        raise Exception("You must make calls in the form of 'component.method'!")

    def __getattr__(self, method):
        return RemoteMethod(self.client, self.base + "." + method)


class RemoteMethod(DottedObject):
    """
    This is used when something like 'client.core.get_something()' is attempted.
    """
    def __call__(self, *args, **kwargs):
        return self.client.call(self.base, *args, **kwargs)
