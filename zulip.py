import logging
import sys

from errbot.backends.base import RoomError, Identifier, Person, RoomOccupant, Stream, ONLINE, Room
from errbot.core import ErrBot
from errbot.rendering.ansiext import enable_format, TEXT_CHRS


# Can't use __name__ because of Yapsy.
log = logging.getLogger('errbot.backends.zulip')

ZULIP_MESSAGE_SIZE_LIMIT = 10000

try:
    import zulip
except ImportError:
    log.exception("Could not start the Zulip back-end")
    log.fatal(
        "You need to install the Zulip support in order "
        "to use the Zulip backend.\n"
        "You should be able to install this package using:\n"
        "pip install zulip"
    )
    sys.exit(1)

def parse_query_result(query_result):
    if query_result['result'] == 'success':
        return query_result
    else:
        raise ZulipRoomError("{}: {}".format(query_result['code'], query_result['msg']))

class ZulipRoomError(RoomError):
    def __init__(self, message=None):
        if message is None:
            message = (
                "I currently do not support this request :(\n"
                "I still love you."
            )
        super().__init__(message)

class ZulipIdentifier(Identifier):
    def __init__(self, id):
        self._id = str(id)

    @property
    def id(self):
        return self._id

    def __unicode__(self):
        return str(self._id)

    def __eq__(self, other):
        return self._id == other.id

    __str__ = __unicode__

# `ZulipPerson` is used for both 1-1 PMs and Group PMs.
class ZulipPerson(ZulipIdentifier, Person):
    def __init__(self, id, full_name, emails, client):
        super().__init__(id)
        self._full_name = full_name
        self._emails = emails
        self._client = client

    @property
    def person(self):
        return self._id

    @property
    def fullname(self):
        return self._full_name

    @property
    def nick(self):
        return None

    @property
    def client(self):
        return self._client

    @property
    def emails(self):
        return self._emails

    @property
    def aclattr(self):
        return ','.join(sorted(self._emails))

# `ZulipRoom` is used for messages to streams.
class ZulipRoom(ZulipIdentifier, Room):
    def __init__(self, title, id=None, subject=None, client=None):
        super().__init__(id)
        self._title = title
        self._subject = subject
        self._client = client

    @property
    def id(self):
        return self._id

    @property
    def aclattr(self):
        return self._id

    # A stream in Zulip can have multiple subjects. However, since
    # a `ZulipRoom` object is used to identify the message recipient,
    # it also needs to include the subject where a message will be
    # sent to.
    @property
    def subject(self):
        return self._subject

    @property
    def title(self):
        return self._title

    def join(self, username: str=None, password: str=None):
        parse_query_result(self._client.add_subscriptions([{"name": self._title}]))

    def create(self):
        raise ZulipRoomError()

    def leave(self, reason: str=None):
        parse_query_result(self._client.remove_subscriptions([self._title]))

    def destroy(self):
        raise ZulipRoomError()

    @property
    def joined(self):
        result = parse_query_result(self._client.get_subscribers(stream=self._title))
        return self._client.email in result['subscribers']

    @property
    def exists(self):
        result = parse_query_result(self._client.get_streams(include_public=True, include_subscribed=False))
        return any([stream['name'] == self._title for stream in result['streams']])

    @property
    def topic(self):
        result = parse_query_result(self._client.get_streams(include_public=True, include_subscribed=False))
        try:
            return next(stream['description'] for stream in result['streams'] if stream['name'] == self._title)
        except StopIteration():
            raise ZulipRoomError("The stream {} could not be found.".format(self._title))

    @property
    def occupants(self):
        result = parse_query_result(self._client.get_subscribers(stream=self._title))
        return [ZulipRoomOccupant(id=email, full_name=None, emails=[email],
                                  client=self._client, room=self) for email in result['subscribers']]

    def invite(self, *args):
        raise ZulipRoomError()

class ZulipRoomOccupant(ZulipPerson, RoomOccupant):
    """
    This class represents a person subscribed to a stream.
    """
    def __init__(self, id, full_name, emails, client, room):
        super().__init__(id=id, full_name=full_name, emails=emails, client=client)
        self._room = room

    @property
    def room(self):
        return self._room

class ZulipBackend(ErrBot):
    def __init__(self, config):
        super().__init__(config)
        config.MESSAGE_SIZE_LIMIT = ZULIP_MESSAGE_SIZE_LIMIT

        self.identity = config.BOT_IDENTITY
        for key in ('email', 'key', 'site'):
            if key not in self.identity:
                log.fatal(
                    "You need to supply the key `{}` for me to use. `{key}` and its value "
                    "can be found in your bot's `zuliprc` config file.".format(key)
                )
                sys.exit(1)

        compact = config.COMPACT_OUTPUT if hasattr(config, 'COMPACT_OUTPUT') else False
        enable_format('text', TEXT_CHRS, borders=not compact)
        self.client = zulip.Client(email=self.identity['email'],
                                   api_key=self.identity['key'],
                                   site=self.identity['site'])

    def serve_once(self):
        self.bot_identifier = self.build_identifier(self.client.email)
        log.info("Initializing connection")
        self.client.ensure_session()
        log.info("Connected")
        self.reset_reconnection_count()
        self.connect_callback()
        try:
            self.client.call_on_each_message(self._handle_message)
        except KeyboardInterrupt:
            log.info("Interrupt received, shutting down..")
            return True  # True means shutdown was requested.
        except Exception:
            log.exception("Error reading from Zulip updates stream.")
            raise
        finally:
            log.debug("Triggering disconnect callback.")
            self.disconnect_callback()

    def _handle_message(self, message):
        """
        Handles incoming messages.
        In Zulip, there are three types of messages: Private messages, Private group messages,
        and Stream messages. This plugin handles Group PMs as normal PMs between the bot and the
        user. Stream messages are handled as messages to rooms.
        """
        if not message['content']:
            log.warning("Unhandled message type (not a text message) ignored")
            return

        message_instance = self.build_message(message['content'])
        if message['type'] == 'private':
            message_instance.frm = ZulipPerson(
                id=message['sender_email'],
                full_name=message['sender_full_name'],
                emails=[message['sender_email']],
                client=message['client']
            )
            message_instance.to = ZulipPerson(
                id=message['sender_email'],
                full_name=','.join([recipient['full_name'] for recipient in message['display_recipient']]),
                emails=[recipient['email'] for recipient in message['display_recipient']],
                client=None
            )
        elif message['type'] == 'stream':
            room = ZulipRoom(
                id=message['display_recipient'],
                title=message['display_recipient'],
                subject=message['subject']
            )
            message_instance.frm = ZulipRoomOccupant(
                id=message['sender_email'],
                full_name=message['sender_full_name'],
                emails=[message['sender_email']],
                client=message['client'],
                room=room
            )
            message_instance.to = room
        else:
            raise ValueError("Invalid message type `{}`.".format(message['type']))
        self.callback_message(message_instance)

    def send_message(self, msg):
        super().send_message(msg)
        msg_data = {
            'content': msg.body,
        }
        if isinstance(msg.to, ZulipRoom):
            msg_data['type'] = 'stream'
            msg_data['subject'] = msg.to.subject
            msg_data['to'] = msg.to.title

        elif isinstance(msg.to, ZulipPerson):
            if isinstance(msg.to, ZulipRoomOccupant):
                msg_data['type'] = 'stream'
                msg_data['subject'] = msg.to.room.subject
                msg_data['to'] = msg.to.room.title
            else:
                msg_data['type'] = 'private'
                msg_data['to'] = msg.to.emails
        else:
            raise ValueError("Invalid message recipient of type {}".format(type(msg.to).__name__))
        try:
            self.client.send_message(msg_data)
        except Exception:
            log.exception(
                "An exception occurred while trying to send the following message "
                "to %s: %s" % (msg.to.id, msg.body)
            )
            raise

    def is_from_self(self, msg):
        return msg.frm.aclattr == self.client.email

    def change_presence(self, status: str = ONLINE, message: str = '') -> None:
        # At this time, Zulip doesn't support active presence change.
        pass

    def build_identifier(self, txtrep):
        return ZulipPerson(id=txtrep,
                           full_name=txtrep,
                           emails=[txtrep],
                           client=self.client)

    def build_reply(self, msg, text=None, private=False, threaded=False):
        response = self.build_message(text)
        response.to = msg.to
        return response

    @property
    def mode(self):
        return 'zulip'

    def query_room(self, room):
        return ZulipRoom(title=room, client=self.client)

    def rooms(self):
        result = parse_query_result(self.client.list_subscriptions())
        return [ZulipRoom(title=subscription['name'], id=subscription['name']) for subscription in result['subscriptions']]

    def prefix_groupchat_reply(self, message, identifier):
        super().prefix_groupchat_reply(message, identifier)
        message.body = '@**{0}** {1}'.format(identifier.full_name, message.body)

    def _zulip_upload_stream(self, stream):
        """Perform upload defined in a stream."""
        try:
            stream.accept()
            result = self.client.upload_file(stream.raw)
            if result['result'] == 'success':
                message_instance = self.build_message("[{}]({})".format(stream.name, result['uri']))
                message_instance.to = stream.identifier
                self.send_message(message_instance)
                stream.success()

            else:
                stream.error()
        except Exception:
            log.exception("Upload of {0} to {1} failed.".format(stream.name,
                                                                stream.identifier))

    def send_stream_request(self, identifier, fsource, name='file', size=None, stream_type=None):
        """Starts a file transfer.

        :param identifier: ZulipPerson or ZulipRoom
            Identifier of the Person or Room to send the stream to.

        :param fsource: str, dict or binary data
            File URL or binary content from a local file.
            Optionally a dict with binary content plus metadata can be given.
            See `stream_type` for more details.

        :param name: str, optional
            Name of the file. Not sure if this works always.

        :param size: str, optional
            Size of the file obtained with os.path.getsize.
            This is only used for debug logging purposes.

        :param stream_type: str, optional
            Type of the stream. Choices: 'document', 'photo', 'audio', 'video', 'sticker', 'location'.
            Right now used for debug logging purposes only.

        :return stream: str or Stream
            If `fsource` is str will return str, else return Stream.
        """
        def _metadata(fsource):
            if isinstance(fsource, dict):
                return fsource.pop('content'), fsource
            else:
                return fsource, None

        def _is_valid_url(url):
            try:
                from urlparse import urlparse
            except Exception:
                from urllib.parse import urlparse

            return bool(urlparse(url).scheme)

        content, meta = _metadata(fsource)
        if isinstance(content, str):
            if not _is_valid_url(content):
                raise ValueError("Not valid URL: {}".format(content))
            else:
                raise NotImplementedError("The Zulip backend does not yet support URL stream requests.")
        else:
            stream = Stream(identifier, content, name, size, stream_type)
            log.debug("Requesting upload of {0} to {1} (size hint: {2}, stream type: {3})".format(name,
                      identifier, size, stream_type))
            self.thread_pool.apply_async(self._zulip_upload_stream, (stream,))

        return stream
