import uuid

from crochet import run_in_reactor, setup
from ctrader_open_api import Auth, Client, EndPoints, Protobuf, TcpProtocol
from ctrader_open_api.messages.OpenApiMessages_pb2 import *  # noqa: F403
from dotenv import load_dotenv
from google.protobuf.json_format import MessageToDict

from config import config
from constant import API_TRANSACTION_SOURCE_CTRADER
from Dto.api_transaction_dto import ApiTransactionDto
from logger.log import APITransactionLogger

load_dotenv()

setup()

auth = Auth(
    config.CTRADER_CLIENT_ID, config.CTRADER_CLIENT_SECRET, config.CTRADER_REDIRECT_URL
)

timeout = 10


class CTraderOpenAPI:
    def __init__(self, host_mode="demo"):
        self.host_mode = host_mode
        if host_mode == "demo":
            self.host_type = EndPoints.PROTOBUF_DEMO_HOST
        else:
            self.host_type = EndPoints.PROTOBUF_LIVE_HOST

        self.ctrader_client_id = config.CTRADER_CLIENT_ID
        self.ctrader_client_secret = config.CTRADER_CLIENT_SECRET
        self.redirect_uri = config.CTRADER_REDIRECT_URL

        self.init_client()

        self.authorize_app()

    def on_error(self, failure):
        print("Error: ", failure)
        print(failure.getErrorMessage())

    def disconnected(self, client, reason):
        print("\nDisconnected: ", reason)

    def msg_receive(self, client, message):
        return message

    def encodeResult(self, result):
        if result is None:
            return

        return MessageToDict(Protobuf.extract(result))

    @run_in_reactor
    def sendProtoOAApplicationAuthReq(self):
        request = ProtoOAApplicationAuthReq()  # type: ignore # noqa: F405

        request.clientId = self.ctrader_client_id
        request.clientSecret = self.ctrader_client_secret

        msg_id = self.get_msg_id("ProtoOAApplicationAuthReq")

        deferred = self.client.send(request, clientMsgId=msg_id)
        deferred.addErrback(self.on_error)

        return deferred

    def authorize_app(self):
        d = self.sendProtoOAApplicationAuthReq()
        result = d.wait(timeout=timeout)
        encoded_result = self.encodeResult(result)
        return encoded_result

    @run_in_reactor
    def sendProtoOAAccountAuthReq(self, access_token, account_id, clientMsgId=None):
        request = ProtoOAAccountAuthReq()  # type: ignore # noqa: F405

        request.ctidTraderAccountId = account_id
        request.accessToken = access_token

        msg_id = self.get_msg_id("ProtoOAAccountAuthReq")

        deferred = self.client.send(request, clientMsgId=msg_id)
        deferred.addErrback(self.on_error)

        return deferred

    def authorize_account(self, access_token, account_id):
        d = self.sendProtoOAAccountAuthReq(access_token, account_id)
        result = d.wait(timeout=10)

        encoded_result = {}
        if result is not None:
            encoded_result = self.encodeResult(result)

        self.log_api_transaction(
            encoded_result, "ProtoOAAccountAuthReq", encoded_result
        )

        return encoded_result

    # ------------------------------------------------------------------------

    def get_msg_id(self, action=None, client_msg_id=None):
        id = str(uuid.uuid4())
        return f"{self.host_mode}#{id}{'#'+action if action else ''}{'#'+client_msg_id if client_msg_id else ''}"

    def init_client(self):
        self.client = Client(
            self.host_type,
            EndPoints.PROTOBUF_PORT,
            TcpProtocol,
        )
        self.client.setDisconnectedCallback(self.disconnected)
        self.client.setMessageReceivedCallback(self.msg_receive)
        self.client.startService()

    def stop_service(self):
        self.client.stopService()

    def get_auth_uri(self):
        return auth.getAuthUri()

    def refresh_token(self, refresh_token):
        return auth.refreshToken(refresh_token)

    def get_access_token(self, auth_code):
        token = auth.getToken(auth_code)

        if "accessToken" not in token:
            raise KeyError(token)

        return token


ctrader_client = CTraderOpenAPI()
ctrader_client_live = CTraderOpenAPI("live")
