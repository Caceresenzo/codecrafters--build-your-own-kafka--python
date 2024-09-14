import enum


@enum.unique
class ErrorCode(enum.Enum):

    NONE = 0
    UNKNOWN_SERVER_ERROR = -1
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC_ID = 100
