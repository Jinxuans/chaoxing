try:
    from requests.exceptions import JSONDecodeError
except ImportError:
    from json import JSONDecodeError


class LoginError(Exception):
    def __init__(self, *args: object):
        super().__init__(*args)


class ManualVerificationRequired(Exception):
    def __init__(self, message: str, *, reason: str = "", evidence: str = ""):
        super().__init__(message)
        self.reason = reason or message
        self.evidence = evidence


class InputFormatError(Exception):
    def __init__(self, *args: object):
        super().__init__(*args)


class MaxRollBackExceeded(Exception):
    def __init__(self, *args: object):
        super().__init__(*args)


class MaxRetryExceeded(Exception):
    def __init__(self, *args: object):
        super().__init__(*args)


class FontDecodeError(Exception):
    def __init__(self, *args: object):
        super().__init__(*args)
