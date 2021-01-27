# pylint: disable=super-init-not-called

class TapQuickbooksException(Exception):
    pass

class TapQuickbooksQuotaExceededException(TapQuickbooksException):
    pass