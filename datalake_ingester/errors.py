class InvalidS3Error(Exception):
    pass

class InvalidS3Notification(InvalidS3Error):
    pass

class InvalidS3Event(InvalidS3Error):
    pass
