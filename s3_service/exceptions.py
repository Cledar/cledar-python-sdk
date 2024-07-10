class RequiredBucketNotFoundException(BaseException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
