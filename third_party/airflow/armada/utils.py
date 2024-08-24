import functools


def log_exceptions(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            if hasattr(self, "log") and hasattr(self.log, "error"):
                self.log.error(f"Exception in {method.__name__}: {e}")
            raise

    return wrapper
