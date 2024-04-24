# Base Errors for the Store

class StoreError(Exception):
    """
    Base class for all store errors
    """
    pass

# Specific Base Errors for the Store
# These are used to make polymorphisms
# for the Data Access Objects and Repositories
# so that error typing is consistent across
# data layers

class NotFoundError(StoreError):
    """
    Raised when a resource is not found
    """
    pass

class AlreadyExistsError(StoreError):
    """
    Raised when a resource already exists
    """
    pass

class RangeError(StoreError):
    """
    Raised when a resource is out of range
    """
    pass

class ValidationError(StoreError):
    """
    Raised when a resource fails validation
    """
    pass

class ArgumentTypeError(StoreError):
    """
    Raised when a resource fails validation
    """
    pass

class ArgumentNameError(StoreError):
    """
    Raised when a resource fails validation
    """
    pass

class ArgumentValueError(StoreError):
    """
    Raised when a resource fails validation
    """
    pass

class ConfigError(StoreError):
    """
    Raised when a resource fails validation
    """
    pass

class UncaughtError(StoreError):
    """
    Raised when a resource fails validation
    """
    pass

