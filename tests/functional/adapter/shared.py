import os


def is2_0():
    if os.getenv('USER_NAME') and "@" in os.getenv('USER_NAME', ""):
        return False
    return True