from pathlib import Path


def root_path():
    return Path(__file__).parent


def tmp_path():
    return root_path().joinpath(".tmp")
