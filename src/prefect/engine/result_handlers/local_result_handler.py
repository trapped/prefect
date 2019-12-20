"""
Result Handlers provide the hooks that Prefect uses to store task results in production; a `ResultHandler` can be provided to a `Flow` at creation.

Anytime a task needs its output or inputs stored, a result handler is used to determine where this data should be stored (and how it can be retrieved).
"""
import os
import pathlib
import tempfile
import glob
from typing import Any

import cloudpickle

from prefect.engine.result_handlers import ResultHandler

# TODO: question: why is absolute path required?
# TODO: we allow for overwrites it seems?
# TODO: it would be nice to know the version of the flow run from the result object? (or similar information)
# answer: added a KEY option
# TODO: allow for grabbing the "latest" version
# answer: added accumulator
# ... but is this good. This should be cleaner: no need for making determinations here. All decision making should be passed in.


# TODO: this is a leaky abstraction: constructor takes a path used by write, but read also takes a possibly different path?


class LocalResultHandler(ResultHandler):
    """
    Hook for storing and retrieving task results to and from local file storage. Only intended to be used
    for local testing and development. Task results are written using `cloudpickle` and stored in the
    provided location for use in future runs.

    **NOTE**: Stored results will _not_ be automatically cleaned up after execution.

    Args:
        - dir (str, optional): the _absolute_ path to a directory for storing
            all results; defaults to `$TMPDIR`
    """

    def __init__(self, dir: str = None, accumulate: bool = True):
        self.dir = dir or "."
        self.dir = os.path.abspath(self.dir)
        self.accumulate = accumulate
        super().__init__()

    def read(self, fpath: str) -> Any:
        """
        Read a result from the given file location.

        Args:
            - fpath (str): the _absolute_ path to the location of a written result

        Returns:
            - the read result from the provided file
        """

        # based on the path given, this may never work?!? that is, if the abs path is given then what's the point?
        # FIX: this should be based on a key within the stored constructor dir

        # if self.accumulate:
        #     sequence = '0'
        #     for dir in sorted(glob.glob(os.path.join(self.dir, "*") + os.path.sep), reverse=True):
        #         sequence = dir
        #         break

        self.logger.debug("Starting to read result from {}...".format(fpath))
        with open(fpath, "rb") as f:
            val = cloudpickle.loads(f.read())
        self.logger.debug("Finished reading result from {}...".format(fpath))
        return val

    def write(self, result: Any, key: str) -> str:
        """
        Serialize the provided result to local disk.

        Args:
            - result (Any): the result to write and store

        Returns:
            - str: the _absolute_ path to the written result on disk
        """
        result_dir = self.dir
        if self.accumulate:
            sequence = "0"
            for dir in sorted(
                glob.glob(os.path.join(self.dir, "[0-9]") + os.path.sep), reverse=True
            ):

                candidate_result = os.path.join(self.dir, dir, key)
                if os.path.exists(candidate_result):
                    current_seq = pathlib.Path(dir).name
                    sequence = str(int(current_seq) + 1)
                    break

            result_dir = os.path.join(self.dir, sequence)
            if not os.path.exists(result_dir):
                os.mkdir(result_dir)

        loc = os.path.join(result_dir, key)

        self.logger.debug("Starting to upload result to {}...".format(loc))
        with open(loc, "wb") as f:
            f.write(cloudpickle.dumps(result))
        self.logger.debug("Finished uploading result to {}...".format(loc))
        return loc
