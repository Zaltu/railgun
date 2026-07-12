"""
Minor utility tooling to provide helpers for circumventing some of python's async
implementation issues.
"""
import asyncio
from threading import Thread, current_thread


def execute_immediately(coroutine):
    """
    Run a coroutine immediately, as though it were sync code.
    That means this is blocking.
    This is done by spawning a separate thread with it's own event loop, and explicitely
    blocking that thread until the loop resolves, returning only when the thread can be
    joined. This is effectively a light(er)weight version of ThreadPoolExecutor. Note that
    due to the syntax, the coroutine is created in a different thread than where it is
    executed.

    :param Coroutine coroutine: an awaitable, unexecuted coroutine to run

    :returns: the result of the provided coroutine function
    :rtype: Any
    """
    async def retrieve_result():
        current_thread().return_value = await coroutine
    _wrap_thread = Thread(target=lambda: asyncio.run(retrieve_result()))
    _wrap_thread = Thread(target=lambda: asyncio.run(retrieve_result()))
    _wrap_thread.start()
    _wrap_thread.join()
    return _wrap_thread.return_value
