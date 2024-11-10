"""
AKA Discharge. Provides an authenticated static file mount to be used for Railgun.
Mostly provided by:
https://github.com/fastapi/fastapi/issues/858
"""
from fastapi import Request
from fastapi.staticfiles import StaticFiles

from src import railsecure

class AuthStaticFiles(StaticFiles):
    def __init__(self, railgun_app, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.railgun_app = railgun_app


    def authenticate_static(self, request: Request):
        """
        Auth function is rewritten for internalization within discharge.
        Should behave effectivelythe same as for normal endpoints however.

        Will trigger response with Railgun Authentication HTTP error (401/405) on failure.

        :params fastapi.Request request: standard request object
        """
        print("AUTHENTICATING STATIC DISCHARGE")
        real_token = request.cookies.get("access_token") or request.headers.get("Authorization", "Bearer ").split("Bearer ")
        railsecure.authenticate_token(self.railgun_app, real_token)


    async def __call__(self, scope, receive, send):
        """
        Hijack the original call to perform authentication on the incoming request,
        then continue as usual.

        See starlette.StaticFiles for full doc.
        """
        request = Request(scope, receive)
        self.authenticate_static(request)

        await super().__call__(scope, receive, send)
