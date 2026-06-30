import os

# Distinct session cookie so it doesn't collide with Airflow's "session" cookie
# on the same domain (cookies are scoped by domain+path, not port, by default)
SESSION_COOKIE_NAME = "superset_session"

# Secret key pulled from environment, same as before
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")