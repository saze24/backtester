import os
import secrets
import sys
from flask import Flask
from . import backtester


def create_app():
    # Run backtester

    try:
        app = Flask(__name__)
        
        # Generate a secure, random string for every launch
        secret_key = secrets.token_hex(24)
        app.config['SECRET_KEY'] = secret_key

        from .views import views

        app.register_blueprint(views, url_prefix='/')

    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        backtester.log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)

    return app
