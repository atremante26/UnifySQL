from flask import Flask

from unifysql.api.middleware import register_middleware
from unifysql.api.routes.feedback import feedback_bp
from unifysql.api.routes.semantic import semantic_bp
from unifysql.api.routes.translate import translate_bp


def create_app() -> Flask:
    """Flask app factory — creates and configures the UnifySQL API."""
    app = Flask(__name__)
    register_middleware(app)
    app.register_blueprint(semantic_bp)
    app.register_blueprint(feedback_bp)
    app.register_blueprint(translate_bp)
    return app
