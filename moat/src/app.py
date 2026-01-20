import uuid
from apis.opa import bundle_api_bp, decision_log_api_bp, status_api_bp
from apis.healthcheck import healthcheck_bp
from apis.scim2 import (
    scim2_service_provider_config_bp,
    scim2_resource_types_bp,
    scim2_schemas_bp,
    scim2_users_bp,
    scim2_groups_bp,
)
from views import (
    resources_bp,
    healthz_bp,
    principals_bp,
    policies_bp,
    root_bp,
)
from app_config import AppConfigModelBase
from app_logger import Logger, get_logger
from events import EventLogger, EventDto
from database import Database
from flask import Flask, g, request, jsonify
from werkzeug.exceptions import HTTPException

logger: Logger = get_logger("app")


class FlaskConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "flask"
    environment: str = "dev"
    secret_key: str = None
    static_url_path: str = ""
    static_folder: str = "../ui/static"
    template_folder: str = "../ui/templates"


def create_app(database: Database | None = None) -> Flask:
    flask_config = FlaskConfig.load()

    flask_app = Flask(
        __name__,
        static_url_path=flask_config.static_url_path,
        static_folder=flask_config.static_folder,
        template_folder=flask_config.template_folder,
    )
    flask_app.secret_key = flask_config.secret_key

    # Database
    database: Database = Database()
    database.connect()

    # views
    flask_app.register_blueprint(root_bp)
    flask_app.register_blueprint(principals_bp)
    flask_app.register_blueprint(healthz_bp)
    flask_app.register_blueprint(resources_bp)
    flask_app.register_blueprint(policies_bp)

    # enable APIs
    flask_app.register_blueprint(bundle_api_bp)
    flask_app.register_blueprint(decision_log_api_bp)
    flask_app.register_blueprint(status_api_bp)
    flask_app.register_blueprint(healthcheck_bp)
    # flask_app.register_blueprint(resource_bp)

    # Register SCIM2 blueprints
    flask_app.register_blueprint(scim2_service_provider_config_bp)
    flask_app.register_blueprint(scim2_resource_types_bp)
    flask_app.register_blueprint(scim2_schemas_bp)
    flask_app.register_blueprint(scim2_users_bp)
    flask_app.register_blueprint(scim2_groups_bp)

    # event logger
    event_logger: EventLogger = EventLogger()

    @flask_app.errorhandler(HTTPException)
    def handle_exception(e):
        api_error: dict = {
            "detail": e.description,
            "status": f"{e.code}",
        }

        # Check if the request is for the SCIM API - probably should be in the blueprint
        if request.path.startswith("/api/scim/"):
            api_error["schemas"] = ["urn:ietf:params:scim:api:messages:2.0:Error"]

        response = jsonify(api_error)
        response.status_code = e.code
        return response

    @flask_app.before_request
    def before_request():
        # connect DB
        g.database = database
        g.event_logger = event_logger
        g.request_id = str(uuid.uuid4())[0:6]
        g.environment = flask_config.environment

        logger.info(
            f"{g.request_id} - Started request {request.remote_addr} {request.method}:{request.full_path}"
        )

    @flask_app.after_request
    def after_request(response):
        logger.info(f"{g.request_id} - Completed request with status {response.status}")
        return response

    return flask_app
