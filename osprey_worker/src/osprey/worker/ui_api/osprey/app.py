# ruff: noqa: E402
from __future__ import absolute_import

import ddtrace
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.lib.storage.bulk_action_files import get_bulk_action_file_manager
from werkzeug.exceptions import HTTPException

ddtrace.patch_all(gevent=True)

from http import HTTPStatus
from typing import NoReturn, Tuple, Union

import sentry_sdk
from flask import Flask, Response
from flask_cors import CORS
from osprey.engine.ast_validator.validation_context import ValidationFailed
from osprey.worker.lib import ddtrace_utils
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.utils.flask_utils import OspreyFlask
from sentry_sdk.integrations.flask import FlaskIntegration


def _after_request(response: Response) -> Response:
    response.mimetype = 'application/json'

    return response


def _handle_validation_failed(err: ValidationFailed) -> Response:
    return Response(status=HTTPStatus.BAD_REQUEST, response=err.rendered(), mimetype='application/json')


def _handle_exception(e: HTTPException) -> Response:
    return Response(status=e.code, response=e.description, mimetype='application/json')


def _register_with_prefix(app, blueprint):
    """Serve every view of `blueprint` both bare and under /api.

    Flask <2.1 let us register the same blueprint object twice under one name, so both
    paths resolved to the SAME `request.endpoint`. Flask 2.1+ rejects the duplicate name.
    Giving the /api copy its own `name=` satisfies that, but renames the endpoint for
    every /api route, silently breaking anything keyed on `request.endpoint` -- audit /
    exemption lookups and ddtrace span resource names among them.

    So register once, then mirror each of the blueprint's rules under /api reusing the
    same endpoint and view function. `add_url_rule` permits a repeated endpoint as long
    as the view function is identical, and this keeps the served URL set, `url_for()`
    output and `request.endpoint` byte-identical to the Flask 1.x behaviour.
    """
    known = {rule.endpoint for rule in app.url_map.iter_rules()}
    app.register_blueprint(blueprint)
    for rule in [r for r in list(app.url_map.iter_rules()) if r.endpoint not in known]:
        app.add_url_rule(
            f'/api{rule.rule}',
            endpoint=rule.endpoint,
            view_func=app.view_functions[rule.endpoint],
            methods=sorted(rule.methods - {'HEAD', 'OPTIONS'}),
            defaults=rule.defaults,
            strict_slashes=rule.strict_slashes,
        )


def health() -> Union[str, Tuple[str, int]]:
    # TODO: Real health reporting
    healthy = True
    if not healthy:
        return 'UNHEALTHY', HTTPStatus.SERVICE_UNAVAILABLE
    return 'OK'


def debug_sentry() -> NoReturn:  # type: ignore
    _division_by_zero = 1 / 0  # noqa: F841


def create_app() -> Flask:
    from osprey.worker.lib.singletons import CONFIG
    from osprey.worker.lib.storage import postgres
    from osprey.worker.ui_api.osprey.lib import auth

    from .lib.audit import audit_request
    from .views import (
        abilities,
        bulk_actions,
        bulk_history,
        config,
        docs,
        entities,
        events,
        features,
        queries,
        rules_visualizer,
        saved_queries,
    )

    CONFIG.instance().configure_from_env()
    sentry_dsn = CONFIG.instance().get_str('SENTRY_UI_API_DSN', '')

    if sentry_dsn:
        sentry_sdk.init(dsn=sentry_dsn, integrations=[FlaskIntegration()])

    postgres.init_from_config('osprey_db')

    gunicorn_logger = get_logger('gunicorn.error')

    app = OspreyFlask(__name__)

    # allows requests to come from any origin
    CORS(app)

    postgres.init_app(app)
    app.after_request(_after_request)
    app.register_error_handler(ValidationFailed, _handle_validation_failed)
    app.register_error_handler(HTTPException, _handle_exception)

    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.debug = CONFIG.instance().debug
    app.testing = CONFIG.instance().testing

    app.bulk_action_file_manager = get_bulk_action_file_manager()

    auth.init_app(app)
    ENGINE.instance()
    ddtrace_utils.init_app(app, service_name='osprey-ui-api')

    app.add_url_rule('/_health', 'health', health, methods=['GET'])
    app.add_url_rule('/_debug_sentry', 'debug_sentry', debug_sentry, methods=['GET'])

    _register_with_prefix(app, entities.blueprint)
    _register_with_prefix(app, events.blueprint)
    _register_with_prefix(app, features.blueprint)
    _register_with_prefix(app, queries.blueprint)
    _register_with_prefix(app, config.blueprint)
    _register_with_prefix(app, docs.blueprint)
    _register_with_prefix(app, saved_queries.blueprint)
    _register_with_prefix(app, abilities.blueprint)
    _register_with_prefix(app, bulk_history.blueprint)
    _register_with_prefix(app, rules_visualizer.blueprint)
    _register_with_prefix(app, bulk_actions.blueprint)

    app.after_request(audit_request)

    return app
