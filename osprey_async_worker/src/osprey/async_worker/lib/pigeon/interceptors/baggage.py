from typing import Dict, Optional

import grpc.aio
from osprey.worker.lib.ddtrace_utils import (
    baggage_propagator,
    current_span,
    get_baggage,
)

BAGGAGE_HEADER = 'baggage'


class BaggageInterceptor(grpc.aio.UnaryUnaryClientInterceptor):  # type: ignore[misc]
    """
    Propagates tracing "baggage" to downstream grpc services.

    This is only a partial, simple implementation of the W3C spec, skipping checks
    such as limits on the maximum number of baggage keys and the lengths of keys
    or values.

    All baggage keys and headers should already be strings, as Datadog expects
    tags to all be pre-stringified.

    For more info:
    https://opentelemetry.io/docs/reference/specification/baggage/api/
    """

    def __init__(self, baggage_header: Optional[str] = None, baggage: Optional[Dict[str, str]] = None):
        self._baggage_header = baggage_header or BAGGAGE_HEADER
        self._baggage = baggage or {}

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        # client_call_details.metadata may be None when the caller passed no metadata.
        metadata = list(client_call_details.metadata) if client_call_details.metadata else []

        # The baggage propagator expects a header-style dictionary but python's grpc metadata
        # is a list of tuples, so we need to convert
        baggage_headers = {}
        baggage_propagator.inject(get_baggage(current_span()), baggage_headers)
        metadata.extend([(k, v) for k, v in baggage_headers.items()])

        return await continuation(client_call_details._replace(metadata=metadata), request)
