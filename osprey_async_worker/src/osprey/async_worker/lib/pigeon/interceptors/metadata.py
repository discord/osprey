from typing import Dict, Text

import grpc.aio


class MetadataInterceptor(grpc.aio.UnaryUnaryClientInterceptor):  # type: ignore[misc]
    def __init__(self, metadata: Dict[Text, Text]):
        self._metadata = metadata

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        # client_call_details.metadata may be None when the caller passed no metadata.
        metadata = list(client_call_details.metadata) if client_call_details.metadata else []

        for key, value in self._metadata.items():
            metadata.insert(0, (key, value))

        return await continuation(client_call_details._replace(metadata=metadata), request)
