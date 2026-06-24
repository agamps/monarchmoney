import inspect
from typing import Any

from gql import Client, GraphQLRequest
from monarchmoney import MonarchMoney, MonarchMoneyEndpoints

MONARCH_API_BASE_URL = "https://api.monarch.com"


async def _gql_call_with_request(
    self: MonarchMoney,
    operation: str,
    graphql_query: Any,
    variables: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request = GraphQLRequest(
        graphql_query,
        variable_values=variables or {},
        operation_name=operation,
    )
    return await self._get_graphql_client().execute_async(request)


def _configure_gql_compatibility() -> None:
    execute_async_signature = inspect.signature(Client.execute_async)
    if "document" in execute_async_signature.parameters:
        return

    MonarchMoney.gql_call = _gql_call_with_request


def configure_monarch_api() -> None:
    MonarchMoneyEndpoints.BASE_URL = MONARCH_API_BASE_URL
    _configure_gql_compatibility()
