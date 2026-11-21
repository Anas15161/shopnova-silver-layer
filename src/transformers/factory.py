"""Factory for instantiating dataset-specific transformers."""

from typing import Dict, Type
from src.transformers.base import BaseTransformer
from src.transformers.customers_transformer import CustomersTransformer
from src.transformers.orders_transformer import OrdersTransformer
from src.transformers.products_transformer import ProductsTransformer


class TransformerFactory:
    """Registry and factory for dataset transformers."""

    _TRANSFORMERS: Dict[str, Type[BaseTransformer]] = {
        "orders": OrdersTransformer,
        "customers": CustomersTransformer,
        "products": ProductsTransformer,
    }

    @classmethod
    def get_transformer(cls, dataset_name: str) -> BaseTransformer:
        """Returns the appropriate transformer for the specified dataset."""
        normalized = dataset_name.strip().lower()
        transformer_cls = cls._TRANSFORMERS.get(normalized)
        if not transformer_cls:
            supported = ", ".join(cls._TRANSFORMERS.keys())
            raise ValueError(
                f"Aucun transformer disponible pour: '{dataset_name}'. "
                f"Datasets supportes: {supported}"
            )
        return transformer_cls()
