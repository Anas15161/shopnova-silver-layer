"""Tests unitaires pour la fabrique de transformers."""

import pytest
from src.transformers.customers_transformer import CustomersTransformer
from src.transformers.factory import TransformerFactory
from src.transformers.orders_transformer import OrdersTransformer
from src.transformers.products_transformer import ProductsTransformer


def test_factory_returns_orders_transformer():
    transformer = TransformerFactory.get_transformer("orders")
    assert isinstance(transformer, OrdersTransformer)


def test_factory_returns_customers_transformer():
    transformer = TransformerFactory.get_transformer("customers")
    assert isinstance(transformer, CustomersTransformer)


def test_factory_returns_products_transformer():
    transformer = TransformerFactory.get_transformer("products")
    assert isinstance(transformer, ProductsTransformer)


def test_factory_case_insensitive():
    transformer = TransformerFactory.get_transformer("  ORDERS  ")
    assert isinstance(transformer, OrdersTransformer)


def test_factory_unsupported_dataset():
    with pytest.raises(ValueError, match="Aucun transformer disponible pour"):
        TransformerFactory.get_transformer("unsupported_dataset")
