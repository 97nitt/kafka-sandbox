import fastavro
import json
import logging

logger = logging.getLogger(__name__)

def from_file(path: str) -> str:
    with open(path, "r") as file:
        schema = (file.read())

    validate(schema)
    return schema

def validate(schema_str: str):
    schema_dict = json.loads(schema_str)
    fastavro.parse_schema(schema_dict)
