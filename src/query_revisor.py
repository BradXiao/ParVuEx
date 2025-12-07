import re
from typing import Union
from schemas import BadQueryException


class Revisor:
    def __init__(self, query: str):
        """params:
        query: str - the query to be revised
        """
        self.query = self.clear_q(query)

    def clear_q(self, query: str) -> str:
        """clear the query from extra whitespaces"""
        result = re.sub(r"\s+", " ", query)
        return result.strip().lower()

    def _rule_no_joins(self) -> BadQueryException | None:
        """no join'ly queries are allowed"""
        if {"left", "right", "full", "inner", "join"}.intersection(
            set(self.query.split())
        ):
            return BadQueryException(
                name="Joins",
                message="Joins are not allowed in the query - app is multi-tabled yet...",
            )

    def run(self) -> Union[bool, BadQueryException]:
        """Run all rules"""
        rules = [
            self._rule_no_joins,
        ]
        for rule in rules:
            rule_res = rule()
            if isinstance(rule_res, BadQueryException):
                return rule_res

        return True
