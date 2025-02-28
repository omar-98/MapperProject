

import inspect

from collections import OrderedDict
from graphlib import TopologicalSorter
from typing import List

from pyspark.sql import DataFrame, SparkSession

class ManagementRuleDependenciesSorter:
    def __init__(self):
        self._dependencies_graph = dict()

    def _add_dependency(self, column_name: str, dependencies: List):
        if not isinstance(column_name, str):
            return
        self._dependencies_graph.update({column_name: dependencies})

    def _reset_dependencies(self):
        self._dependencies_graph = {}

    def sort_methods(self):
        self._reset_dependencies()
        all_methods_map = dict(inspect.getmembers(type(self), predicate=lambda fn: inspect.isfunction(fn)))
        simple_methods = []

        for name, func in all_methods_map.items():
            deps = getattr(func, "dependency_list", [])
            if deps:
                self._add_dependency(name, deps)
            else:
                simple_methods.append(name)

        complex_methods: List[str] = [_
            for _ in TopologicalSorter(self._dependencies_graph).static_order()
            if _ not in simple_methods
        ]

        return all_methods_map, simple_methods, complex_methods

def management_rule_dependency(rules: List):
        def decorator(func):
            def wrapper(self, *args, **kwargs):
                return func(self, *args, **kwargs)

            wrapper.dependency_list = rules
            return wrapper

        return decorator

class ClassMapper(ManagementRuleDependenciesSorter):
    def __init__(self, schema):
            ManagementRuleDependenciesSorter.__init__(self)
            self.schema = schema
            self.columns = {}
            self.management_rules = []

            for column_name, rule_expr in self.sort_management_rules().items():
                expression = rule_expr(self)
                try:
                    self.columns[column_name] = expression
                    if column_name in self.schema.names:
                        datatype = self.schema[column_name].dataType
                        self.management_rules.append(self.columns[column_name].cast(datatype).alias(column_name))
                except Exception as _:
                    raise ValueError(f'Error when applying {self.__class__.__name__} management rules: {column_name}')

    def column(self, name):
            return self.columns[name]

    def sort_management_rules(self):
            at_lambda_map, simple_methods, dependent_methods = self.sort_methods()
            simple_methods = [_ for _ in simple_methods if _ in self.schema.names]
            ordered_methods = simple_methods + dependent_methods
            remaining_rules = set(self.schema.names) - set(ordered_methods)
            if remaining_rules:
                raise ValueError(f'Not implemented rules for columns: {remaining_rules}')
            # sorted mapping according to the dependencies
            return OrderedDict([(_, at_lambda_map[_]) for _ in ordered_methods])
    def __call__(self, input_df:DataFrame):
            return input_df.select(*self.management_rules)