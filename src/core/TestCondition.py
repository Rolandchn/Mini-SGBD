import unittest
from Condition import Condition
from Column import ColumnInfo, Int, VarChar

class Record:
    def __init__(self, values):
        self.values = values

class TestCondition(unittest.TestCase):

    def setUp(self):
        self.columns = [
            ColumnInfo(name="id", type=Int()),
            ColumnInfo(name="name", type=VarChar(size=50)),
            ColumnInfo(name="age", type=Int()),
        ]

        self.record = Record([1, "Alice", 25])  # Exemple de valeurs cohérentes

    def test_from_string(self):
        condition = Condition.from_string("id = 1")
        self.assertEqual(condition.left_term, "id")
        self.assertEqual(condition.operator, "=")
        self.assertEqual(condition.right_term, "1")

    def test_get_operator(self):
        self.assertEqual(Condition.get_operator("id = 1"), "=")
        self.assertEqual(Condition.get_operator("age > 18"), ">")
        self.assertEqual(Condition.get_operator("name <> 'Bob'"), "<>")
        with self.assertRaises(ValueError):
            Condition.get_operator("invalid condition")

    def test_evaluate_equal(self):
        condition = Condition("id", "=", 1)
        self.assertTrue(condition.evaluate(self.record, self.columns))

    def test_evaluate_greater_than(self):
        condition = Condition("age", ">", 20)
        self.assertTrue(condition.evaluate(self.record, self.columns))

    def test_evaluate_less_than(self):
        condition = Condition("age", "<", 30)
        self.assertTrue(condition.evaluate(self.record, self.columns))

    def test_evaluate_invalid_column(self):
        condition = Condition("invalid_column", "=", 1)
        with self.assertRaises(ValueError):
            condition.evaluate(self.record, self.columns)

    def test_evaluate_string_comparison(self):
        condition = Condition("name", "=", "Alice")
        self.assertTrue(condition.evaluate(self.record, self.columns))

        condition = Condition("name", "<>", "Bob")
        self.assertTrue(condition.evaluate(self.record, self.columns))

    def test_evaluate_incompatible_types(self):
        condition = Condition("age", "=", "25")
        with self.assertRaises(ValueError):
            condition.evaluate(self.record, self.columns)

if __name__ == "__main__":
    unittest.main()
