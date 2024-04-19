import unittest
import json
from datetime import datetime
import os

from data_access.connection import get_collection
from data_access.repository import collect_dataset

proj_dir = os.getcwd()


class TestDataset(unittest.IsolatedAsyncioTestCase):

    async def test_month(self):
        path_to_test_cases = os.path.join(proj_dir, "src", "test_cases", "month")
        with open(os.path.join(path_to_test_cases, "month.json")) as file:
            expected_month_dataset = json.load(file)
        with open(os.path.join(path_to_test_cases, "month_input.json")) as file:
            month_input = json.load(file)
        collection = await get_collection()
        actual_dataset = await collect_dataset(
            collection,
            datetime.fromisoformat(month_input["dt_from"]),
            datetime.fromisoformat(month_input["dt_upto"]),
            month_input["group_type"]
        )
        self.assertEqual(expected_month_dataset, actual_dataset)
    
    async def test_day(self):
        path_to_test_cases = os.path.join(proj_dir, "src", "test_cases", "day")
        with open(os.path.join(path_to_test_cases, "day.json")) as file:
            expected_day_dataset = json.load(file)
        with open(os.path.join(path_to_test_cases, "day_input.json")) as file:
            day_input = json.load(file)
        collection = await get_collection()
        actual_dataset = await collect_dataset(
            collection,
            datetime.fromisoformat(day_input["dt_from"]),
            datetime.fromisoformat(day_input["dt_upto"]),
            day_input["group_type"]
        )
        self.assertEqual(expected_day_dataset, actual_dataset)
    
    async def test_hour(self):
        path_to_test_cases = os.path.join(proj_dir, "src", "test_cases", "hour")
        with open(os.path.join(path_to_test_cases, "hour.json")) as file:
            expected_day_dataset = json.load(file)
        with open(os.path.join(path_to_test_cases, "hour_input.json")) as file:
            hour_input = json.load(file)
        collection = await get_collection()
        actual_dataset = await collect_dataset(
            collection,
            datetime.fromisoformat(hour_input["dt_from"]),
            datetime.fromisoformat(hour_input["dt_upto"]),
            hour_input["group_type"]
        )
        self.assertEqual(expected_day_dataset, actual_dataset)
