# test_task_queue.py
import unittest
import threading
import time
from backend.core.metadata.worker import PriorityTaskQueue, MetadataTask


class TestPriorityTaskQueue(unittest.TestCase):
    def setUp(self):
        self.queue = PriorityTaskQueue()

        # Create test tasks with different priorities
        self.high_task = MetadataTask("test", "conn-1", {}, "high")
        self.medium_task = MetadataTask("test", "conn-1", {}, "medium")
        self.low_task = MetadataTask("test", "conn-1", {}, "low")

    def test_put_and_get(self):
        # Add tasks to queue
        self.queue.put(self.low_task)
        self.queue.put(self.medium_task)
        self.queue.put(self.high_task)

        # Verify task counts
        stats = self.queue.get_stats()
        self.assertEqual(stats["high"], 1)
        self.assertEqual(stats["medium"], 1)
        self.assertEqual(stats["low"], 1)
        self.assertEqual(stats["total"], 3)

        # Get tasks and verify priority order
        task1 = self.queue.get(block=False)
        self.assertEqual(task1.priority, "high")

        task2 = self.queue.get(block=False)
        self.assertEqual(task2.priority, "medium")

        task3 = self.queue.get(block=False)
        self.assertEqual(task3.priority, "low")

        # Verify queue is empty
        self.assertTrue(self.queue.empty())

    def test_blocking_get(self):
        # Test blocking get with timeout
        result = self.queue.get(block=True, timeout=0.1)
        self.assertIsNone(result)

        # Add task in another thread
        def add_task():
            time.sleep(0.2)
            self.queue.put(self.high_task)

        thread = threading.Thread(target=add_task)
        thread.start()

        # Get should block until task is added
        start_time = time.time()
        task = self.queue.get(block=True)
        elapsed = time.time() - start_time

        self.assertIsNotNone(task)
        self.assertEqual(task.priority, "high")
        self.assertTrue(elapsed >= 0.2)

        thread.join()