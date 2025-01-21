import time
import math


class TokenBucketFilter:
    def __init__(self, max_size, fill_tokens=10, fill_time_secs = 100):
        self.max_size = max_size
        self.fill_tokens = fill_tokens
        self.fill_time_secs = fill_time_secs
        self.curr_tokens = self.max_size
        self.last_refill_time = time.time()

    def consume(self):
        self._refill()
        if self.curr_tokens >= 1:
            self.curr_tokens -= 1
            return True
        return False

    def _refill(self):
        # Is it time to refill the bucket?
        if time.time() >= self.last_refill_time + self.fill_time_secs and self.curr_tokens < self.max_size:
            tokens_to_add = math.floor((time.time() - self.last_refill_time) // self.fill_time_secs)
            self.curr_tokens  = min(self.max_size, self.curr_tokens + tokens_to_add)
            self.last_refill_time = time.time()
