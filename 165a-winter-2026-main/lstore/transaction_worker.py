import threading
import time
import random
from lstore.table import Table, Record
from lstore.index import Index


class TransactionWorker:

    def __init__(self, transactions=None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.thread = None

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()

    def join(self):
        if self.thread is not None:
            self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            retry_count = 0
            while True:
                result = transaction.run()
                if result == True:
                    self.stats.append(True)
                    break

                # Transaction aborted — give it a new ID and retry
                transaction.txn_id = transaction.__class__._next_id
                transaction.__class__._next_id += 1
                retry_count += 1

                # Random backoff to avoid livelock (all threads retrying at same time)
                # Waits between 0 and 10ms, increasing slightly with retry count
                wait = random.uniform(0, min(0.01 * retry_count, 0.05))
                time.sleep(wait)

        self.result = len(list(filter(lambda x: x, self.stats)))