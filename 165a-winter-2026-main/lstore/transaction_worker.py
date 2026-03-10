import threading
from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:
    def __init__(self, transactions = None):
        # Initialize transactionworker object
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.thread = None

    def add_transaction(self, t):
        # Append t to transaction
        self.transactions.append(t)

    def run(self):
        # Run transactions as thread
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()
    
    def join(self):
        # Wait for worker
        if self.thread is not None:
            self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            # Transactions returns True if committed or False if aborted
            while True:
                result = transaction.run()
                if result == True:
                    self.stats.append(True)
                    break
                # Transaction aborted — retry
                transaction.rollback_log = []
                transaction.txn_id = transaction.__class__._next_id
                transaction.__class__._next_id += 1

        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

