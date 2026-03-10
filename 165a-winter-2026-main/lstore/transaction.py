from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import get_lock_manager

class Transaction:

    # Class-level counter for transaction UIDs
    _next_id = 0

    def __init__(self):
        self.queries = []
        # Each transaction gets a UID so the lock manager can track
        self.txn_id = Transaction._next_id
        Transaction._next_id += 1
        # Stores info needed for abort
        self.rollback_log = []


    def add_query(self, query, table, *args):
        # Add given query to transaction
        self.queries.append((query, table, args))

    def run(self):
        lm = get_lock_manager()

        for query, table, args in self.queries:
            # Get method name to determine necessary lock type
            method_name = query.__name__

            # Acquire the appropriate lock before running the query.
            lock_acquired = self._acquire_locks(lm, method_name, table, args)
            if not lock_acquired:
                return self.abort()

            # Run query
            result = query(*args)

            # Abort transaction if fail
            if result == False:
                return self.abort()

            # Log operation
            self._log_operation(method_name, table, args, result)

        return self.commit()

    def _acquire_locks(self, lm, method_name, table, args):
        if method_name == 'insert':
            # Insert creates a new record
            key_value = args[0] if args else None
            if key_value is not None:
                if not lm.acquire_exclusive(('key', table.name, key_value), self.txn_id):
                    return False
            return True

        elif method_name == 'update':
            # args:(primary_key, col1, col2, ...)
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            if not rids:
                return True 
            for rid in rids:
                if not lm.acquire_exclusive(('rid', table.name, rid), self.txn_id):
                    return False
            return True

        elif method_name == 'delete':
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            if not rids:
                return True
            for rid in rids:
                if not lm.acquire_exclusive(('rid', table.name, rid), self.txn_id):
                    return False
            return True

        elif method_name == 'select':
            # args: (search_key, search_key_index, projected_columns_index)
            search_key = args[0]
            search_key_index = args[1]
            rids = table.index.locate(search_key_index, search_key)
            for rid in rids:
                if not lm.acquire_shared(('rid', table.name, rid), self.txn_id):
                    return False
            return True

        elif method_name == 'sum':
            # args: (start_range, end_range, aggregate_column_index)
            start_range = args[0]
            end_range = args[1]
            rids = table.index.locate_range(start_range, end_range, table.key)
            for rid in rids:
                if not lm.acquire_shared(('rid', table.name, rid), self.txn_id):
                    return False
            return True

        elif method_name == 'select_version':
            search_key = args[0]
            search_key_index = args[1]
            rids = table.index.locate(search_key_index, search_key)
            for rid in rids:
                if not lm.acquire_shared(('rid', table.name, rid), self.txn_id):
                    return False
            return True

        elif method_name == 'sum_version':
            start_range = args[0]
            end_range = args[1]
            rids = table.index.locate_range(start_range, end_range, table.key)
            for rid in rids:
                if not lm.acquire_shared(('rid', table.name, rid), self.txn_id):
                    return False
            return True

        elif method_name == 'increment':
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            for rid in rids:
                if not lm.acquire_exclusive(('rid', table.name, rid), self.txn_id):
                    return False
            return True
        
        # No locks needed
        return True

    def _log_operation(self, method_name, table, args, result):
        if method_name == 'insert':
            # Delete record on rollback
            primary_key = args[0]
            self.rollback_log.append({
                'type': 'insert',
                'table': table,
                'primary_key': primary_key,
            })

        elif method_name == 'update':
            # Mark that update happened
            # Store primary key so we can undo.
            primary_key = args[0]
            self.rollback_log.append({
                'type': 'update',
                'table': table,
                'primary_key': primary_key,
            })

        elif method_name == 'delete':
            primary_key = args[0]
            self.rollback_log.append({
                'type': 'delete',
                'table': table,
                'primary_key': primary_key,
            })

    def abort(self):
        lm = get_lock_manager()

        # Undo operations by walking rollback in reverse
        for entry in reversed(self.rollback_log):
            try:
                if entry['type'] == 'insert':
                    # Undo insert by deleting the record we inserted
                    table = entry['table']
                    primary_key = entry['primary_key']
                    rids = table.index.locate(table.key, primary_key)
                    if rids:
                        table.delete(rids[0])
            except Exception:
                pass

        self.rollback_log = []

        # Release all locks held by this transaction
        lm.release_all(self.txn_id)
        return False

    def commit(self):
        lm = get_lock_manager()
        self.rollback_log = []
        lm.release_all(self.txn_id)
        return True