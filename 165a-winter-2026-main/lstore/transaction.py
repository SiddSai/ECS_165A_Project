from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import get_lock_manager


class Transaction:

    # Class-level counter for transaction UIDs
    _next_id = 0

    def __init__(self):
        self.queries = []
        self.txn_id = Transaction._next_id
        Transaction._next_id += 1
        self.rollback_log = []

    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

    def run(self):
        lm = get_lock_manager()

        # Clear rollback log at the start of each run attempt
        self.rollback_log = []

        for query, table, args in self.queries:
            method_name = query.__name__

            # Snapshot old values BEFORE running the query so we can undo it on abort
            old_snapshot = self._snapshot_before(method_name, table, args)

            # Acquire the appropriate lock before running the query
            if not self._acquire_locks(lm, method_name, table, args):
                return self.abort()

            # Run the query
            result = query(*args)

            # Abort if query failed
            if result == False:
                return self.abort()

            # Log the operation so we can undo it on abort
            self._log_operation(method_name, table, args, old_snapshot)

        return self.commit()

    def _snapshot_before(self, method_name, table, args):
        """
        Read and return the current state of the record before we change it.
        Used to undo the operation if we abort later.
        """
        try:
            if method_name in ('update', 'delete'):
                primary_key = args[0]
                rids = table.index.locate(table.key, primary_key)
                if rids:
                    record = table.read(rids[0])
                    if record:
                        return record.columns[:]
        except Exception:
            pass
        return None

    def _acquire_locks(self, lm, method_name, table, args):
        try:
            if method_name == 'insert':
                # Lock on the key value to prevent two transactions inserting the same key
                key_value = args[0] if args else None
                if key_value is not None:
                    if not lm.acquire_exclusive(('key', table.name, key_value), self.txn_id):
                        return False
                return True

            elif method_name in ('update', 'delete', 'increment'):
                primary_key = args[0]
                rids = table.index.locate(table.key, primary_key)
                if not rids:
                    return True
                for rid in rids:
                    if not lm.acquire_exclusive(('rid', table.name, rid), self.txn_id):
                        return False
                return True

            elif method_name == 'select':
                search_key = args[0]
                search_key_index = args[1]
                rids = table.index.locate(search_key_index, search_key)
                for rid in rids:
                    if not lm.acquire_shared(('rid', table.name, rid), self.txn_id):
                        return False
                return True

            elif method_name in ('sum', 'sum_version'):
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

            return True

        except Exception:
            return False

    def _log_operation(self, method_name, table, args, old_snapshot):
        """
        Record enough info to undo this operation if we abort later.
        """
        if method_name == 'insert':
            self.rollback_log.append({
                'type': 'insert',
                'table': table,
                'primary_key': args[0],
            })

        elif method_name == 'update':
            self.rollback_log.append({
                'type': 'update',
                'table': table,
                'primary_key': args[0],
                'old_values': old_snapshot,
            })

        elif method_name == 'delete':
            self.rollback_log.append({
                'type': 'delete',
                'table': table,
                'old_values': old_snapshot,
            })

    def abort(self):
        lm = get_lock_manager()

        # Undo all operations in reverse order
        for entry in reversed(self.rollback_log):
            try:
                table = entry['table']

                if entry['type'] == 'insert':
                    # Undo insert: delete the record we just inserted
                    rids = table.index.locate(table.key, entry['primary_key'])
                    if rids:
                        table.delete(rids[0])

                elif entry['type'] == 'update':
                    # Undo update: restore the old column values
                    old_values = entry['old_values']
                    if old_values is not None:
                        rids = table.index.locate(table.key, entry['primary_key'])
                        if rids:
                            restore_cols = list(old_values)
                            restore_cols[table.key] = None  # key column cannot be updated
                            table.update(rids[0], restore_cols)

                elif entry['type'] == 'delete':
                    # Undo delete: re-insert the deleted record
                    old_values = entry['old_values']
                    if old_values is not None:
                        rec = Record(None, old_values[table.key], list(old_values))
                        table.insert(rec)

            except Exception:
                pass  # best-effort rollback, keep going even if one step fails

        self.rollback_log = []
        lm.release_all(self.txn_id)
        return False

    def commit(self):
        lm = get_lock_manager()
        self.rollback_log = []
        lm.release_all(self.txn_id)
        return True