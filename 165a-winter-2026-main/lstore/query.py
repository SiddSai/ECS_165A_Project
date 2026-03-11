from lstore.table import Table, Record, USER_COL_OFFSET, RID_COLUMN, INDIRECTION_COLUMN, BASE_RID_COLUMN, NULL_RID
from lstore.index import Index
from lstore.lock_manager import get_lock_manager
_lock_manager = get_lock_manager()


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        self._current_txn_id = None  # 当前是哪个事务在用这个Query，由Transaction.run()注入
        self._undo_actions = []      # 本次操作的撤销方法，由Transaction.run()收走

    # ------------------------------------------------------------------
    # 锁相关的内部方法
    # ------------------------------------------------------------------

    def _acquire_read(self, rid):
        """申请共享锁。没有事务上下文就跳过。"""
        if self._current_txn_id is None:
            return True
        return _lock_manager.acquire_shared(self._current_txn_id, rid)

    def _acquire_write(self, rid):
        """申请排他锁。没有事务上下文就跳过。"""
        if self._current_txn_id is None:
            return True
        return _lock_manager.acquire_exclusive(self._current_txn_id, rid)

    def _record_undo(self, fn, *args):
        """记录一个撤销动作。"""
        self._undo_actions.append((fn, args))

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            rid = rids[0]

            # 写操作申请排他锁
            if not self._acquire_write(rid):
                return False

            # 保存当前数据，用于回滚
            current_rec = self.table.read(rid)
            if current_rec is None:
                return False

            ok = self.table.delete(rid)
            if not ok:
                return False

            # 撤销方法：把删除标记去掉，恢复index
            def _undo_delete(tbl, saved_rid, saved_columns):
                if saved_rid in tbl.page_directory:
                    range_id, is_tail, page_id, offset = tbl.page_directory[saved_rid]
                    bundle = tbl._get_bundle(range_id, is_tail, page_id)
                    bundle[INDIRECTION_COLUMN].update(offset, NULL_RID)
                    tbl._unpin_bundle(range_id, is_tail, page_id, dirty=True)
                    tbl.index.insert(saved_columns[tbl.key], saved_rid)
                    for col in range(tbl.num_columns):
                        if col != tbl.key:
                            tbl.index.insert_secondary(col, saved_columns[col], saved_rid)

            self._record_undo(_undo_delete, self.table, rid, current_rec.columns)
            return True
        except Exception:
            return False

    def insert(self, *columns):
        try:
            if len(columns) != self.table.num_columns:
                return False
            if any(v is None for v in columns):
                return False

            record = Record(None, columns[self.table.key], list(columns))
            ok = self.table.insert(record)
            if not ok:
                return False

            # insert成功后拿到新RID
            new_rid = self.table.next_rid - 1

            # 申请排他锁
            if not self._acquire_write(new_rid):
                self.table.delete(new_rid)
                return False

            # 撤销方法：删掉这条新插入的记录
            def _undo_insert(tbl, ins_rid):
                tbl.delete(ins_rid)

            self._record_undo(_undo_insert, self.table, new_rid)
            return True

        except Exception:
            return False

    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            rids = self.table.index.locate(search_key_index, search_key)

            if not rids and self.table.index.indices[search_key_index] is None:
                rids = self._full_scan(search_key, search_key_index)

            results = []
            for rid in rids:
                # 读操作申请共享锁
                if not self._acquire_read(rid):
                    return False

                rec_obj = self.table.read(rid)
                if rec_obj is None:
                    continue

                full_cols = rec_obj.columns
                projected = [None] * self.table.num_columns
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        projected[i] = full_cols[i]

                key_val = full_cols[self.table.key]
                results.append(Record(rid, key_val, projected))

            return results
        except Exception:
            return False

    def _full_scan(self, search_key, search_key_index):
        rids = []
        for rid, (range_id, is_tail, page_id, offset) in self.table.page_directory.items():
            if is_tail:
                continue
            rec = self.table.read(rid)
            if rec is None:
                continue
            if rec.columns[search_key_index] == search_key:
                rids.append(rid)
        return rids

    def _full_scan_base_rids(self, search_key, search_key_index):
        rids = []
        for rid, (range_id, is_tail, page_id, offset) in self.table.page_directory.items():
            if is_tail:
                continue
            rec = self.table.read(rid)
            if rec is None:
                continue
            if rec.columns[search_key_index] == search_key:
                rids.append(rid)
        return rids

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            if relative_version == 0:
                return self.select(search_key, search_key_index, projected_columns_index)

            rids = self.table.index.locate(search_key_index, search_key)

            if not rids and self.table.index.indices[search_key_index] is None:
                rids = self._full_scan_base_rids(search_key, search_key_index)

            if not rids:
                return False

            results = []
            for base_rid in rids:
                if base_rid not in self.table.page_directory:
                    continue

                if not self._acquire_read(base_rid):
                    return False

                range_id, is_tail, page_id, offset = self.table.page_directory[base_rid]
                if is_tail:
                    continue

                base_bundle = self.table._get_bundle(range_id, False, page_id)
                latest_tail_rid = base_bundle[INDIRECTION_COLUMN].read(offset)
                self.table._unpin_bundle(range_id, False, page_id, dirty=False)

                current_rid = latest_tail_rid
                steps = abs(relative_version)

                if current_rid == NULL_RID:
                    current_rid = base_rid

                for _ in range(steps):
                    if current_rid == NULL_RID:
                        current_rid = base_rid
                        break
                    if current_rid == self.table.deleted:
                        break
                    if current_rid not in self.table.page_directory:
                        current_rid = base_rid
                        break

                    t_range, t_is_tail, t_page_id, t_offset = self.table.page_directory[current_rid]

                    if not t_is_tail:
                        break

                    tail_bundle = self.table._get_bundle(t_range, True, t_page_id)
                    next_rid = tail_bundle[INDIRECTION_COLUMN].read(t_offset)
                    self.table._unpin_bundle(t_range, True, t_page_id, dirty=False)

                    if next_rid == NULL_RID:
                        current_rid = base_rid
                        break
                    else:
                        current_rid = next_rid

                rec_obj = self.table._read_without_indirection(current_rid)
                if rec_obj is None:
                    continue

                projected = [None] * self.table.num_columns
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        projected[i] = rec_obj.columns[i]

                results.append(Record(current_rid, rec_obj.key, projected))

            return results if results else False
        except Exception:
            return False

    def update(self, primary_key, *columns):
        try:
            if len(columns) != self.table.num_columns:
                return False

            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            base_rid = rids[0]

            # 写操作申请排他锁
            if not self._acquire_write(base_rid):
                return False

            # 保存修改前的数据，用于回滚
            current_rec = self.table.read(base_rid)
            if current_rec is None:
                return False

            ok = self.table.update(base_rid, list(columns))
            if not ok:
                return False

            # 拿到新生成的tail RID
            new_tail_rid = self.table.next_tail_rid - 1

            # 读出新tail的INDIRECTION（指向上一个tail），回滚时需要用它恢复链
            t_range, t_is_tail, t_page_id, t_offset = self.table.page_directory[new_tail_rid]
            t_bundle = self.table._get_bundle(t_range, t_is_tail, t_page_id)
            prev_indirection = t_bundle[INDIRECTION_COLUMN].read(t_offset)
            self.table._unpin_bundle(t_range, t_is_tail, t_page_id, dirty=False)

            # 撤销方法：把新tail标记删除，把base的indirection指针改回去
            def _undo_update(tbl, b_rid, t_rid, prev_ind, old_values):
                # 把新tail标记为删除
                if t_rid in tbl.page_directory:
                    tr, tt, tp, to = tbl.page_directory[t_rid]
                    tb = tbl._get_bundle(tr, tt, tp)
                    tb[INDIRECTION_COLUMN].update(to, tbl.deleted)
                    tbl._unpin_bundle(tr, tt, tp, dirty=True)

                # 恢复base的indirection指针
                if b_rid in tbl.page_directory:
                    br, bt, bp, bo = tbl.page_directory[b_rid]
                    bb = tbl._get_bundle(br, bt, bp)
                    bb[INDIRECTION_COLUMN].update(bo, prev_ind)
                    tbl._unpin_bundle(br, bt, bp, dirty=True)

                # 恢复secondary index
                for col in range(tbl.num_columns):
                    if col == tbl.key or tbl.index.indices[col] is None:
                        continue
                    tbl.index.insert_secondary(col, old_values[col], b_rid)

            self._record_undo(_undo_update, self.table, base_rid, new_tail_rid,
                              prev_indirection, current_rec.columns)
            return True

        except Exception:
            return False

    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False
            total = 0
            col = aggregate_column_index + USER_COL_OFFSET
            for rid in rids:
                if not self._acquire_read(rid):
                    return False

                if rid not in self.table.page_directory:
                    continue
                range_id, is_tail, page_id, offset = self.table.page_directory[rid]
                if not is_tail:
                    bundle = self.table._get_bundle(range_id, False, page_id)
                    indirection = bundle[INDIRECTION_COLUMN].read(offset)
                    if (indirection != NULL_RID and indirection != self.table.deleted
                            and indirection in self.table.page_directory):
                        self.table._unpin_bundle(range_id, False, page_id, dirty=False)
                        r2, t2, p2, o2 = self.table.page_directory[indirection]
                        tail_bundle = self.table._get_bundle(r2, True, p2)
                        total += tail_bundle[col].read(o2)
                        self.table._unpin_bundle(r2, True, p2, dirty=False)
                        continue
                    if indirection != self.table.deleted:
                        total += bundle[col].read(offset)
                    self.table._unpin_bundle(range_id, False, page_id, dirty=False)
                else:
                    bundle = self.table._get_bundle(range_id, True, page_id)
                    total += bundle[col].read(offset)
                    self.table._unpin_bundle(range_id, True, page_id, dirty=False)
            return total
        except Exception:
            return False

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            total = 0
            found_any = False
            projection = [0] * self.table.num_columns
            projection[aggregate_column_index] = 1

            for key in range(start_range, end_range + 1):
                recs = self.select_version(key, self.table.key, projection, relative_version)
                if recs is False:
                    continue
                if len(recs) > 0:
                    found_any = True
                    value = recs[0].columns[aggregate_column_index]
                    if value is not None:
                        total += value

            return total if found_any else False
        except Exception:
            return False

    def increment(self, key, column):
        recs = self.select(key, self.table.key, [1] * self.table.num_columns)
        if recs is False or len(recs) == 0:
            return False
        r = recs[0]
        updated_columns = [None] * self.table.num_columns
        updated_columns[column] = r.columns[column] + 1
        return self.update(key, *updated_columns)