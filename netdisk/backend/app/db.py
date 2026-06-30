"""SQLite 元数据库: 虚拟目录树(nodes) + 分片清单(chunks) + 杂项(meta)。

该 sqlite 文件本身会被整体上传到云端(分片), 换设备时下载回来即可恢复全部元数据。
"""
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER NOT NULL DEFAULT 0,   -- 0 表示根目录
    name      TEXT    NOT NULL,
    type      TEXT    NOT NULL,             -- 'dir' | 'file'
    size      INTEGER NOT NULL DEFAULT 0,
    sha256    TEXT,
    kdocs_id  TEXT,                         -- 云端真实对象id: 目录=文件夹id; 单片文件=文件id; 大文件=分片子文件夹id
    ctime     INTEGER NOT NULL,
    mtime     INTEGER NOT NULL,
    UNIQUE(parent_id, name)
);
CREATE TABLE IF NOT EXISTS chunks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id      INTEGER NOT NULL,
    idx          INTEGER NOT NULL,          -- 分片序号(从0)
    kdocs_fileid TEXT    NOT NULL,          -- 云文档 fileid
    size         INTEGER NOT NULL,
    md5          TEXT,
    sha1         TEXT,
    UNIQUE(node_id, idx)
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
CREATE INDEX IF NOT EXISTS idx_nodes_parent ON nodes(parent_id);
CREATE INDEX IF NOT EXISTS idx_chunks_node  ON chunks(node_id);
"""


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_schema(conn: sqlite3.Connection):
    conn.executescript(SCHEMA)
    conn.commit()


def now() -> int:
    return int(time.time())


# ---------- 查询 ----------
def list_children(conn, parent_id: int):
    cur = conn.execute(
        "SELECT * FROM nodes WHERE parent_id=? ORDER BY type DESC, name ASC",
        (parent_id,))
    return [dict(r) for r in cur.fetchall()]


def get_node(conn, node_id: int):
    cur = conn.execute("SELECT * FROM nodes WHERE id=?", (node_id,))
    r = cur.fetchone()
    return dict(r) if r else None


def child_by_name(conn, parent_id: int, name: str):
    cur = conn.execute("SELECT * FROM nodes WHERE parent_id=? AND name=?", (parent_id, name))
    r = cur.fetchone()
    return dict(r) if r else None


def get_chunks(conn, node_id: int):
    cur = conn.execute(
        "SELECT * FROM chunks WHERE node_id=? ORDER BY idx ASC", (node_id,))
    return [dict(r) for r in cur.fetchall()]


def node_path(conn, node_id: int):
    """返回从根到该节点的列表 [{id,name},...] 供面包屑使用。"""
    out = []
    nid = node_id
    while nid and nid != 0:
        n = get_node(conn, nid)
        if not n:
            break
        out.append({"id": n["id"], "name": n["name"]})
        nid = n["parent_id"]
    out.reverse()
    return out


# ---------- 写入 ----------
def create_dir(conn, parent_id: int, name: str, kdocs_id=None) -> int:
    t = now()
    cur = conn.execute(
        "INSERT INTO nodes(parent_id,name,type,size,kdocs_id,ctime,mtime) VALUES(?,?,?,0,?,?,?)",
        (parent_id, name, "dir", str(kdocs_id) if kdocs_id else None, t, t))
    conn.commit()
    return cur.lastrowid


def create_file(conn, parent_id: int, name: str, size: int, sha256: str, chunks: list, kdocs_id=None) -> int:
    t = now()
    cur = conn.execute(
        "INSERT INTO nodes(parent_id,name,type,size,sha256,kdocs_id,ctime,mtime) VALUES(?,?,?,?,?,?,?,?)",
        (parent_id, name, "file", size, sha256, str(kdocs_id) if kdocs_id else None, t, t))
    node_id = cur.lastrowid
    for c in chunks:
        conn.execute(
            "INSERT INTO chunks(node_id,idx,kdocs_fileid,size,md5,sha1) VALUES(?,?,?,?,?,?)",
            (node_id, c["idx"], str(c["fileid"]), c["size"], c.get("md5"), c.get("sha1")))
    conn.commit()
    return node_id


def rename_node(conn, node_id: int, new_name: str):
    conn.execute("UPDATE nodes SET name=?, mtime=? WHERE id=?", (new_name, now(), node_id))
    conn.commit()


def delete_node_row(conn, node_id: int):
    conn.execute("DELETE FROM chunks WHERE node_id=?", (node_id,))
    conn.execute("DELETE FROM nodes WHERE id=?", (node_id,))
    conn.commit()


def collect_subtree(conn, node_id: int):
    """返回该节点(含)及其所有后代的节点列表(深度优先), 用于递归删除。"""
    result = []
    stack = [node_id]
    while stack:
        nid = stack.pop()
        n = get_node(conn, nid)
        if not n:
            continue
        result.append(n)
        if n["type"] == "dir":
            for ch in list_children(conn, nid):
                stack.append(ch["id"])
    return result


def get_meta(conn, key: str, default=None):
    cur = conn.execute("SELECT value FROM meta WHERE key=?", (key,))
    r = cur.fetchone()
    return r["value"] if r else default


def set_meta(conn, key: str, value: str):
    conn.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
    conn.commit()
