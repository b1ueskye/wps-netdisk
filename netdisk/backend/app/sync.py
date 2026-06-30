"""DB 同步层 —— 整个系统可迁移性的核心。

云端布局(镜像模式, 都在 group 下):
    <root_name>/                固定根目录(列 parentid=0 按名发现), 用户文件夹/文件直接镜像到这里
        _netdisk_manifest.json  极小、固定名: {version, db_chunks, db_sha256, chunk_size}
        _netdisk_sys/           SQLite 文件的分片(系统用)
        哈哈哈哈/                ← 用户真实文件夹(镜像)
        sso.txt                 ← 用户真实文件(<=2G 单片)
        大视频.mp4/             ← 用户大文件(>2G): 同名子文件夹内放 .partNNNN

自举(换设备只给 Cookie): 列根目录 -> 读 manifest -> 下载 db 分片拼回 SQLite。
一致性(单写者乐观锁): manifest.version 为权威版本; 每次回传前复查云端版本未变才提交,
提交点 = 覆盖 manifest。先传新 db 分片, 最后换 manifest, 半路失败不破坏旧状态。
"""
import hashlib
import json
import os

from . import db as dbm
from . import storage
from .kdocs import KdocsClient

MANIFEST_NAME = "_netdisk_manifest.json"
SYS_DIR = "_netdisk_sys"


class ConflictError(Exception):
    """云端版本与本地基线不一致(可能另一台设备写过)。"""


class NetDiskSync:
    def __init__(self, kdocs: KdocsClient, db_path: str, root_name: str, chunk_size: int):
        self.kdocs = kdocs
        self.db_path = db_path
        self.root_name = root_name
        self.chunk_size = chunk_size

        self.root_id = None
        self.db_dir_id = None
        self.manifest_fileid = None
        self.version = 0
        self.db_chunk_fileids = []   # 当前 db 在云端的分片 fileid, 用于回传后清理旧分片
        self.conn = None

    # ---------- 云端目录解析 ----------
    def _resolve_folders(self):
        self.root_id = self.kdocs.find_or_create_folder(0, self.root_name)
        self.db_dir_id = self.kdocs.find_or_create_folder(self.root_id, SYS_DIR)

    def _read_remote_manifest(self):
        f = self.kdocs.find_child(self.root_id, MANIFEST_NAME, ftype="file")
        if not f:
            return None, None
        data = self.kdocs.download_blob(int(f["id"]))
        return int(f["id"]), json.loads(data.decode("utf-8"))

    # ---------- 自举 ----------
    def bootstrap(self):
        self._resolve_folders()
        manifest_id, manifest = self._read_remote_manifest()

        if manifest:
            self.manifest_fileid = manifest_id
            self.version = int(manifest.get("version", 0))
            chunks = manifest.get("db_chunks", [])
            # 适配 storage 下载所需的字段名
            dl = [{"idx": c["idx"], "kdocs_fileid": c["fileid"]} for c in chunks]
            data = storage.download_all(self.kdocs, dl)
            sha = hashlib.sha256(data).hexdigest()
            if manifest.get("db_sha256") and sha != manifest["db_sha256"]:
                raise RuntimeError("云端 DB 分片校验失败(sha256 不一致)")
            self._write_local_db(data)
            self.db_chunk_fileids = [int(c["fileid"]) for c in chunks]
            self.conn = dbm.connect(self.db_path)
            dbm.init_schema(self.conn)
        else:
            # 全新: 建空库并首次回传
            self._reset_local_db()
            self.conn = dbm.connect(self.db_path)
            dbm.init_schema(self.conn)
            self.version = 0
            self.manifest_fileid = None
            self.db_chunk_fileids = []
            self.push_db()
        return self

    def _reset_local_db(self):
        for suffix in ("", "-wal", "-shm"):
            p = self.db_path + suffix
            if os.path.exists(p):
                os.remove(p)

    def _write_local_db(self, data: bytes):
        self._reset_local_db()
        with open(self.db_path, "wb") as f:
            f.write(data)

    # ---------- 回传(提交) ----------
    def push_db(self):
        """把本地 SQLite 整体分片上传并切换 manifest, 版本号 +1。单写者乐观锁保护。"""
        # 1) 复查云端版本(防止另一设备写过)
        if self.manifest_fileid is not None:
            _mid, remote = self._read_remote_manifest()
            remote_ver = int(remote.get("version", 0)) if remote else 0
            if remote_ver != self.version:
                raise ConflictError(
                    f"云端版本({remote_ver}) 与本地基线({self.version}) 不一致, 可能有另一设备在写。")

        # 2) 取一致的 DB 文件(WAL checkpoint 到主文件)
        if self.conn is not None:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except Exception:
                pass
        with open(self.db_path, "rb") as f:
            data = f.read()
        sha = hashlib.sha256(data).hexdigest()

        # 3) 上传新 db 分片
        new_chunks, _total, _sha = storage.upload_bytes(
            self.kdocs, self.db_dir_id, data, self.chunk_size)
        manifest_chunks = [{"idx": c["idx"], "fileid": c["fileid"], "size": c["size"]}
                           for c in new_chunks]

        # 4) 写 manifest (提交点)
        new_version = self.version + 1
        manifest = {
            "version": new_version,
            "db_sha256": sha,
            "chunk_size": self.chunk_size,
            "db_chunks": manifest_chunks,
        }
        mbytes = json.dumps(manifest, ensure_ascii=False).encode("utf-8")
        if self.manifest_fileid:
            self.kdocs.upload_blob(self.root_id, MANIFEST_NAME, mbytes,
                                   file_id=self.manifest_fileid, up_new_ver=True)
        else:
            res = self.kdocs.upload_blob(self.root_id, MANIFEST_NAME, mbytes)
            self.manifest_fileid = int(res["id"])

        # 5) 清理旧 db 分片
        old = self.db_chunk_fileids
        self.db_chunk_fileids = [int(c["fileid"]) for c in new_chunks]
        self.version = new_version
        for fid in old:
            try:
                self.kdocs.delete(fid)
            except Exception:
                pass

    def get_conn(self):
        return self.conn
