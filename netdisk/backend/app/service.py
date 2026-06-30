"""业务服务层: 列目录/建夹/上传/下载/删除/重命名。

每个会改动元数据的操作都在全局锁内完成, 并在成功后立即把 SQLite 回传云端,
保证本地与云端一致、断点可恢复。
"""
import hashlib
import math
import threading
import time
import uuid

from . import db as dbm
from . import storage
from .applog import logger
from .kdocs import md5_hex, sha1_hex
from .sync import NetDiskSync


class ServiceError(Exception):
    pass


class NetDiskService:
    def __init__(self, sync: NetDiskSync):
        self.sync = sync
        self.kdocs = sync.kdocs
        self.lock = threading.RLock()
        self.sessions = {}            # upload_id -> session dict
        self.sessions_lock = threading.Lock()

    @property
    def conn(self):
        return self.sync.get_conn()

    # ---------- 读 ----------
    def list_dir(self, parent_id: int):
        with self.lock:
            nodes = dbm.list_children(self.conn, parent_id)
            crumbs = dbm.node_path(self.conn, parent_id) if parent_id else []
            return {"parent_id": parent_id, "breadcrumb": crumbs, "nodes": nodes,
                    "version": self.sync.version}

    def get_node(self, node_id: int):
        with self.lock:
            return dbm.get_node(self.conn, node_id)

    # ---------- 写 ----------
    def mkdir(self, parent_id: int, name: str):
        name = (name or "").strip()
        if not name:
            raise ServiceError("文件夹名不能为空")
        with self.lock:
            self._ensure_parent_dir(parent_id)
            if dbm.child_by_name(self.conn, parent_id, name):
                raise ServiceError(f"已存在同名项: {name}")
            # 在云端真实建出同名文件夹(镜像)
            real_parent = self._real_parent_id(parent_id)
            kid = int(self.kdocs.mkdir(real_parent, name)["id"])
            node_id = dbm.create_dir(self.conn, parent_id, name, kdocs_id=kid)
            self.sync.push_db()
            return dbm.get_node(self.conn, node_id)

    def upload_file(self, parent_id: int, name: str, fileobj):
        name = (name or "").strip()
        if not name:
            raise ServiceError("文件名不能为空")
        with self.lock:
            self._ensure_parent_dir(parent_id)
            existing = dbm.child_by_name(self.conn, parent_id, name)
            if existing and existing["type"] == "dir":
                raise ServiceError(f"已存在同名文件夹: {name}")

            # 同名文件 -> 覆盖: 先删旧云端真实对象(避免重名), 再上传新的
            if existing and existing["type"] == "file":
                self._delete_node_cloud(existing)
                dbm.delete_node_row(self.conn, existing["id"])

            # 镜像上传到真实父目录(单片=真实文件; 大文件=同名子文件夹存分片)
            real_parent = self._real_parent_id(parent_id)
            chunks, total, sha256, kid = storage.mirror_upload(
                self.kdocs, real_parent, name, fileobj, self.sync.chunk_size)

            node_id = dbm.create_file(self.conn, parent_id, name, total, sha256, chunks, kdocs_id=kid)
            self.sync.push_db()
            return dbm.get_node(self.conn, node_id)

    # ---------- 分片上传(浏览器端切片) ----------
    def _adopt_partial(self, folder_id: int, name: str, size: int, cs: int, num_chunks: int):
        """检查已存在的同名子文件夹里的 .partNNNN, 若与本次大小/分片一致则采纳,
        返回 {idx: chunk} 用于续传跳过; 任一片不一致则返回 None 表示应清理重来。"""
        prefix = name + ".part"
        adopted = {}
        for f in self.kdocs.list_dir(folder_id):
            fn = f.get("fname", "")
            if f.get("ftype") != "file" or not fn.startswith(prefix):
                continue
            try:
                idx = int(fn[len(prefix):]) - 1
            except ValueError:
                continue
            if idx < 0 or idx >= num_chunks:
                return None
            expected = cs if idx < num_chunks - 1 else size - (num_chunks - 1) * cs
            if int(f.get("fsize", -1)) != expected:
                return None
            adopted[idx] = {"idx": idx, "fileid": f["id"], "size": expected,
                            "md5": None, "sha1": f.get("fsha") or None}
        return adopted

    def upload_init(self, parent_id: int, name: str, size: int, resume: bool = True):
        name = (name or "").strip()
        if not name:
            raise ServiceError("文件名不能为空")
        size = int(size)
        cs = self.sync.chunk_size
        num_chunks = max(1, math.ceil(size / cs)) if size > 0 else 1
        multi = size > cs
        adopted = {}
        with self.lock:
            self._ensure_parent_dir(parent_id)
            existing = dbm.child_by_name(self.conn, parent_id, name)
            if existing and existing["type"] == "dir":
                raise ServiceError(f"已存在同名文件夹: {name}")
            # 已完成的同名文件 → 覆盖(删旧对象+db行)
            if existing and existing["type"] == "file":
                self._delete_node_cloud(existing)
                dbm.delete_node_row(self.conn, existing["id"])
            real_parent = self._real_parent_id(parent_id)
            sub_id = None
            if multi:
                ex_folder = self.kdocs.find_child(real_parent, name, ftype="folder")
                if resume and ex_folder:
                    # 续传: 采纳云端已存在且一致的分片
                    part = self._adopt_partial(int(ex_folder["id"]), name, size, cs, num_chunks)
                    if part is not None:
                        sub_id = int(ex_folder["id"])
                        adopted = part
                    else:  # 不一致(可能旧分片大小/内容不同) → 清理重来
                        self.kdocs.delete(int(ex_folder["id"]))
                if sub_id is None:
                    # 清理任何同名孤儿后新建子文件夹
                    orphan = self.kdocs.find_child(real_parent, name)
                    if orphan:
                        self.kdocs.delete(int(orphan["id"]))
                    sub_id = int(self.kdocs.mkdir(real_parent, name)["id"])
            else:
                # 单片文件: 清理同名孤儿(单片无法续传, 直接覆盖)
                orphan = self.kdocs.find_child(real_parent, name)
                if orphan:
                    self.kdocs.delete(int(orphan["id"]))

        upload_id = uuid.uuid4().hex
        now = time.time()
        sess = {
            "parent_id": parent_id, "name": name, "size": size,
            "real_parent": real_parent, "multi": multi, "sub_id": sub_id,
            "num_chunks": num_chunks, "chunks": dict(adopted),
            "kdocs_id": sub_id, "lock": threading.Lock(),
            "status": "uploading", "created": now, "updated": now,
        }
        with self.sessions_lock:
            self.sessions[upload_id] = sess
        done_idx = sorted(adopted.keys())
        logger.info("UPLOAD init id=%s name=%s size=%.1fMB chunks=%d multi=%s 续传已有=%d片",
                    upload_id[:8], name, size / 1024 / 1024, num_chunks, multi, len(done_idx))
        return {"upload_id": upload_id, "num_chunks": num_chunks,
                "chunk_size": cs, "multi": multi, "done_idx": done_idx}

    def upload_part(self, upload_id: str, idx: int, data: bytes):
        with self.sessions_lock:
            sess = self.sessions.get(upload_id)
        if not sess:
            raise ServiceError("上传会话不存在或已过期")
        # 云端上传不持任何锁, 允许多片真正并发
        try:
            if sess["multi"]:
                part_name = f"{sess['name']}.part{idx + 1:04d}"
                res = self.kdocs.upload_blob(sess["sub_id"], part_name, data)
            else:
                res = self.kdocs.upload_blob(sess["real_parent"], sess["name"], data)
        except Exception as e:  # noqa
            sess["status"] = "error"
            logger.error("UPLOAD part FAIL id=%s name=%s idx=%d: %s",
                         upload_id[:8], sess["name"], idx, e)
            raise ServiceError(f"分片 {idx} 上传失败: {e}")
        chunk = {"idx": idx, "fileid": res["id"], "size": len(data),
                 "md5": md5_hex(data), "sha1": sha1_hex(data)}
        with sess["lock"]:
            sess["chunks"][idx] = chunk
            sess["updated"] = time.time()
            if not sess["multi"]:
                sess["kdocs_id"] = res["id"]
            done = len(sess["chunks"])
        logger.info("UPLOAD part id=%s name=%s idx=%d %d/%d %.1fMB",
                    upload_id[:8], sess["name"], idx, done, sess["num_chunks"],
                    len(data) / 1024 / 1024)
        return {"ok": True, "idx": idx}

    def upload_complete(self, upload_id: str):
        with self.sessions_lock:
            sess = self.sessions.get(upload_id)
        if not sess:
            raise ServiceError("上传会话不存在或已过期")
        chunks = [sess["chunks"][i] for i in sorted(sess["chunks"])]
        if len(chunks) != sess["num_chunks"]:
            raise ServiceError(f"分片不完整: 期望 {sess['num_chunks']} 实到 {len(chunks)}")
        total = sum(c["size"] for c in chunks)
        # 与到达顺序无关的整文件指纹: 按 idx 排序后组合各片 sha1。
        # 续传采纳的分片若缺 sha1 则跳过指纹(置空), 不影响下载(按 idx+fileid 重组)。
        sha1s = [c.get("sha1") for c in chunks]
        if all(sha1s):
            h = hashlib.sha256()
            for s in sha1s:
                h.update(bytes.fromhex(s))
            sha256 = h.hexdigest()
        else:
            sha256 = ""
        with self.lock:
            node_id = dbm.create_file(self.conn, sess["parent_id"], sess["name"],
                                      total, sha256, chunks, kdocs_id=sess["kdocs_id"])
            self.sync.push_db()
            node = dbm.get_node(self.conn, node_id)
        with self.sessions_lock:
            self.sessions.pop(upload_id, None)
        logger.info("UPLOAD done id=%s name=%s size=%.1fMB chunks=%d 用时%.0fs",
                    upload_id[:8], sess["name"], total / 1024 / 1024,
                    len(chunks), time.time() - sess["created"])
        return node

    def upload_abort(self, upload_id: str):
        with self.sessions_lock:
            sess = self.sessions.pop(upload_id, None)
        if not sess:
            return False
        logger.info("UPLOAD abort id=%s name=%s 已传%d/%d片",
                    upload_id[:8], sess["name"], len(sess["chunks"]), sess["num_chunks"])
        # 清理已上传的云端分片/子文件夹
        try:
            if sess.get("sub_id"):
                self.kdocs.delete(int(sess["sub_id"]))
            else:
                for c in sess["chunks"].values():
                    self.kdocs.delete(int(c["fileid"]))
        except Exception:
            pass
        return True

    def list_uploads(self):
        """当前服务端已知的上传会话(用于刷新后恢复显示)。"""
        out = []
        with self.sessions_lock:
            for uid, s in self.sessions.items():
                done = len(s["chunks"])
                out.append({
                    "upload_id": uid, "name": s["name"], "size": s["size"],
                    "num_chunks": s["num_chunks"], "done_chunks": done,
                    "pct": min(99, int(done / s["num_chunks"] * 100)) if s["num_chunks"] else 0,
                    "status": s["status"], "created": s["created"], "updated": s["updated"],
                })
        out.sort(key=lambda x: x["created"], reverse=True)
        return out

    def rename(self, node_id: int, new_name: str):
        new_name = (new_name or "").strip()
        if not new_name:
            raise ServiceError("名称不能为空")
        with self.lock:
            node = dbm.get_node(self.conn, node_id)
            if not node:
                raise ServiceError("节点不存在")
            if dbm.child_by_name(self.conn, node["parent_id"], new_name):
                raise ServiceError(f"已存在同名项: {new_name}")
            # 同步重命名云端真实对象(目录/单片文件/大文件子文件夹)
            if node.get("kdocs_id"):
                try:
                    self.kdocs.rename(int(node["kdocs_id"]), new_name)
                except Exception:
                    pass
            dbm.rename_node(self.conn, node_id, new_name)
            self.sync.push_db()
            return dbm.get_node(self.conn, node_id)

    def delete(self, node_id: int):
        with self.lock:
            node = dbm.get_node(self.conn, node_id)
            if not node:
                raise ServiceError("节点不存在")
            subtree = dbm.collect_subtree(self.conn, node_id)
            # 删云端真实对象: 叶子优先(反转前序=后序), 文件夹删除会级联其内容, 这里逐个删更稳妥
            for n in reversed(subtree):
                self._delete_node_cloud(n)
            # 再删 db 行
            for n in reversed(subtree):
                dbm.delete_node_row(self.conn, n["id"])
            self.sync.push_db()
            return True

    # ---------- 下载 ----------
    def open_download(self, node_id: int):
        with self.lock:
            node = dbm.get_node(self.conn, node_id)
            if not node:
                raise ServiceError("节点不存在")
            if node["type"] != "file":
                raise ServiceError("只能下载文件")
            chunks = dbm.get_chunks(self.conn, node_id)
        gen = storage.iter_download(self.kdocs, chunks)
        return node, gen

    # ---------- 内部 ----------
    def _ensure_parent_dir(self, parent_id: int):
        if parent_id == 0:
            return
        p = dbm.get_node(self.conn, parent_id)
        if not p or p["type"] != "dir":
            raise ServiceError("父目录不存在")

    def _real_parent_id(self, parent_id: int) -> int:
        """虚拟父目录 -> 云端真实文件夹id。根目录映射到云端 root_id。"""
        if parent_id == 0:
            return self.sync.root_id
        p = dbm.get_node(self.conn, parent_id)
        if not p or not p.get("kdocs_id"):
            raise ServiceError("父目录在云端不存在")
        return int(p["kdocs_id"])

    def _delete_node_cloud(self, node: dict):
        """删除一个节点对应的云端真实对象。

        - 目录: 删文件夹(级联)
        - 单片文件: 删该文件
        - 大文件: 删同名子文件夹(级联其分片)
        kdocs_id 已涵盖以上三种; 再兜底删分片 fileid 以防万一。
        """
        kid = node.get("kdocs_id")
        if kid:
            try:
                self.kdocs.delete(int(kid))
                return
            except Exception:
                pass
        for c in dbm.get_chunks(self.conn, node["id"]):
            try:
                self.kdocs.delete(int(c["kdocs_fileid"]))
            except Exception:
                pass
