"""kdocs(WPS 云文档) 客户端。把云文档当作 blob 存储, 只用 Cookie 鉴权。

所有接口均已用真实账号实测:
  - 上传/覆盖: PUT  /api/v5/files/upload/create_update -> PUT <store url> -> POST /api/v5/files/file
  - 下载:      GET  https://www.kdocs.cn/api/v3/office/file/{id}/download -> GET <presigned url>
  - 建文件夹:  POST /api/v5/files/folder
  - 删除:      DELETE /api/v3/groups/{gid}/files/{id}
  - 列目录:    GET  /api/v5/groups/{gid}/files?parentid=...
"""
import hashlib
import json
import urllib.parse

import requests

DRIVE = "https://drive.kdocs.cn"
WWW = "https://www.kdocs.cn"

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
)


class KdocsError(Exception):
    pass


def md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def sha1_hex(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


class KdocsClient:
    def __init__(self, cookie: str, group_id: int, csrf: str):
        self.cookie = cookie
        self.group_id = int(group_id)
        self.csrf = csrf
        self.sess = requests.Session()
        self.sess.headers.update({
            "user-agent": _UA,
            "origin": WWW,
            "referer": WWW + "/",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "cookie": cookie,
        })

    # ---------- 自动发现私有空间 group ----------
    def discover_group_id(self) -> int:
        """用 Cookie 取「我的云文档」私有空间 group id, 无需用户手填。"""
        r = self.sess.get(f"{DRIVE}/api/v3/groups/special")
        r.raise_for_status()
        gid = int(r.json()["id"])
        self.group_id = gid
        return gid

    # ---------- 列目录 ----------
    def list_dir(self, parent_id: int, offset: int = 0, count: int = 200) -> list:
        out = []
        while True:
            r = self.sess.get(
                f"{DRIVE}/api/v5/groups/{self.group_id}/files",
                params={"parentid": parent_id, "offset": offset, "count": count,
                        "orderby": "fname", "order": "asc"},
            )
            r.raise_for_status()
            d = r.json()
            files = d.get("files") or d.get("list") or []
            out.extend(files)
            if len(files) < count:
                break
            offset += count
        return out

    def find_child(self, parent_id: int, name: str, ftype: str = None):
        for f in self.list_dir(parent_id):
            if f.get("fname") == name and (ftype is None or f.get("ftype") == ftype):
                return f
        return None

    # ---------- 建文件夹 ----------
    def mkdir(self, parent_id: int, name: str) -> dict:
        body = {"groupid": self.group_id, "parentid": int(parent_id), "name": name,
                "parsed": True, "owner": True, "csrfmiddlewaretoken": self.csrf}
        r = self.sess.post(f"{DRIVE}/api/v5/files/folder",
                           data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                           headers={"content-type": "application/json"})
        r.raise_for_status()
        return r.json()

    def find_or_create_folder(self, parent_id: int, name: str) -> int:
        f = self.find_child(parent_id, name, ftype="folder")
        if f:
            return int(f["id"])
        return int(self.mkdir(parent_id, name)["id"])

    # ---------- 删除 ----------
    def delete(self, file_id: int) -> bool:
        r = self.sess.delete(f"{DRIVE}/api/v3/groups/{self.group_id}/files/{file_id}")
        # 已不存在也视作成功
        if r.status_code in (200, 403, 404):
            return True
        r.raise_for_status()
        return True

    # ---------- 上传 / 覆盖 ----------
    def _create_update(self, parent_id, name, size, md5, file_id=0):
        body = {
            "groupid": self.group_id, "parentid": int(parent_id), "parent_path": [],
            "size": size, "name": name, "req_by_internal": False,
            "client_stores": "ks3", "contenttype": "application/octet-stream",
            "startswithfilename": name, "successactionstatus": 201,
            "group_id": self.group_id, "parent_id": int(parent_id),
            "file_id": int(file_id), "with_rapid": True, "tried_store": [],
            "md5": md5, "csrfmiddlewaretoken": self.csrf,
        }
        r = self.sess.put(f"{DRIVE}/api/v5/files/upload/create_update",
                          data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                          headers={"content-type": "application/json"})
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _extract(spec, resp):
        if not spec:
            return None
        where, _, name = spec.partition(".")
        if where == "header":
            return resp.headers.get(name)
        if where == "body":
            try:
                return resp.json().get(name)
            except ValueError:
                return None
        return None

    def _put_store(self, plan, data):
        headers = dict(plan["request"]["headers"])
        r = self.sess.put(plan["url"], data=data, headers=headers)
        r.raise_for_status()
        spec = plan.get("response", {})
        key = self._extract(spec.get("args_key"), r)
        etag = self._extract(spec.get("args_etag"), r) or r.headers.get("ETag")
        return key, etag

    def _commit(self, parent_id, name, key, md5, sha1, size, store, file_id=0, up_new_ver=False):
        body = {
            "key": key, "groupid": self.group_id, "parentid": int(parent_id),
            "parent_path": [], "name": name, "isUpNewVer": up_new_ver,
            "etag": f'"{md5}"', "store": store, "size": size, "sha1": sha1,
            "apiErrorInfo": None, "csrfmiddlewaretoken": self.csrf,
        }
        if file_id:
            body["file_id"] = int(file_id)
        r = self.sess.post(f"{DRIVE}/api/v5/files/file",
                           data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                           headers={"content-type": "application/json"})
        r.raise_for_status()
        return r.json()

    def upload_blob(self, parent_id: int, name: str, data: bytes,
                    file_id: int = 0, up_new_ver: bool = False) -> dict:
        """上传单个对象(一个分片或小文件)。file_id!=0 + up_new_ver=True 时覆盖更新(同 fileid)。

        返回 commit 响应 dict, 含: id(fileid), fsize, fver, fsha 等。
        """
        md5 = md5_hex(data)
        sha1 = sha1_hex(data)
        plan = self._create_update(parent_id, name, len(data), md5, file_id=file_id)
        store = plan.get("store", "ks3")
        key, _etag = self._put_store(plan, data)
        if not key:
            key = sha1  # saveKey=${SHA1}
        res = self._commit(parent_id, name, key, md5, sha1, len(data), store,
                           file_id=file_id, up_new_ver=up_new_ver)
        if res.get("result") != "ok":
            raise KdocsError(f"commit 失败: {res}")
        return res

    # ---------- 下载 ----------
    def download_url(self, file_id: int) -> str:
        """与扩展名无关的通用下载: 返回对象存储预签名直链。

        用 /api/v3/groups/{gid}/files/{id}/download (任意文件均可),
        而非 /office/file/{id}/download(仅支持可预览的 office 类型)。
        """
        r = self.sess.get(f"{DRIVE}/api/v3/groups/{self.group_id}/files/{file_id}/download")
        r.raise_for_status()
        info = r.json()
        fi = info.get("fileinfo") or {}
        url = fi.get("url") or fi.get("download_url") or info.get("download_url") or info.get("url")
        if not url:
            raise KdocsError(f"未取得下载直链: {info}")
        return url

    def download_blob(self, file_id: int) -> bytes:
        url = self.download_url(file_id)
        r = requests.get(url, headers={"user-agent": _UA})
        r.raise_for_status()
        return r.content

    def rename(self, file_id: int, new_name: str) -> dict:
        """重命名文件/文件夹: PUT /api/v3/groups/{gid}/files/{id} {fname}。"""
        body = {"fname": new_name, "csrfmiddlewaretoken": self.csrf}
        r = self.sess.put(f"{DRIVE}/api/v3/groups/{self.group_id}/files/{file_id}",
                          data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                          headers={"content-type": "application/json"})
        r.raise_for_status()
        return r.json()

    def ping(self) -> bool:
        """简单校验 Cookie/group 是否可用。"""
        self.list_dir(0, offset=0, count=1)
        return True
