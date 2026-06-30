"""分片存储层: 把任意字节流按 chunk_size 切片, 逐片上传到云文档; 下载时按序拼回。

云文档单文件上限 2G, 所以默认分片 512MB。每个分片是云端一个独立文件(fileid)。
"""
import hashlib
import uuid

from .kdocs import KdocsClient, md5_hex, sha1_hex


def upload_stream(kdocs: KdocsClient, folder_id: int, fileobj, chunk_size: int):
    """从 fileobj 读取并分片上传到 folder_id。

    返回 (chunks, total_size, sha256):
      chunks = [{idx, fileid, size, md5, sha1}, ...]
    """
    chunks = []
    total = 0
    hasher = hashlib.sha256()
    base = uuid.uuid4().hex
    idx = 0
    while True:
        data = fileobj.read(chunk_size)
        if not data:
            break
        total += len(data)
        hasher.update(data)
        name = f"{base}.{idx:06d}.part"
        res = kdocs.upload_blob(folder_id, name, data)
        chunks.append({
            "idx": idx,
            "fileid": res["id"],
            "size": len(data),
            "md5": md5_hex(data),
            "sha1": sha1_hex(data),
        })
        idx += 1
    # 空文件: 也产生 0 个分片, 允许
    return chunks, total, hasher.hexdigest()


def upload_bytes(kdocs: KdocsClient, folder_id: int, data: bytes, chunk_size: int):
    """把一段 bytes 分片上传(用于 SQLite DB 文件本身)。返回 (chunks, total, sha256)。"""
    import io
    return upload_stream(kdocs, folder_id, io.BytesIO(data), chunk_size)


def mirror_upload(kdocs: KdocsClient, real_parent_id: int, name: str, fileobj, chunk_size: int):
    """镜像模式上传:

    - 文件 <= 单分片: 直接以真实文件名存到父目录(云端就是一个完整文件, 原生界面可见可下)。
    - 文件 > 单分片: 在父目录下建同名子文件夹, 内部存 name.partNNNN 分片。

    返回 (chunks, total_size, sha256, kdocs_id):
      kdocs_id = 单片时为该文件id; 多片时为子文件夹id。
      chunks = [{idx, fileid, size, md5, sha1}, ...]
    """
    import hashlib
    hasher = hashlib.sha256()

    first = fileobj.read(chunk_size)
    second = fileobj.read(chunk_size)

    # 单分片(含空文件): 直接存为真实文件名
    if not second:
        hasher.update(first)
        res = kdocs.upload_blob(real_parent_id, name, first)
        fid = res["id"]
        chunks = [{"idx": 0, "fileid": fid, "size": len(first),
                   "md5": md5_hex(first), "sha1": sha1_hex(first)}]
        return chunks, len(first), hasher.hexdigest(), fid

    # 多分片: 建同名子文件夹, 内部放 name.partNNNN
    sub_id = int(kdocs.mkdir(real_parent_id, name)["id"])
    chunks = []
    total = 0
    idx = 0
    pending = [first, second]
    while True:
        for data in pending:
            total += len(data)
            hasher.update(data)
            part_name = f"{name}.part{idx + 1:04d}"
            res = kdocs.upload_blob(sub_id, part_name, data)
            chunks.append({"idx": idx, "fileid": res["id"], "size": len(data),
                           "md5": md5_hex(data), "sha1": sha1_hex(data)})
            idx += 1
        nxt = fileobj.read(chunk_size)
        if not nxt:
            break
        pending = [nxt]
    return chunks, total, hasher.hexdigest(), sub_id


def iter_download(kdocs: KdocsClient, chunks: list):
    """按 idx 顺序下载每个分片并 yield 字节流, 供流式下载使用。"""
    for c in sorted(chunks, key=lambda x: x["idx"]):
        yield kdocs.download_blob(int(c["kdocs_fileid"]))


def download_all(kdocs: KdocsClient, chunks: list) -> bytes:
    """下载并拼接为完整 bytes(用于 DB 文件恢复)。"""
    buf = bytearray()
    for part in iter_download(kdocs, chunks):
        buf.extend(part)
    return bytes(buf)
