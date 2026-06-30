"""分片存储层: 把任意字节流按 chunk_size 切片, 逐片上传到云文档; 下载时按序拼回。

云文档单文件上限 2G, 所以默认分片 512MB。每个分片是云端一个独立文件(fileid)。
"""
import hashlib
import time
import queue
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from .applog import logger
from .kdocs import KdocsClient, md5_hex, sha1_hex

# 并发下载分片数。与上传保持同一量级(5 路), 再多受云端限速/连接数制约收益递减。
DOWNLOAD_CONCURRENCY = 3
# 每个分片在内存中预取的块数(背压), 防止某个分片跑太快把内存撑爆。
DOWNLOAD_QUEUE_SIZE = 8


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


def iter_download(kdocs: KdocsClient, chunks: list, concurrency: int = DOWNLOAD_CONCURRENCY):
    """并发下载各分片, 按 idx 顺序逐块 yield 字节流, 供流式下载使用。

    多个分片在线程池中并发拉取, 但输出严格按 idx 顺序, 保证客户端拿到完整文件。
    每个分片用有界队列做背压, 避免顺序靠后的分片把内存撑爆。
    单分片下载失败会重新取直链并从断点续传重试, 应对预签名链接过期/连接中断。
    数据一到即下发, 浏览器立即开始下载进度。
    """
    ordered = sorted(chunks, key=lambda x: x["idx"])
    if not ordered:
        return
    if len(ordered) == 1:
        yield from _download_chunk_resilient(kdocs, ordered[0])
        return

    SENTINEL = object()
    queues = [queue.Queue(maxsize=DOWNLOAD_QUEUE_SIZE) for _ in ordered]

    def worker(i, c):
        q = queues[i]
        try:
            for block in _download_chunk_resilient(kdocs, c):
                q.put(block)
        except Exception as e:
            logger.error("DOWNLOAD worker idx=%d fileid=%s FAIL: %s", i, c["kdocs_fileid"], e)
            q.put(("__error__", e))
        finally:
            q.put(SENTINEL)

    with ThreadPoolExecutor(max_workers=min(concurrency, len(ordered))) as ex:
        for i, c in enumerate(ordered):
            ex.submit(worker, i, c)
        for q in queues:
            while True:
                item = q.get()
                if item is SENTINEL:
                    break
                if isinstance(item, tuple) and len(item) == 2 and item[0] == "__error__":
                    raise item[1]
                yield item


def _download_chunk_resilient(kdocs: KdocsClient, chunk: dict, max_retries: int = 4):
    """下载单个分片, 失败后从断点续传重试。

    预签名直链可能过期、连接可能被限速饿死, 这里在异常时重新获取直链并从
    已接收字节偏移处用 Range 续传, 避免整个分片从头重来。
    """
    fileid = int(chunk["kdocs_fileid"])
    expected = chunk.get("size") or 0
    offset = 0
    for attempt in range(1, max_retries + 1):
        try:
            for block in kdocs.iter_download_blob(fileid, start_offset=offset):
                offset += len(block)
                yield block
            # 校验: 若 DB 记录了 size 且不匹配, 视为失败重试
            if expected and offset < expected:
                raise IOError(f"分片不完整: 已收 {offset} / 预期 {expected}")
            return
        except Exception as e:
            logger.warning("DOWNLOAD chunk idx=%s fileid=%s attempt=%d/%d offset=%d: %s",
                           chunk.get("idx"), fileid, attempt, max_retries, offset, e)
            if attempt >= max_retries:
                raise
            time.sleep(min(2 ** attempt, 8))


def download_all(kdocs: KdocsClient, chunks: list) -> bytes:
    """下载并拼接为完整 bytes(用于 DB 文件恢复)。"""
    buf = bytearray()
    for part in iter_download(kdocs, chunks):
        buf.extend(part)
    return bytes(buf)
