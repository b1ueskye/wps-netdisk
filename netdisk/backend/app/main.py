"""FastAPI 入口: 配置入口 + 网盘 REST + 托管前端。"""
import os
import urllib.parse

from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Request
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import config as cfgmod
from .kdocs import KdocsClient
from .sync import NetDiskSync, ConflictError
from .service import NetDiskService, ServiceError

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB_DIR = os.path.join(HERE, "web")

app = FastAPI(title="WPS 云文档网盘")

STATE = {"service": None, "error": None}


def _build_service(cfg: dict) -> NetDiskService:
    cookie = cfgmod.normalize_cookie(cfg["cookie"])
    if cookie != cfg.get("cookie"):
        cfgmod.save({"cookie": cookie})  # 回写成干净的 header 串
    csrf = cfgmod.parse_csrf(cookie)
    gid = int(cfg.get("group_id") or 0)
    kd = KdocsClient(cookie, gid, csrf)
    if not gid:
        gid = kd.discover_group_id()       # 仅凭 Cookie 自动发现「我的云文档」group
        cfgmod.save({"group_id": gid})      # 持久化, 下次直接用
    kd.ping()  # 校验 Cookie / group 可用
    sync = NetDiskSync(kd, cfgmod.DB_PATH, cfg["root_name"], cfgmod.chunk_size_bytes(cfg))
    sync.bootstrap()
    return NetDiskService(sync)


def _try_autoinit():
    cfg = cfgmod.load()
    if cfgmod.is_configured(cfg):
        try:
            STATE["service"] = _build_service(cfg)
            STATE["error"] = None
        except Exception as e:  # noqa
            STATE["error"] = str(e)


@app.on_event("startup")
def _startup():
    _try_autoinit()


def _svc() -> NetDiskService:
    if STATE["service"] is None:
        raise HTTPException(status_code=409, detail="未初始化, 请先在设置里填入云文档 Cookie")
    return STATE["service"]


# ---------------- 配置 ----------------
@app.get("/api/status")
def status():
    cfg = cfgmod.load()
    svc = STATE["service"]
    return {
        "configured": cfgmod.is_configured(cfg),
        "initialized": svc is not None,
        "error": STATE["error"],
        "group_id": cfg.get("group_id"),
        "root_name": cfg.get("root_name"),
        "chunk_size_mb": cfg.get("chunk_size_mb"),
        "has_cookie": bool(cfg.get("cookie")),
        "version": svc.sync.version if svc else None,
    }


@app.post("/api/config")
def set_config(body: dict = Body(...)):
    patch = {}
    for k in ("cookie", "group_id", "root_name", "chunk_size_mb"):
        if k in body and body[k] not in (None, ""):
            patch[k] = body[k]
    if "group_id" in patch:
        patch["group_id"] = int(patch["group_id"])
    if "chunk_size_mb" in patch:
        patch["chunk_size_mb"] = int(patch["chunk_size_mb"])
    cfg = cfgmod.save(patch)
    if not cfgmod.is_configured(cfg):
        return {"ok": True, "initialized": False, "msg": "已保存, 但 cookie/group_id 仍不完整"}
    try:
        STATE["service"] = _build_service(cfg)
        STATE["error"] = None
    except Exception as e:  # noqa
        STATE["service"] = None
        STATE["error"] = str(e)
        raise HTTPException(status_code=400, detail=f"初始化失败: {e}")
    return {"ok": True, "initialized": True, "version": STATE["service"].sync.version}


@app.post("/api/reload")
def reload_from_cloud():
    """从云端重新拉取最新 DB(用于另一设备写过之后同步)。"""
    cfg = cfgmod.load()
    if not cfgmod.is_configured(cfg):
        raise HTTPException(status_code=409, detail="未配置")
    try:
        STATE["service"] = _build_service(cfg)
        STATE["error"] = None
    except Exception as e:  # noqa
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, "version": STATE["service"].sync.version}


# ---------------- 文件操作 ----------------
@app.get("/api/files")
def list_files(parent_id: int = 0):
    try:
        return _svc().list_dir(parent_id)
    except ServiceError as e:
        raise HTTPException(400, str(e))


@app.post("/api/folder")
def make_folder(body: dict = Body(...)):
    parent_id = int(body.get("parent_id", 0))
    name = body.get("name", "")
    try:
        return _svc().mkdir(parent_id, name)
    except ConflictError as e:
        raise HTTPException(409, str(e))
    except ServiceError as e:
        raise HTTPException(400, str(e))


@app.post("/api/upload")
async def upload(parent_id: int = 0, file: UploadFile = File(...)):
    svc = _svc()
    name = file.filename or "未命名"
    try:
        node = svc.upload_file(parent_id, name, file.file)
        return node
    except ConflictError as e:
        raise HTTPException(409, str(e))
    except ServiceError as e:
        raise HTTPException(400, str(e))


@app.post("/api/upload/init")
def upload_init(body: dict = Body(...)):
    svc = _svc()
    try:
        return svc.upload_init(int(body.get("parent_id", 0)),
                               body.get("name", ""), int(body.get("size", 0)))
    except ConflictError as e:
        raise HTTPException(409, str(e))
    except ServiceError as e:
        raise HTTPException(400, str(e))


@app.post("/api/upload/part")
async def upload_part(upload_id: str, idx: int, request: Request):
    svc = _svc()
    data = await request.body()
    try:
        return svc.upload_part(upload_id, idx, data)
    except ServiceError as e:
        raise HTTPException(400, str(e))


@app.post("/api/upload/complete")
def upload_complete(upload_id: str):
    svc = _svc()
    try:
        return svc.upload_complete(upload_id)
    except ConflictError as e:
        raise HTTPException(409, str(e))
    except ServiceError as e:
        raise HTTPException(400, str(e))


@app.post("/api/upload/abort")
def upload_abort(upload_id: str):
    return {"ok": _svc().upload_abort(upload_id)}


@app.get("/api/download/{node_id}")
def download(node_id: int):
    svc = _svc()
    try:
        node, gen = svc.open_download(node_id)
    except ServiceError as e:
        raise HTTPException(400, str(e))
    fname = node["name"]
    quoted = urllib.parse.quote(fname)
    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{quoted}",
        "Content-Length": str(node["size"]),
    }
    return StreamingResponse(gen, media_type="application/octet-stream", headers=headers)


@app.post("/api/rename")
def rename(body: dict = Body(...)):
    node_id = int(body["node_id"])
    name = body.get("name", "")
    try:
        return _svc().rename(node_id, name)
    except ConflictError as e:
        raise HTTPException(409, str(e))
    except ServiceError as e:
        raise HTTPException(400, str(e))


@app.delete("/api/node/{node_id}")
def delete_node(node_id: int):
    try:
        _svc().delete(node_id)
        return {"ok": True}
    except ConflictError as e:
        raise HTTPException(409, str(e))
    except ServiceError as e:
        raise HTTPException(400, str(e))


# ---------------- 前端 ----------------
@app.get("/")
def index():
    return FileResponse(os.path.join(WEB_DIR, "index.html"))


if os.path.isdir(WEB_DIR):
    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
