# wps-netdisk

把 WPS 云文档（kdocs）「我的云文档」当作对象存储的个人网盘。本地用 SQLite 维护虚拟文件树与分片元数据，文件以**镜像模式**存到云端（云端目录/文件名与网盘一致，可在 kdocs 原生界面浏览）。

## 特性

- **只需 Cookie**：自动发现私有空间 group、自动查找/创建根目录、自动建库。
- **跨设备恢复**：SQLite 元数据库整体分片上传到云端，换电脑只需重新填 Cookie，本地库自动从云端拼回。
- **大文件分片**：单文件超过分片大小时切片为 `name.partNNNN` 存入同名子文件夹，绕过 kdocs 单文件 2GB 上限；下载时按序重组。
- **浏览器端并发分片上传**：进度真实反映云端上传，3 并发吃满上行带宽。
- **Web 界面**：上传 / 下载 / 新建文件夹 / 重命名 / 删除。

## 技术栈

FastAPI（后端 REST + 静态托管） + React（CDN 单文件前端） + SQLite。

## 运行

```bash
cd netdisk/backend
pip install -r requirements.txt

# 准备配置: 复制示例并填入 Cookie
cp data/config.example.json data/config.json
# 编辑 data/config.json, 把 cookie 换成浏览器里 drive.kdocs.cn 的完整 Cookie

python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8799
```

打开 http://127.0.0.1:8799 。也可以先不填 `config.json`，启动后在网页设置里粘贴 Cookie。

## 如何获取 Cookie

浏览器登录 kdocs.cn → F12 → 任意 `drive.kdocs.cn` 请求 → 复制完整 `Cookie` 请求头（需含 `wps_sid`、`kso_sid`、`csrf`）。Cookie 过期后在网页重新粘贴并触发重载即可。

## 云端布局

```
我的云文档/
  WpsNetDisk/                  # 根目录(按名发现)
    _netdisk_manifest.json     # 版本 + DB 分片清单 + sha256
    _netdisk_sys/              # SQLite 库的分片
    <你的文件夹>/ <你的文件>    # 镜像的真实结构
    <大文件>/                  # >分片大小: 同名子文件夹内放 .partNNNN
```

## 说明

个人单写者场景；通过 manifest 版本号做乐观锁。Cookie 仅保存在本地 `data/config.json`（已被 `.gitignore` 排除）。
