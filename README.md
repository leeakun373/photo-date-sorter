# Nikon Photo Importer

一个用于导入相机卡并按日期整理照片/视频的桌面工具（PySide6）。

## 功能

- 一键导卡并整理（自动识别相机卡卷标和媒体目录）
- 整理历史照片/视频
- 按日期创建目录（支持多种日期格式）
- 照片优先读取 EXIF 时间，失败回退文件时间
- 视频使用文件时间
- 一键模式配置可持久化（换电脑可重新配置）

## 环境要求

- Windows 10/11
- Python 3.10+（推荐 3.10/3.11）
- pip

## 本地开发配置

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

运行：

```bash
python photo_date_sorter.py
```

## 打包 EXE（目录版，推荐）

> 目录版不会使用 onefile 临时解包目录，运行更稳定。

```bash
pyinstaller --noconfirm --clean --onedir --windowed ^
  --name "NikonPhotoImporter" ^
  --icon "camera.ico" ^
  --add-data "camera.ico;." ^
  "photo_date_sorter.py"
```

产物路径：

- `dist\NikonPhotoImporter\NikonPhotoImporter.exe`

## 打包 EXE（单文件版，可选）

```bash
pyinstaller --noconfirm --clean --onefile --windowed ^
  --name "NikonPhotoImporter" ^
  --icon "camera.ico" ^
  --add-data "camera.ico;." ^
  "photo_date_sorter.py"
```

## 一键模式配置说明

首次运行建议在界面中填写并保存：

- 默认照片库目录（例如 `E:\Photos\NIKON Z F`）
- 相机卡卷标（例如 `NIKON Z F`）
- 相机媒体子目录（例如 `DCIM\101NCZ_F`）

配置会写入用户目录配置文件，换电脑时重新设置一次即可。

## 常见问题

- EXE 图标不刷新：多为 Windows 图标缓存问题，重命名 EXE 或重启资源管理器后可见。
- 扫描慢：RAW/HEIC 的 EXIF 解析耗时较长，属于正常现象，程序会显示阶段进度。
