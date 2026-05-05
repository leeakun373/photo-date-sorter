# -*- coding: utf-8 -*-
"""
按拍摄日期整理照片：优先 EXIF（DateTimeOriginal / DateTime），否则使用文件修改时间。
在目标根目录下按所选格式创建日期子文件夹并移动或复制文件。
"""

from __future__ import annotations

import csv
import logging
import os
import re
import shutil
import sys
import json
import threading
from functools import partial
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from ctypes import byref, create_unicode_buffer, windll
from ctypes.wintypes import DWORD
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Callable, Iterator

from PIL import Image
try:
    import exifread
except Exception:  # pragma: no cover - 打包/环境缺失时回退 Pillow
    exifread = None

from PySide6.QtCore import QObject, QThread, Qt, QTimer, QUrl, Signal, Slot
from PySide6.QtGui import QDesktopServices, QFont, QFontDatabase, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPlainTextEdit,
    QPushButton,
    QRadioButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

LOG = logging.getLogger("photo_date_sorter")
DEFAULT_LIBRARY_ROOT = Path(r"E:\Photos\NIKON Z F")
CAMERA_VOLUME_LABEL = "NIKON Z F"
CAMERA_RELATIVE_MEDIA_DIR = Path("DCIM") / "101NCZ_F"


def resolve_asset_path(name: str) -> Path:
    """兼容源码/打包环境的资源定位。"""
    if getattr(sys, "frozen", False):
        mei = getattr(sys, "_MEIPASS", None)
        if mei:
            p = Path(mei) / name
            if p.exists():
                return p
        exe_dir = Path(sys.executable).resolve().parent
        p = exe_dir / name
        if p.exists():
            return p
    return Path(__file__).resolve().parent / name


def _config_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", str(Path.home())))
    else:
        base = Path.home() / ".config"
    return base / "PhotoDateSorter" / "config.json"


def load_app_config() -> dict[str, str]:
    cfg = {
        "library_root": str(DEFAULT_LIBRARY_ROOT),
        "camera_volume_label": CAMERA_VOLUME_LABEL,
        "camera_media_subdir": str(CAMERA_RELATIVE_MEDIA_DIR).replace("/", "\\"),
    }
    p = _config_path()
    try:
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                cfg.update({k: str(v) for k, v in data.items() if k in cfg and v})
    except Exception:
        LOG.exception("读取配置失败，将使用默认配置。")
    return cfg


def save_app_config(cfg: dict[str, str]) -> None:
    p = _config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


class GuiLogEmitter(QObject):
    """供 logging.Handler 发射到主线程 UI（QueuedConnection）。"""

    append = Signal(str)


class GuiLogHandler(logging.Handler):
    def __init__(self, emitter: GuiLogEmitter) -> None:
        super().__init__()
        self._emitter = emitter

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self._emitter.append.emit(msg)
        except Exception:
            self.handleError(record)


def _log_dir() -> Path:
    # onefile exe 运行时 __file__ 位于临时目录，改为写到 exe 同级目录，便于排查问题
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / "logs"
    return Path(__file__).resolve().parent / "logs"


def configure_logging(gui_emitter: GuiLogEmitter | None = None) -> Path:
    """配置文件 + 控制台日志；可选 GUI Handler。返回日志文件路径。"""
    if LOG.handlers:
        return _log_dir() / "photo_date_sorter.log"

    LOG.setLevel(logging.DEBUG)
    LOG.propagate = False
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    brief = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

    log_dir = _log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "photo_date_sorter.log"
    fh = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    LOG.addHandler(fh)

    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.INFO)
    sh.setFormatter(brief)
    LOG.addHandler(sh)

    if gui_emitter is not None:
        gh = GuiLogHandler(gui_emitter)
        gh.setLevel(logging.INFO)
        gh.setFormatter(brief)
        LOG.addHandler(gh)

    return log_path


IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".tif",
    ".tiff",
    ".webp",
    ".bmp",
    ".gif",
    ".heic",
    ".heif",
    ".jfif",
    ".dng",
    ".cr2",
    ".nef",
    ".arw",
    ".orf",
    ".rw2",
}

VIDEO_EXTENSIONS = {
    ".mp4",
    ".mov",
    ".avi",
    ".mkv",
    ".mts",
    ".m2ts",
    ".mpg",
    ".mpeg",
    ".wmv",
    ".3gp",
    ".webm",
}

MEDIA_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS


def _parse_exif_datetime(s: str) -> datetime | None:
    s = s.strip()
    for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def exif_date_taken(path: Path) -> datetime | None:
    """优先用 exifread 快速提取拍摄时间，失败再回退 Pillow。"""
    if path.suffix.lower() not in IMAGE_EXTENSIONS:
        return None
    if exifread is not None:
        try:
            with path.open("rb") as file_handle:
                tags = exifread.process_file(
                    file_handle,
                    details=False,
                    extract_thumbnail=False,
                    stop_tag="DateTimeOriginal",
                )
                for key in ("EXIF DateTimeOriginal", "Image DateTime", "EXIF DateTimeDigitized"):
                    raw = tags.get(key)
                    if raw:
                        dt = _parse_exif_datetime(str(raw))
                        if dt:
                            return dt
        except Exception:
            pass
    # 回退到 Pillow，兼容部分 exifread 无法处理的文件
    try:
        with Image.open(path) as img:
            exif = img.getexif()
            if not exif:
                return None
            # DateTimeOriginal, DateTime, 兼容部分厂商
            for tag in (0x9003, 0x0132, 0x9004):
                raw = exif.get(tag)
                if raw:
                    dt = _parse_exif_datetime(str(raw))
                    if dt:
                        return dt
    except Exception:
        return None
    return None


def file_mtime_date(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime)


@dataclass
class PlannedFile:
    source: Path
    taken_at: datetime
    date_source: str  # "EXIF" | "文件时间"
    dest_folder: Path
    dest_path: Path


def iter_images(root: Path, recursive: bool) -> Iterator[Path]:
    # 使用 os.scandir / os.walk，通常比 Path.rglob 更快
    if recursive:
        for dirpath, _, filenames in os.walk(root):
            base = Path(dirpath)
            for name in filenames:
                p = base / name
                if p.suffix.lower() in MEDIA_EXTENSIONS:
                    yield p
    else:
        with os.scandir(root) as it:
            for entry in it:
                if not entry.is_file():
                    continue
                p = Path(entry.path)
                if p.suffix.lower() in MEDIA_EXTENSIONS:
                    yield p


def list_images(
    root: Path,
    recursive: bool,
    on_progress: Callable[[str, int, int, str], None] | None = None,
) -> list[Path]:
    files: list[Path] = []
    scanned = 0
    if recursive:
        for dirpath, _, filenames in os.walk(root):
            base = Path(dirpath)
            for name in filenames:
                scanned += 1
                p = base / name
                if p.suffix.lower() in MEDIA_EXTENSIONS:
                    files.append(p)
                if on_progress is not None and (scanned <= 10 or scanned % 200 == 0):
                    on_progress("枚举文件", scanned, 0, p.name)
    else:
        with os.scandir(root) as it:
            for entry in it:
                if not entry.is_file():
                    continue
                scanned += 1
                p = Path(entry.path)
                if p.suffix.lower() in MEDIA_EXTENSIONS:
                    files.append(p)
                if on_progress is not None and (scanned <= 10 or scanned % 200 == 0):
                    on_progress("枚举文件", scanned, 0, p.name)
    return sorted(files, key=lambda p: p.name.lower())


def date_folder_name(dt: datetime, pattern: str) -> str:
    if pattern == "YYYY-MM-DD":
        return dt.strftime("%Y-%m-%d")
    if pattern == "YYYY/MM/DD":
        return dt.strftime("%Y") + os.sep + dt.strftime("%m") + os.sep + dt.strftime("%d")
    if pattern == "YYYY年MM月DD日":
        return dt.strftime("%Y年%m月%d日")
    return dt.strftime("%Y-%m-%d")


def unique_dest(dest: Path) -> Path:
    if not dest.exists():
        return dest
    stem, suf = dest.stem, dest.suffix
    parent = dest.parent
    for i in range(1, 10_000):
        cand = parent / f"{stem}_{i}{suf}"
        if not cand.exists():
            return cand
    raise RuntimeError("无法生成唯一文件名: " + str(dest))


# 与 FAT32/exFAT 时间戳精度兼容：已存在目标与源 size 相同且 mtime 足够接近则视为同一文件
MTIME_DEDUP_TOLERANCE_SEC = 2.0

# 匹配「原名_数字.扩展名」形式的旧版 unique_dest 副本（如 _DSC6997_1.JPG）
DUP_NUMERIC_SUFFIX_RE = re.compile(r"^(.+)_(\d+)(\.[^.]+)$")

# 仅将 _N 视为 unique_dest 生成的副本序号：N 必须 ≤ 此上限。
# 相机常见「前缀_四位机身号」如 DSC_6997.JPG 会被正则拆成「原文件 DSC.jpg + 序号 6997」，
# 若不做上限会把大量正常照片误判为候选；真实重复导入产生的副本多为 _1、_2…，序号通常远小于四位数。
DUP_CLEANUP_INDEX_MAX = 999

CLEANUP_REPORT_CSV_NAME = "cleanup_report.csv"


@dataclass(frozen=True)
class RedundantDupEntry:
    candidate: Path
    original: Path
    size_bytes: int
    mtime_diff_sec: float


def app_run_directory() -> Path:
    """程序运行目录：打包后为 exe 所在目录，源码运行为脚本所在目录。"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def cleanup_report_csv_path() -> Path:
    return app_run_directory() / CLEANUP_REPORT_CSV_NAME


def collect_redundant_numeric_suffix_duplicates(root: Path) -> tuple[list[RedundantDupEntry], int]:
    """
    深度遍历 root，找出可安全删除的 _N 副本：同目录下存在「去掉 _N」的原文件，
    且 size 相同、mtime 差在 MTIME_DEDUP_TOLERANCE_SEC 内。
    仅当序号 N 满足 1 <= N <= DUP_CLEANUP_INDEX_MAX 时才参与（排除机身四位序号等误匹配）。
    返回 (条目列表, 冗余文件总字节数)。
    """
    found: list[RedundantDupEntry] = []
    total_bytes = 0
    skipped_index_cap = 0
    if not root.is_dir():
        return found, total_bytes
    for dirpath, _, filenames in os.walk(root):
        base = Path(dirpath)
        for name in filenames:
            m = DUP_NUMERIC_SUFFIX_RE.match(name)
            if not m:
                continue
            dup_index = int(m.group(2))
            if dup_index < 1 or dup_index > DUP_CLEANUP_INDEX_MAX:
                skipped_index_cap += 1
                continue
            base_stem = m.group(1)
            ext = m.group(3)
            original_name = base_stem + ext
            candidate = base / name
            original = base / original_name
            if not original.is_file():
                continue
            try:
                if candidate.resolve() == original.resolve():
                    continue
            except OSError:
                continue
            try:
                c_st = candidate.stat()
                o_st = original.stat()
            except OSError:
                continue
            if c_st.st_size != o_st.st_size:
                continue
            mtime_diff = abs(c_st.st_mtime - o_st.st_mtime)
            if mtime_diff > MTIME_DEDUP_TOLERANCE_SEC:
                continue
            found.append(
                RedundantDupEntry(
                    candidate=candidate,
                    original=original,
                    size_bytes=c_st.st_size,
                    mtime_diff_sec=mtime_diff,
                )
            )
            total_bytes += c_st.st_size
    if skipped_index_cap:
        LOG.info(
            "冗余清理扫描：因「_序号 > %s」跳过 %s 个文件名（多为机身序号如 DSC_6997.JPG，非 unique_dest 副本）。",
            DUP_CLEANUP_INDEX_MAX,
            skipped_index_cap,
        )
    return found, total_bytes


def find_camera_media_dir(
    target_volume_label: str = CAMERA_VOLUME_LABEL,
    relative_media_dir: Path = CAMERA_RELATIVE_MEDIA_DIR,
) -> Path | None:
    """在 Windows 所有盘符中按卷标定位相机卡，并返回照片目录。"""
    if os.name != "nt":
        return None
    try:
        drive_mask = windll.kernel32.GetLogicalDrives()
        for i in range(26):
            if not (drive_mask & (1 << i)):
                continue
            drive = f"{chr(ord('A') + i)}:\\"
            vol_name = create_unicode_buffer(261)
            fs_name = create_unicode_buffer(261)
            serial = DWORD()
            max_comp_len = DWORD()
            flags = DWORD()
            ok = windll.kernel32.GetVolumeInformationW(
                drive,
                vol_name,
                261,
                byref(serial),
                byref(max_comp_len),
                byref(flags),
                fs_name,
                261,
            )
            if not ok:
                continue
            if vol_name.value.strip().lower() != target_volume_label.strip().lower():
                continue
            media_dir = Path(drive) / relative_media_dir
            if media_dir.is_dir():
                return media_dir
    except Exception:
        LOG.exception("自动识别相机卡目录失败")
    return None


def plan_sort(
    root: Path,
    out_root: Path,
    recursive: bool,
    prefer_exif: bool,
    folder_pattern: str,
    on_progress: Callable[[str, int, int, str], None] | None = None,
) -> list[PlannedFile]:
    paths = list_images(root, recursive, on_progress=on_progress)
    total = len(paths)
    planned: list[PlannedFile] = []
    if total == 0:
        return planned

    def resolve_date(src: Path) -> tuple[datetime, str]:
        if prefer_exif:
            dt = exif_date_taken(src)
            if dt:
                return dt, "EXIF"
        return file_mtime_date(src), "文件时间"

    # EXIF 读取并发化（I/O + 解码），并按完成顺序更新进度，避免“前几个慢文件卡住”
    workers = min(8, (os.cpu_count() or 4))
    date_results: dict[Path, tuple[datetime, str]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures: dict[Future[tuple[datetime, str]], Path] = {
            executor.submit(resolve_date, src): src for src in paths
        }
        for idx, future in enumerate(as_completed(futures), start=1):
            src = futures[future]
            date_results[src] = future.result()
            if on_progress is not None:
                on_progress("读取时间", idx, total, src.name)

    skipped_dedup = 0
    for idx, src in enumerate(paths, start=1):
        dt, src_label = date_results[src]
        if on_progress is not None:
            on_progress("生成计划", idx, total, src.name)
        rel = date_folder_name(dt, folder_pattern)
        dest_dir = out_root / rel
        dest_path = dest_dir / src.name
        try:
            src_st = src.stat()
        except OSError:
            LOG.warning("无法读取源文件信息，跳过计划项：%s", src)
            continue

        if dest_path.exists():
            try:
                dest_st = dest_path.stat()
            except OSError:
                dest_st = None
            if dest_st is not None:
                if src_st.st_size == dest_st.st_size and abs(
                    src_st.st_mtime - dest_st.st_mtime
                ) <= MTIME_DEDUP_TOLERANCE_SEC:
                    skipped_dedup += 1
                    continue
            dest_path = unique_dest(dest_path)

        planned.append(
            PlannedFile(
                source=src,
                taken_at=dt,
                date_source=src_label,
                dest_folder=dest_path.parent,
                dest_path=dest_path,
            )
        )
    if skipped_dedup:
        LOG.info(
            "极快去重：跳过已导入文件 %s 个（size + mtime 在 %.1fs 内匹配）。",
            skipped_dedup,
            MTIME_DEDUP_TOLERANCE_SEC,
        )
    return planned


class ScanWorker(QObject):
    finished = Signal(list)
    failed = Signal(str)
    progress = Signal(str, int, int, str)

    def __init__(
        self,
        root: str,
        out_root: str,
        recursive: bool,
        prefer_exif: bool,
        folder_pattern: str,
    ) -> None:
        super().__init__()
        self._root = root
        self._out_root = out_root
        self._recursive = recursive
        self._prefer_exif = prefer_exif
        self._folder_pattern = folder_pattern

    @Slot()
    def run(self) -> None:
        try:
            r = Path(self._root)
            o = Path(self._out_root)
            LOG.info(
                "开始扫描：源=%s 输出根=%s 子文件夹=%s EXIF优先=%s 格式=%s",
                r,
                o,
                self._recursive,
                self._prefer_exif,
                self._folder_pattern,
            )
            if not r.is_dir():
                LOG.error("源目录无效：%s", r)
                self.failed.emit("源文件夹不存在或不是目录。")
                return
            o.mkdir(parents=True, exist_ok=True)

            def on_prog(stage: str, i: int, n: int, name: str) -> None:
                self.progress.emit(stage, i, n, name)

            items = plan_sort(
                r,
                o,
                self._recursive,
                self._prefer_exif,
                self._folder_pattern,
                on_progress=on_prog,
            )
            LOG.info("扫描完成，共 %s 个媒体文件。", len(items))
            self.finished.emit(items)
        except Exception as e:
            LOG.exception("扫描异常")
            self.failed.emit(str(e))


class RunWorker(QObject):
    progress = Signal(int, int, str, str)
    finished_ok = Signal(int, int, bool)  # ok, err, was_cancelled
    failed = Signal(str)

    def __init__(self, items: list[PlannedFile], do_move: bool) -> None:
        super().__init__()
        self._items = items
        self._do_move = do_move
        self._cancel_event = threading.Event()

    @Slot()
    def cancel(self) -> None:
        self._cancel_event.set()
        LOG.info("收到取消指令，正在停止…")

    @Slot()
    def run(self) -> None:
        total = len(self._items)
        ok = 0
        err = 0
        mode = "移动" if self._do_move else "复制"
        cancelled = False
        LOG.info("开始执行整理：模式=%s，共 %s 个文件。", mode, total)
        try:
            for i, pf in enumerate(self._items, start=1):
                if self._cancel_event.is_set():
                    cancelled = True
                    LOG.info("任务已被用户取消（已完成 %s / %s 个文件的处理循环）。", i - 1, total)
                    break
                self.progress.emit(i, total, pf.source.name, str(pf.dest_path))
                try:
                    pf.dest_folder.mkdir(parents=True, exist_ok=True)
                    if self._do_move:
                        shutil.move(str(pf.source), str(pf.dest_path))
                        LOG.debug("移动 OK：%s -> %s", pf.source, pf.dest_path)
                    else:
                        shutil.copy2(str(pf.source), str(pf.dest_path))
                        LOG.debug("复制 OK：%s -> %s", pf.source, pf.dest_path)
                    ok += 1
                except Exception:
                    LOG.exception("处理失败（%s）：%s", mode, pf.source)
                    err += 1
            if cancelled:
                LOG.info("整理中止：用户取消；成功 %s，失败 %s。", ok, err)
            else:
                LOG.info("整理结束：成功 %s，失败 %s。", ok, err)
            self.finished_ok.emit(ok, err, cancelled)
        except Exception as e:
            LOG.exception("整理过程异常中止")
            self.failed.emit(str(e))


class RedundantDupScanWorker(QObject):
    """在后台线程扫描图库中的安全可删 _N 副本。"""

    # 使用 JSON 字符串跨线程传递路径列表，避免 Signal(object/list, …) 在 PySide6 下偶发崩溃
    finished = Signal(str, int)  # json 数组字符串, total_bytes
    failed = Signal(str)

    def __init__(self, root: str) -> None:
        super().__init__()
        self._root = root

    @Slot()
    def run(self) -> None:
        try:
            r = Path(self._root)
            if not r.is_dir():
                self.failed.emit("输出目录不存在或不是文件夹。")
                return
            entries, total = collect_redundant_numeric_suffix_duplicates(r)
            payload = json.dumps(
                [
                    {
                        "candidate": str(e.candidate),
                        "original": str(e.original),
                        "size_bytes": e.size_bytes,
                        "mtime_diff_sec": round(e.mtime_diff_sec, 6),
                    }
                    for e in entries
                ],
                ensure_ascii=False,
            )
            self.finished.emit(payload, total)
        except Exception as e:
            LOG.exception("扫描冗余副本失败")
            self.failed.emit(str(e))


class RedundantDupDeleteWorker(QObject):
    """在后台线程删除已确认的冗余副本。"""

    finished = Signal(int, int)  # ok_count, err_count
    failed = Signal(str)

    def __init__(self, paths: list[str]) -> None:
        super().__init__()
        self._paths = paths

    @Slot()
    def run(self) -> None:
        ok = 0
        err = 0
        try:
            for p in self._paths:
                try:
                    Path(p).unlink()
                    ok += 1
                    LOG.info("已删除冗余副本：%s", p)
                except Exception:
                    err += 1
                    LOG.exception("删除失败：%s", p)
            LOG.info("冗余副本清理结束：成功删除 %s，失败 %s。", ok, err)
            self.finished.emit(ok, err)
        except Exception as e:
            LOG.exception("清理过程异常")
            self.failed.emit(str(e))


class MainWindow(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("按日期整理照片")
        icon_path = resolve_asset_path("camera.ico")
        if icon_path.exists():
            self.setWindowIcon(QIcon(str(icon_path)))
        self.resize(1000, 680)
        self._plan: list[PlannedFile] = []
        self._scan_thread: QThread | None = None
        self._scan_worker: ScanWorker | None = None
        self._run_thread: QThread | None = None
        self._run_worker: RunWorker | None = None
        self._dup_scan_thread: QThread | None = None
        self._dup_scan_worker: RedundantDupScanWorker | None = None
        self._dup_delete_thread: QThread | None = None
        self._dup_delete_worker: RedundantDupDeleteWorker | None = None
        self._run_mode_cn = "复制"
        self._auto_run_after_scan = False
        self._skip_preview_table = False
        self._cfg = load_app_config()
        self._log_emitter = GuiLogEmitter(self)
        self._log_path = configure_logging(self._log_emitter)
        self._log_emitter.append.connect(self._append_log, Qt.QueuedConnection)

        self._src = QLineEdit()
        self._src.setPlaceholderText("选择包含照片/视频的文件夹，例如 …\\101NCZ_F")
        btn_src = QPushButton("浏览…")
        btn_src.clicked.connect(self._pick_src)

        src_row = QHBoxLayout()
        src_row.addWidget(self._src, 1)
        src_row.addWidget(btn_src)

        self._out = QLineEdit()
        self._out.setPlaceholderText("整理后的根目录（将在此下创建按日期的子文件夹）")
        self._out.setText(self._cfg["library_root"])
        btn_open_out = QPushButton("打开目标文件夹")
        btn_open_out.setToolTip("在资源管理器中打开当前输出根目录")
        btn_open_out.clicked.connect(self._open_output_folder)
        btn_out = QPushButton("浏览…")
        btn_out.clicked.connect(self._pick_out)
        out_row = QHBoxLayout()
        out_row.addWidget(self._out, 1)
        out_row.addWidget(btn_open_out)
        out_row.addWidget(btn_out)

        self._recursive = QCheckBox("包含子文件夹中的照片/视频")
        self._recursive.setChecked(False)

        self._prefer_exif = QCheckBox("优先使用 EXIF 拍摄时间（否则用文件修改时间）")
        self._prefer_exif.setChecked(True)

        self._pattern = QComboBox()
        self._pattern.addItem("YYYY-MM-DD（例 2024-05-01）", "YYYY-MM-DD")
        self._pattern.addItem("YYYY/MM/DD（按年/月/日分层）", "YYYY/MM/DD")
        self._pattern.addItem("YYYY年MM月DD日", "YYYY年MM月DD日")

        self._same_as_src = QCheckBox("输出目录与源目录相同（在源目录下创建日期文件夹）")
        self._same_as_src.setChecked(False)
        self._same_as_src.toggled.connect(self._on_same_toggled)
        self._src.textChanged.connect(self._sync_out_with_src)

        opt = QGroupBox("选项")
        fl = QFormLayout(opt)
        fl.addRow(self._recursive)
        fl.addRow(self._prefer_exif)
        fl.addRow("日期文件夹格式:", self._pattern)
        fl.addRow(self._same_as_src)

        mode = QGroupBox("执行方式")
        ml = QHBoxLayout(mode)
        self._mode_move = QRadioButton("移动（整理后源位置不再保留）")
        self._mode_copy = QRadioButton("复制（保留原文件）")
        self._mode_copy.setChecked(True)
        ml.addWidget(self._mode_move)
        ml.addWidget(self._mode_copy)

        self._btn_scan = QPushButton("扫描并预览")
        self._btn_scan.clicked.connect(self._start_scan)
        self._btn_run = QPushButton("执行整理")
        self._btn_run.setEnabled(False)
        self._btn_run.clicked.connect(self._start_run)
        self._btn_cancel_run = QPushButton("取消执行")
        self._btn_cancel_run.setEnabled(False)
        self._btn_cancel_run.setToolTip("在复制/移动过程中安全停止后续文件（已完成的保留）")
        self._btn_cancel_run.clicked.connect(self._on_cancel_run)
        self._btn_clean_duplicates = QPushButton("清理旧重复文件")
        self._btn_clean_duplicates.setToolTip(
            "深度扫描输出目录：仅删除与同目录原文件大小、mtime 一致的 _1、_2 等旧版副本"
        )
        self._btn_clean_duplicates.clicked.connect(self._start_redundant_dup_scan)
        self._btn_import_card = QPushButton("一键导卡并整理")
        self._btn_import_card.clicked.connect(self._one_click_import)
        self._btn_history = QPushButton("整理历史照片")
        self._btn_history.clicked.connect(self._organize_history)

        quick_cfg = QGroupBox("一键模式设置（换电脑/路径时改这里）")
        self._cfg_library_root = QLineEdit(self._cfg["library_root"])
        btn_cfg_library = QPushButton("浏览…")
        btn_cfg_library.clicked.connect(self._pick_cfg_library_root)
        cfg_lib_row = QHBoxLayout()
        cfg_lib_row.addWidget(self._cfg_library_root, 1)
        cfg_lib_row.addWidget(btn_cfg_library)
        cfg_lib_wrap = QWidget()
        cfg_lib_wrap.setLayout(cfg_lib_row)
        self._cfg_camera_label = QLineEdit(self._cfg["camera_volume_label"])
        self._cfg_camera_subdir = QLineEdit(self._cfg["camera_media_subdir"])
        btn_save_cfg = QPushButton("保存一键设置")
        btn_save_cfg.clicked.connect(self._save_quick_config)
        qf = QFormLayout(quick_cfg)
        qf.addRow("默认照片库目录:", cfg_lib_wrap)
        qf.addRow("相机卡卷标:", self._cfg_camera_label)
        qf.addRow("相机媒体子目录:", self._cfg_camera_subdir)
        qf.addRow(btn_save_cfg)

        bar_row = QHBoxLayout()
        bar_row.addWidget(self._btn_import_card)
        bar_row.addWidget(self._btn_history)
        bar_row.addWidget(self._btn_scan)
        bar_row.addWidget(self._btn_run)
        bar_row.addWidget(self._btn_cancel_run)
        bar_row.addWidget(self._btn_clean_duplicates)
        bar_row.addStretch(1)

        self._phase = QLabel("当前阶段：就绪")
        self._phase.setStyleSheet("font-weight: 600;")

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        self._progress.setFormat("%v / %m（%p%）")
        self._progress.setMinimumHeight(22)

        self._status = QLabel("就绪：选择源文件夹后点击「扫描并预览」。")
        self._status.setWordWrap(True)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(
            ["文件名", "采用日期", "日期来源", "目标文件夹", "目标完整路径"]
        )
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)

        tip = QLabel(
            "说明：视频默认使用文件时间；照片优先 EXIF（读不到则回退文件时间）。"
        )
        tip.setStyleSheet("color: #666;")
        tip.setWordWrap(True)

        log_cap = QLabel(f"运行日志（同时写入文件：{self._log_path}）")
        log_cap.setStyleSheet("color: #444;")
        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setMaximumBlockCount(4000)
        mono = QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont)
        mono.setPointSize(9)
        self._log.setFont(mono)
        self._log.setPlaceholderText("扫描、整理过程中的提示与错误会出现在这里…")
        btn_clear_log = QPushButton("清空界面日志")
        btn_clear_log.clicked.connect(self._log.clear)
        log_btns = QHBoxLayout()
        log_btns.addWidget(log_cap, 1)
        log_btns.addWidget(btn_clear_log)

        left = QWidget()
        lv = QVBoxLayout(left)
        lv.addLayout(src_row)
        lv.addLayout(out_row)
        lv.addWidget(opt)
        lv.addWidget(mode)
        lv.addWidget(quick_cfg)
        lv.addLayout(bar_row)
        lv.addWidget(self._phase)
        lv.addWidget(self._progress)
        lv.addWidget(self._status)
        lv.addWidget(tip)

        right_split = QSplitter(Qt.Vertical)
        right_split.addWidget(self._table)
        log_wrap = QWidget()
        log_l = QVBoxLayout(log_wrap)
        log_l.setContentsMargins(0, 0, 0, 0)
        log_l.addLayout(log_btns)
        log_l.addWidget(self._log)
        right_split.addWidget(log_wrap)
        right_split.setStretchFactor(0, 3)
        right_split.setStretchFactor(1, 2)
        right_split.setSizes([340, 220])

        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(left)
        splitter.addWidget(right_split)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([360, 620])

        root = QVBoxLayout(self)
        root.addWidget(splitter)

        f = QFont()
        f.setPointSize(10)
        self.setFont(f)

        # 支持：将文件夹拖到快捷方式，或命令行传入路径
        if len(sys.argv) > 1:
            p = Path(sys.argv[1]).expanduser().resolve()
            if p.is_dir():
                self._src.setText(str(p))
                if self._same_as_src.isChecked():
                    self._out.setText(str(p))

        LOG.info("程序已启动，日志文件：%s", self._log_path)

    @Slot(str)
    def _append_log(self, line: str) -> None:
        self._log.appendPlainText(line.rstrip("\n"))

    def _bring_window_forward(self) -> None:
        try:
            self.raise_()
            self.activateWindow()
        except Exception:
            LOG.exception("激活主窗口失败")

    def _safe_information(self, title: str, text: str) -> None:
        try:
            QMessageBox.information(self, title, text)
        except Exception:
            LOG.exception("提示框显示失败：%s", title)
        finally:
            self._bring_window_forward()

    def _safe_critical(self, title: str, text: str) -> None:
        try:
            QMessageBox.critical(self, title, text)
        except Exception:
            LOG.exception("错误框显示失败：%s", title)
        finally:
            self._bring_window_forward()

    @Slot()
    def _open_output_folder(self) -> None:
        path = self._out.text().strip() or self._cfg_library_root.text().strip()
        if not path:
            QMessageBox.warning(self, "提示", "请先设置输出根目录或一键模式中的照片库目录。")
            return
        p = Path(path)
        if not p.is_dir():
            QMessageBox.warning(self, "提示", f"目录不存在：\n{path}")
            return
        try:
            if os.name == "nt":
                os.startfile(str(p.resolve()))
            else:
                QDesktopServices.openUrl(QUrl.fromLocalFile(str(p.resolve())))
        except Exception as e:
            LOG.exception("打开文件夹失败")
            QMessageBox.critical(self, "错误", f"无法打开文件夹：{e}")

    @Slot()
    def _deferred_start_run_after_scan(self) -> None:
        if self._plan:
            self._start_run(skip_confirm=True)

    def _sync_out_with_src(self) -> None:
        if self._same_as_src.isChecked():
            t = self._src.text().strip()
            if t:
                self._out.setText(t)

    def _on_same_toggled(self, on: bool) -> None:
        self._out.setEnabled(not on)
        if on:
            self._sync_out_with_src()
        elif not self._out.text().strip() and self._src.text().strip():
            self._out.setText(self._src.text().strip())

    def _pick_src(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "选择源照片文件夹")
        if d:
            self._src.setText(d)
            if self._same_as_src.isChecked():
                self._out.setText(d)

    def _pick_out(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "选择输出根目录")
        if d:
            self._out.setText(d)
            self._same_as_src.setChecked(False)

    def _pick_cfg_library_root(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "选择默认照片库目录")
        if d:
            self._cfg_library_root.setText(d)

    def _save_quick_config(self, notify: bool = True) -> bool:
        library_root = self._cfg_library_root.text().strip()
        camera_label = self._cfg_camera_label.text().strip()
        media_subdir = self._cfg_camera_subdir.text().strip().replace("/", "\\")
        if not library_root or not camera_label or not media_subdir:
            if notify:
                QMessageBox.warning(self, "提示", "一键模式设置不能为空。")
            return False
        self._cfg = {
            "library_root": library_root,
            "camera_volume_label": camera_label,
            "camera_media_subdir": media_subdir,
        }
        try:
            save_app_config(self._cfg)
            if not self._same_as_src.isChecked():
                self._out.setText(library_root)
            if notify:
                QMessageBox.information(self, "已保存", "一键模式设置已保存。")
            LOG.info("一键模式设置已保存：%s", self._cfg)
            return True
        except Exception as e:
            LOG.exception("保存配置失败")
            if notify:
                QMessageBox.critical(self, "保存失败", str(e))
            return False

    def _start_scan(self) -> None:
        src = self._src.text().strip()
        if not src:
            QMessageBox.warning(self, "提示", "请先选择源文件夹。")
            return
        out = src if self._same_as_src.isChecked() else self._out.text().strip()
        if not out:
            QMessageBox.warning(self, "提示", "请指定输出根目录，或勾选「输出目录与源目录相同」。")
            return

        self._btn_scan.setEnabled(False)
        self._btn_import_card.setEnabled(False)
        self._btn_history.setEnabled(False)
        self._btn_run.setEnabled(False)
        self._btn_clean_duplicates.setEnabled(False)
        self._plan.clear()
        self._table.setRowCount(0)
        self._phase.setText("当前阶段：扫描并生成预览")
        self._status.setText("正在枚举并读取 EXIF，请稍候…")
        self._progress.setVisible(True)
        self._progress.setRange(0, 1)
        self._progress.setValue(0)
        self._progress.setFormat("0%")
        LOG.info("扫描任务已提交：源=%s，输出=%s", src, out)

        self._scan_thread = QThread()
        self._scan_worker = ScanWorker(
            src,
            out,
            self._recursive.isChecked(),
            self._prefer_exif.isChecked(),
            self._pattern.currentData(),
        )
        self._scan_worker.moveToThread(self._scan_thread)
        self._scan_thread.started.connect(self._scan_worker.run)
        self._scan_worker.progress.connect(self._on_scan_progress)
        self._scan_worker.finished.connect(self._on_scan_done)
        self._scan_worker.failed.connect(self._on_scan_err)
        self._scan_worker.finished.connect(self._scan_thread.quit)
        self._scan_worker.failed.connect(self._scan_thread.quit)
        self._scan_worker.finished.connect(self._scan_worker.deleteLater)
        self._scan_worker.failed.connect(self._scan_worker.deleteLater)
        self._scan_thread.finished.connect(self._scan_thread.deleteLater)
        self._scan_thread.start()
        LOG.info("扫描线程已启动。")

    @Slot(str, int, int, str)
    def _on_scan_progress(self, stage: str, cur: int, total: int, name: str) -> None:
        if stage == "枚举文件":
            if total <= 0:
                self._progress.setRange(0, 0)
                self._progress.setFormat("枚举中…")
                self._status.setText(f"枚举文件：已检查 {cur} 个 — {name}")
            else:
                self._progress.setRange(0, max(total, 1))
                self._progress.setValue(cur)
                self._progress.setFormat("%v / %m（%p%）")
                self._status.setText(f"枚举文件：{cur} / {total} — {name}")
        else:
            self._progress.setRange(0, max(total, 1))
            self._progress.setValue(cur)
            self._progress.setFormat("%v / %m（%p%）")
            self._status.setText(f"{stage}：{cur} / {total} — {name}")

        if cur == 1:
            LOG.info("%s阶段开始。", stage)
        elif cur % 50 == 0 or (total > 0 and cur == total):
            suffix = f"{cur} / {total}" if total > 0 else f"已检查 {cur}"
            LOG.info("%s进度：%s", stage, suffix)

    @Slot(list)
    def _on_scan_done(self, items: list) -> None:
        self._scan_worker = None
        self._plan = items
        if not self._skip_preview_table:
            self._fill_table()
        else:
            self._table.setRowCount(0)
        self._btn_scan.setEnabled(True)
        self._btn_import_card.setEnabled(True)
        self._btn_history.setEnabled(True)
        self._btn_run.setEnabled(bool(self._plan))
        self._btn_clean_duplicates.setEnabled(True)
        self._progress.setVisible(False)
        self._progress.setFormat("%v / %m（%p%）")
        self._phase.setText("当前阶段：预览已就绪，等待执行")
        if self._skip_preview_table:
            self._status.setText(f"扫描完成：共 {len(self._plan)} 张，准备执行导入…")
        else:
            self._status.setText(f"共 {len(self._plan)} 个媒体文件，请确认右侧表格后点击「执行整理」。")
        LOG.info("界面已加载预览：%s 条。", len(self._plan))
        if self._auto_run_after_scan:
            self._auto_run_after_scan = False
            self._skip_preview_table = False
            if self._plan:
                QTimer.singleShot(0, self._deferred_start_run_after_scan)
            else:
                QTimer.singleShot(
                    0, partial(self._safe_information, "提示", "未找到可整理的照片。")
                )
        else:
            self._skip_preview_table = False

    @Slot(str)
    def _on_scan_err(self, msg: str) -> None:
        self._scan_worker = None
        self._btn_scan.setEnabled(True)
        self._btn_import_card.setEnabled(True)
        self._btn_history.setEnabled(True)
        self._btn_clean_duplicates.setEnabled(True)
        self._auto_run_after_scan = False
        self._skip_preview_table = False
        self._progress.setVisible(False)
        self._phase.setText("当前阶段：扫描失败")
        self._status.setText("扫描失败，详见日志。")
        QTimer.singleShot(0, partial(self._safe_critical, "扫描失败", msg))

    def _fill_table(self) -> None:
        self._table.setRowCount(len(self._plan))
        for row, pf in enumerate(self._plan):
            self._table.setItem(row, 0, QTableWidgetItem(pf.source.name))
            self._table.setItem(row, 1, QTableWidgetItem(pf.taken_at.strftime("%Y-%m-%d %H:%M:%S")))
            self._table.setItem(row, 2, QTableWidgetItem(pf.date_source))
            self._table.setItem(row, 3, QTableWidgetItem(str(pf.dest_folder)))
            self._table.setItem(row, 4, QTableWidgetItem(str(pf.dest_path)))
        self._table.resizeColumnsToContents()

    def _start_run(self, skip_confirm: bool = False) -> None:
        if not self._plan:
            return
        move = self._mode_move.isChecked()
        act = "移动" if move else "复制"
        n = len(self._plan)
        if not skip_confirm:
            r = QMessageBox.question(
                self,
                "确认",
                f"将对 {n} 个文件执行「{act}」。是否继续？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if r != QMessageBox.Yes:
                return

        self._btn_scan.setEnabled(False)
        self._btn_import_card.setEnabled(False)
        self._btn_history.setEnabled(False)
        self._btn_run.setEnabled(False)
        self._btn_clean_duplicates.setEnabled(False)
        self._btn_cancel_run.setEnabled(True)
        self._phase.setText(f"当前阶段：正在{act}文件")
        self._status.setText(f"准备{act}，共 {n} 个文件…")
        self._progress.setVisible(True)
        self._progress.setRange(0, n)
        self._progress.setValue(0)
        self._progress.setFormat("%v / %m（%p%）")
        LOG.info("用户确认执行：%s，%s 个文件。", act, n)
        self._run_mode_cn = act

        self._run_thread = QThread()
        self._run_worker = RunWorker(self._plan, move)
        self._run_worker.moveToThread(self._run_thread)
        self._run_thread.started.connect(self._run_worker.run)
        self._run_worker.progress.connect(self._on_run_progress)
        self._run_worker.finished_ok.connect(self._on_run_done)
        self._run_worker.failed.connect(self._on_run_err)
        self._run_worker.finished_ok.connect(self._run_thread.quit)
        self._run_worker.failed.connect(self._run_thread.quit)
        self._run_worker.finished_ok.connect(self._run_worker.deleteLater)
        self._run_worker.failed.connect(self._run_worker.deleteLater)
        self._run_thread.finished.connect(self._run_thread.deleteLater)
        self._run_thread.start()

    @Slot()
    def _on_cancel_run(self) -> None:
        if self._run_worker is not None:
            self._run_worker.cancel()

    @Slot(int, int, str, str)
    def _on_run_progress(self, cur: int, total: int, name: str, dest: str) -> None:
        self._progress.setMaximum(total)
        self._progress.setValue(cur)
        pct = int(round(100 * cur / total)) if total else 0
        self._status.setText(
            f"正在{self._run_mode_cn}：{cur} / {total}（{pct}%）\n"
            f"文件：{name}\n"
            f"目标：{dest}"
        )
        if cur == 1:
            LOG.info("执行阶段开始，共 %s 个文件。", total)
        elif cur % 50 == 0 or cur == total:
            LOG.info("执行进度：%s / %s", cur, total)

    @Slot(int, int, bool)
    def _on_run_done(self, ok: int, err: int, was_cancelled: bool) -> None:
        self._run_worker = None
        self._btn_scan.setEnabled(True)
        self._btn_import_card.setEnabled(True)
        self._btn_history.setEnabled(True)
        self._btn_run.setEnabled(False)
        self._btn_cancel_run.setEnabled(False)
        self._btn_clean_duplicates.setEnabled(True)
        self._auto_run_after_scan = False
        self._progress.setVisible(False)
        self._plan.clear()
        self._table.setRowCount(0)
        if was_cancelled:
            self._phase.setText("当前阶段：已取消")
            self._status.setText(
                f"已取消：成功 {ok}，失败 {err}。后续文件未处理；可再次扫描继续。"
            )
            QTimer.singleShot(
                0,
                partial(
                    self._safe_information,
                    "已取消",
                    f"任务已取消。\n成功：{ok}\n失败：{err}",
                ),
            )
        else:
            self._phase.setText("当前阶段：已完成")
            self._status.setText(
                f"完成：成功 {ok}，失败 {err}。可再次点击「扫描并预览」继续整理其他文件。"
            )
            QTimer.singleShot(
                0,
                partial(self._safe_information, "完成", f"成功：{ok}\n失败：{err}"),
            )

    @Slot(str)
    def _on_run_err(self, msg: str) -> None:
        self._run_worker = None
        self._btn_scan.setEnabled(True)
        self._btn_import_card.setEnabled(True)
        self._btn_history.setEnabled(True)
        self._btn_run.setEnabled(True)
        self._btn_cancel_run.setEnabled(False)
        self._btn_clean_duplicates.setEnabled(True)
        self._auto_run_after_scan = False
        self._progress.setVisible(False)
        self._phase.setText("当前阶段：执行出错")
        self._status.setText("执行过程出错，详见日志。")
        QTimer.singleShot(0, partial(self._safe_critical, "错误", msg))

    def _start_redundant_dup_scan(self) -> None:
        root = self._out.text().strip() or self._cfg_library_root.text().strip()
        if not root:
            QMessageBox.warning(self, "提示", "请先设置输出根目录或一键模式中的照片库目录。")
            return
        r = Path(root)
        if not r.is_dir():
            QMessageBox.warning(self, "提示", f"目录不存在：\n{root}")
            return
        self._btn_clean_duplicates.setEnabled(False)
        self._btn_scan.setEnabled(False)
        self._btn_import_card.setEnabled(False)
        self._btn_history.setEnabled(False)
        self._btn_run.setEnabled(False)
        self._phase.setText("当前阶段：扫描冗余副本")
        self._status.setText("正在深度遍历图库，查找可安全删除的 _1、_2 副本…")
        self._progress.setVisible(True)
        self._progress.setRange(0, 0)
        self._progress.setFormat("扫描中…")

        self._dup_scan_thread = QThread()
        self._dup_scan_worker = RedundantDupScanWorker(str(r))
        self._dup_scan_worker.moveToThread(self._dup_scan_thread)
        self._dup_scan_thread.started.connect(self._dup_scan_worker.run)
        self._dup_scan_worker.finished.connect(
            self._on_dup_scan_done, Qt.QueuedConnection
        )
        self._dup_scan_worker.failed.connect(
            self._on_dup_scan_failed, Qt.QueuedConnection
        )
        # 先处理结果再结束线程、再 deleteLater，降低竞态
        self._dup_scan_worker.finished.connect(self._dup_scan_thread.quit)
        self._dup_scan_worker.failed.connect(self._dup_scan_thread.quit)
        self._dup_scan_thread.finished.connect(self._dup_scan_worker.deleteLater)
        self._dup_scan_thread.finished.connect(self._dup_scan_thread.deleteLater)
        self._dup_scan_thread.start()
        LOG.info("冗余副本扫描已启动：%s", r)

    @Slot(str, int)
    def _on_dup_scan_done(self, paths_json: str, total_bytes: int) -> None:
        self._dup_scan_worker = None
        try:
            raw = json.loads(paths_json)
            if not isinstance(raw, list):
                raise TypeError("扫描结果不是列表")
            entries: list[dict[str, object]] = []
            for item in raw:
                if not isinstance(item, dict):
                    raise TypeError("扫描结果项格式错误")
                entries.append(
                    {
                        "candidate": str(item["candidate"]),
                        "original": str(item["original"]),
                        "size_bytes": int(item["size_bytes"]),
                        "mtime_diff_sec": float(item["mtime_diff_sec"]),
                    }
                )
        except Exception:
            LOG.exception("解析冗余扫描结果失败，原始长度=%s", len(paths_json))
            self._progress.setVisible(False)
            self._phase.setText("当前阶段：就绪")
            self._btn_scan.setEnabled(True)
            self._btn_import_card.setEnabled(True)
            self._btn_history.setEnabled(True)
            self._btn_run.setEnabled(bool(self._plan))
            self._btn_clean_duplicates.setEnabled(True)
            self._status.setText("扫描结果解析失败，详见日志。")
            QTimer.singleShot(
                0,
                partial(
                    self._safe_critical,
                    "内部错误",
                    "无法解析扫描结果，请查看日志文件。",
                ),
            )
            return

        self._progress.setVisible(False)
        self._phase.setText("当前阶段：就绪")
        self._btn_scan.setEnabled(True)
        self._btn_import_card.setEnabled(True)
        self._btn_history.setEnabled(True)
        self._btn_run.setEnabled(bool(self._plan))
        mb = total_bytes / (1024 * 1024) if total_bytes else 0.0
        if not entries:
            self._btn_clean_duplicates.setEnabled(True)
            self._status.setText("未发现可安全清理的冗余副本。")
            QTimer.singleShot(
                0,
                partial(
                    self._safe_information,
                    "扫描完成",
                    "未发现可安全清理的冗余副本（需同目录存在原文件，且大小与修改时间一致）。",
                ),
            )
            return
        self._status.setText(
            f"扫描完成：发现 {len(entries)} 个可删副本（约 {mb:.2f} MB），将导出 CSV 后确认…"
        )
        QTimer.singleShot(0, partial(self._prompt_redundant_dup_delete, entries))

    @Slot(str)
    def _on_dup_scan_failed(self, msg: str) -> None:
        self._dup_scan_worker = None
        self._progress.setVisible(False)
        self._phase.setText("当前阶段：就绪")
        self._btn_scan.setEnabled(True)
        self._btn_import_card.setEnabled(True)
        self._btn_history.setEnabled(True)
        self._btn_run.setEnabled(bool(self._plan))
        self._btn_clean_duplicates.setEnabled(True)
        self._status.setText("冗余副本扫描失败，详见日志。")
        QTimer.singleShot(0, partial(self._safe_critical, "扫描失败", msg))

    def _write_cleanup_report_csv(self, entries: list[dict[str, object]]) -> Path:
        path = cleanup_report_csv_path()
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["冗余副本路径", "推导原文件路径", "大小(Bytes)", "修改时间差(秒)"])
            for e in entries:
                w.writerow(
                    [
                        e["candidate"],
                        e["original"],
                        e["size_bytes"],
                        e["mtime_diff_sec"],
                    ]
                )
        LOG.info("待清理明细已写入：%s（共 %s 行）", path, len(entries))
        return path

    def _prompt_redundant_dup_delete(self, entries: list[dict[str, object]]) -> None:
        count = len(entries)
        total_b = sum(int(e["size_bytes"]) for e in entries)
        mb = total_b / (1024 * 1024)
        delete_started = False
        try:
            try:
                report_path = self._write_cleanup_report_csv(entries)
            except Exception as ex:
                LOG.exception("写入 cleanup_report.csv 失败")
                QMessageBox.critical(
                    self,
                    "导出失败",
                    f"无法写入待清理明细 CSV：{ex}",
                )
                self._status.setText("导出 CSV 失败，未执行删除。")
                self._btn_clean_duplicates.setEnabled(True)
                return

            QMessageBox.information(
                self,
                "已导出待清理明细",
                f"已将待清理明细导出至：\n{report_path}\n\n"
                f"共 {count} 行，约 {mb:.2f} MB。\n\n"
                "请用 Excel 或记事本打开 cleanup_report.csv，核对「冗余副本路径」与"
                "「推导原文件路径」无误后，再在下一步选择是否删除。\n\n"
                "选择「否」将只保留此 CSV，不会删除文件。",
            )
        finally:
            self._bring_window_forward()

        try:
            r = QMessageBox.question(
                self,
                "确认清理",
                f"扫描到 {count} 个冗余副本，明细已保存为：\n{CLEANUP_REPORT_CSV_NAME}\n\n"
                "规则摘要：序号在 "
                f"1～{DUP_CLEANUP_INDEX_MAX}；与同目录原文件大小一致；"
                f"mtime 相差 ≤ {MTIME_DEDUP_TOLERANCE_SEC:.1f} 秒。\n\n"
                "是否立即从磁盘删除这些「冗余副本」文件？\n"
                "（选「否」仅保留 CSV，不删除。）",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if r == QMessageBox.Yes:
                delete_started = True
                paths_only = [str(e["candidate"]) for e in entries]
                self._start_dup_delete(paths_only)
            else:
                LOG.info("用户选择不删除冗余副本，仅保留 CSV。")
                self._status.setText("已取消删除；明细见 cleanup_report.csv。")
        finally:
            self._bring_window_forward()
            if not delete_started:
                self._btn_clean_duplicates.setEnabled(True)

    def _start_dup_delete(self, paths: list[str]) -> None:
        self._btn_clean_duplicates.setEnabled(False)
        self._btn_scan.setEnabled(False)
        self._btn_import_card.setEnabled(False)
        self._btn_history.setEnabled(False)
        self._btn_run.setEnabled(False)
        self._phase.setText("当前阶段：删除冗余副本")
        self._status.setText(f"正在删除 {len(paths)} 个文件…")
        self._progress.setVisible(True)
        self._progress.setRange(0, 0)
        self._progress.setFormat("删除中…")

        self._dup_delete_thread = QThread()
        self._dup_delete_worker = RedundantDupDeleteWorker(paths)
        self._dup_delete_worker.moveToThread(self._dup_delete_thread)
        self._dup_delete_thread.started.connect(self._dup_delete_worker.run)
        self._dup_delete_worker.finished.connect(
            self._on_dup_delete_done, Qt.QueuedConnection
        )
        self._dup_delete_worker.failed.connect(
            self._on_dup_delete_failed, Qt.QueuedConnection
        )
        self._dup_delete_worker.finished.connect(self._dup_delete_thread.quit)
        self._dup_delete_worker.failed.connect(self._dup_delete_thread.quit)
        self._dup_delete_thread.finished.connect(self._dup_delete_worker.deleteLater)
        self._dup_delete_thread.finished.connect(self._dup_delete_thread.deleteLater)
        self._dup_delete_thread.start()
        LOG.info("冗余副本删除已启动：%s 个文件", len(paths))

    @Slot(int, int)
    def _on_dup_delete_done(self, ok: int, err: int) -> None:
        self._dup_delete_worker = None
        self._progress.setVisible(False)
        self._phase.setText("当前阶段：就绪")
        self._btn_scan.setEnabled(True)
        self._btn_import_card.setEnabled(True)
        self._btn_history.setEnabled(True)
        self._btn_run.setEnabled(bool(self._plan))
        self._btn_clean_duplicates.setEnabled(True)
        self._status.setText(f"清理完成：成功删除 {ok}，失败 {err}。")
        QTimer.singleShot(
            0,
            partial(self._safe_information, "清理完成", f"成功删除：{ok}\n失败：{err}"),
        )

    @Slot(str)
    def _on_dup_delete_failed(self, msg: str) -> None:
        self._dup_delete_worker = None
        self._progress.setVisible(False)
        self._phase.setText("当前阶段：就绪")
        self._btn_scan.setEnabled(True)
        self._btn_import_card.setEnabled(True)
        self._btn_history.setEnabled(True)
        self._btn_run.setEnabled(bool(self._plan))
        self._btn_clean_duplicates.setEnabled(True)
        self._status.setText("清理过程出错，详见日志。")
        QTimer.singleShot(0, partial(self._safe_critical, "清理失败", msg))

    def _one_click_import(self) -> None:
        if not self._save_quick_config(notify=False):
            QMessageBox.warning(self, "提示", "请先完善并保存「一键模式设置」。")
            return
        label = self._cfg["camera_volume_label"]
        media_subdir = Path(self._cfg["camera_media_subdir"])
        library_root = Path(self._cfg["library_root"])
        src = find_camera_media_dir(target_volume_label=label, relative_media_dir=media_subdir)
        if src is None:
            QMessageBox.warning(
                self,
                "未找到相机卡",
                f"未识别到卷标为「{label}」且包含「{media_subdir}」的存储卡。",
            )
            return
        self._mode_copy.setChecked(True)
        self._same_as_src.setChecked(False)
        self._src.setText(str(src))
        self._out.setText(str(library_root))
        self._recursive.setChecked(False)
        self._prefer_exif.setChecked(True)
        self._auto_run_after_scan = True
        self._skip_preview_table = True
        LOG.info("一键导卡：源=%s，目标=%s", src, library_root)
        self._start_scan()

    def _organize_history(self) -> None:
        if not self._save_quick_config(notify=False):
            QMessageBox.warning(self, "提示", "请先完善并保存「一键模式设置」。")
            return
        src = Path(self._cfg["library_root"])
        if not src.exists():
            QMessageBox.warning(
                self,
                "目录不存在",
                f"历史照片目录不存在：{src}",
            )
            return
        self._mode_move.setChecked(True)
        self._same_as_src.setChecked(True)
        self._src.setText(str(src))
        self._out.setText(str(src))
        self._recursive.setChecked(False)
        self._prefer_exif.setChecked(True)
        self._auto_run_after_scan = True
        self._skip_preview_table = True
        LOG.info("整理历史照片：目录=%s", src)
        self._start_scan()


def main() -> int:
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
