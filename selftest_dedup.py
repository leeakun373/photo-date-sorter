# -*- coding: utf-8 -*-
"""自检 plan_sort 的 size+mtime 去重与同名冲突。运行：python selftest_dedup.py"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from photo_date_sorter import MTIME_DEDUP_TOLERANCE_SEC, plan_sort  # noqa: E402


def main() -> int:
    failures: list[str] = []
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        src = base / "src"
        out = base / "out"
        src.mkdir()
        out.mkdir()
        pic = src / "testpic.jpg"
        pic.write_bytes(b"\xff\xd8\xff\xd9")  # 最小 JPEG 魔数，EXIF 通常为空，走文件时间

        p1 = plan_sort(src, out, False, False, "YYYY-MM-DD")
        if len(p1) != 1:
            failures.append(f"首次计划应为 1 个文件，实际 {len(p1)}")
        else:
            p1[0].dest_folder.mkdir(parents=True, exist_ok=True)
            shutil.copy2(pic, p1[0].dest_path)

        p2 = plan_sort(src, out, False, False, "YYYY-MM-DD")
        if len(p2) != 0:
            failures.append(f"去重后应为 0 个待复制项，实际 {len(p2)}")

        # 同名不同内容：触发 unique_dest -> *_1.jpg
        dest_primary = p1[0].dest_path
        dest_primary.write_bytes(b"x")
        p3 = plan_sort(src, out, False, False, "YYYY-MM-DD")
        if len(p3) != 1:
            failures.append(f"冲突时应为 1 个文件（带 _1），实际 {len(p3)}")
        elif "_1" not in p3[0].dest_path.name:
            failures.append(f"冲突时应生成 _1 后缀，实际 {p3[0].dest_path.name!r}")

    if failures:
        print("selftest_dedup FAIL:")
        for f in failures:
            print(" -", f)
        return 1
    print(
        "selftest_dedup OK（去重 + 冲突重命名）。"
        f" MTIME_DEDUP_TOLERANCE_SEC={MTIME_DEDUP_TOLERANCE_SEC}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
