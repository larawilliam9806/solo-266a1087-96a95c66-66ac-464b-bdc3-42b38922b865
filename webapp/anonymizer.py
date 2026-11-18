"""代号化导出器：把含敏感信息的传感器落地库加工成可外发的代号库。

需求与设计的对应关系：
- 仅接受 <= MAX_EXPORT_BYTES（10MB）、与演示库同结构的库；额外用户表/视图/触发器
  以及列、主外键、AUTOINCREMENT 等结构差异一律拒绝（validate_structure）。
- 受理时先用 SQLite backup API 对“当前库”做完整一致性快照，之后的一切处理都在
  快照与新文件上进行；调用方随后替换/清除当前库都不会混入另一版数据（make_snapshot）。
- 每个原 sensor_id 用 secrets 生成唯一随机代号，两表同步改写；location 统一替换为
  “已隐藏”；其余采样字段、主键、外键关系与重复行数原样保留（anonymize_snapshot）。
- 保留字段（metric/unit/ts/ingested_at/batch_id 等）若含原编号或位置文本，拒绝导出，
  避免敏感信息从旁路字段漏出（scan_retained_leaks + 成品字节级复核）。
- 成品写入“全新创建”的库文件而非原地改写，天然不含旧页；导出后再做字节级扫描，
  确认活动数据与空闲页都不存在任何旧编号/旧位置，并确认无 WAL/SHM 旁路文件残留。
- 任何一步失败都抛 ExportError，调用方不会得到半成品下载；工作目录由调用方负责清理。
"""
from __future__ import annotations

import os
import secrets
import sqlite3
from urllib.parse import quote

MAX_EXPORT_BYTES = 10 * 1024 * 1024  # 仅受理不超过 10MB 的库
HIDDEN_LOCATION = "已隐藏"
CODE_PREFIX = "SN-"
_CODE_HEX_LEN = 10  # 40 bit 随机空间，配合已用集合去重，碰撞概率可忽略

SQLITE_MAGIC = b"SQLite format 3\x00"

# 与 demo/make_demo_db.py 完全一致的目标结构（“与演示库同结构”的硬编码基准）。
# 列定义：(列名, 类型亲和, 非空, 主键序号, 默认值)
EXPECTED_SCHEMA = {
    "sensors": {
        # 注意：SQLite 对 TEXT PRIMARY KEY 的 notnull 在 table_info 中历史地报告为 0
        "columns": [
            ("sensor_id", "TEXT", 0, 1, None),
            ("location", "TEXT", 1, 0, None),
            ("metric", "TEXT", 1, 0, None),
            ("unit", "TEXT", 1, 0, None),
        ],
        "foreign_keys": [],
        "autoincrement": False,
    },
    "sensor_samples": {
        "columns": [
            ("id", "INTEGER", 0, 1, None),
            ("sensor_id", "TEXT", 1, 0, None),
            ("ts", "TEXT", 1, 0, None),
            ("value", "REAL", 1, 0, None),
            ("ingested_at", "TEXT", 1, 0, None),
            ("batch_id", "TEXT", 1, 0, None),
        ],
        # (序号, 列, 引用表, 引用列)
        "foreign_keys": [(0, "sensor_id", "sensors", "sensor_id")],
        "autoincrement": True,
    },
}

# 成品库重建结构（与演示库同构，索引一并重建）
OUTPUT_SCHEMA_SQL = """
CREATE TABLE sensors (
    sensor_id TEXT PRIMARY KEY,
    location  TEXT NOT NULL,
    metric    TEXT NOT NULL,
    unit      TEXT NOT NULL
);
CREATE TABLE sensor_samples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id   TEXT NOT NULL REFERENCES sensors(sensor_id),
    ts          TEXT NOT NULL,
    value       REAL NOT NULL,
    ingested_at TEXT NOT NULL,
    batch_id    TEXT NOT NULL
);
CREATE INDEX idx_samples_sensor_ts ON sensor_samples(sensor_id, ts);
CREATE INDEX idx_samples_batch     ON sensor_samples(batch_id);
"""

# 改写列：除此之外的列全部属于“保留字段”，需要逐值扫描是否夹带旧编号/位置。
# 用集合差集动态计算，新增列时不会漏扫（SQLite 动态类型允许 REAL 列存文本）。
REWRITTEN_COLUMNS = {
    "sensors": {"sensor_id", "location"},
    "sensor_samples": {"sensor_id"},
}


def _retained_columns():
    return {
        table: [name for (name, *_rest) in spec["columns"]
                if name not in REWRITTEN_COLUMNS[table]]
        for table, spec in EXPECTED_SCHEMA.items()
    }


RETAINED_TEXT_COLUMNS = _retained_columns()


class ExportError(Exception):
    """所有可预期的导出拒绝原因都用该异常携带中文说明。"""


# ---------------- 基础工具 ----------------

def _connect_ro(path):
    """只读打开，关闭扩展加载；结构校验与快照读取均不允许写源库。"""
    uri = "file:" + quote(os.path.abspath(path)) + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.enable_load_extension(False)
    return conn


def _connect_rw(path):
    conn = sqlite3.connect(path)
    conn.enable_load_extension(False)
    return conn


def _new_code(used):
    """生成唯一随机代号：固定前缀 + 十六进制随机串，碰撞则重新抽取。"""
    while True:
        code = CODE_PREFIX + secrets.token_hex(_CODE_HEX_LEN).upper()
        if code not in used:
            used.add(code)
            return code


# ---------------- 受理：大小、快照 ----------------

def check_source_file(path):
    """受理前的文件级校验：存在、SQLite 头、10MB 上限。"""
    if not os.path.isfile(path):
        raise ExportError("当前数据库副本不存在，请重新上传")
    size = os.path.getsize(path)
    if size > MAX_EXPORT_BYTES:
        raise ExportError(
            f"仅支持不超过 10MB 的数据库（当前 {size / 1024 / 1024:.1f}MB），已拒绝导出"
        )
    with open(path, "rb") as fh:
        if fh.read(16) != SQLITE_MAGIC:
            raise ExportError("文件不是有效的 SQLite 数据库，已拒绝导出")
    return size


def make_snapshot(src_path, snapshot_path):
    """对受理时的当前库做完整一致性快照（backup API 自动合并 WAL 内容）。

    之后所有校验/加工都针对快照，源库保持不动、仍可查询；
    调用方随后替换或清除当前库也不会影响本次导出。
    backup 完成后再 VACUUM INTO 到单文件，并删除一切 WAL/SHM/journal 旁路，
    保证工作目录里不存在携带旧值的旁路文件。
    """
    packed_path = snapshot_path + ".packed"
    src = _connect_ro(src_path)
    try:
        dst = _connect_rw(packed_path)
        try:
            src.backup(dst)
            # 将已合并的整库压缩为单文件（同时丢弃空闲页），输出到最终快照路径
            dst.execute("VACUUM INTO ?", (snapshot_path,))
        finally:
            dst.close()
    except sqlite3.Error as exc:
        raise ExportError(f"数据库快照失败：{exc}") from exc
    finally:
        src.close()
        for p in (packed_path, packed_path + "-wal", packed_path + "-shm",
                  packed_path + "-journal", snapshot_path + "-wal",
                  snapshot_path + "-shm", snapshot_path + "-journal"):
            if p != packed_path and os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass
        if os.path.exists(packed_path):
            try:
                os.remove(packed_path)
            except OSError:
                pass
    if not os.path.isfile(snapshot_path):
        raise ExportError("数据库快照失败：成品快照缺失")


# ---------------- 结构校验：只允许与演示库同构的两表库 ----------------

def _norm_type(t):
    return (t or "").strip().upper()


def validate_structure(conn):
    """结构白名单校验，任何偏差都给出明确中文拒绝原因。"""
    # 1) 对象类型：只允许两张表及其索引；视图/触发器/额外表/影子表全部拒绝。
    #    索引不属于“用户表/视图/触发器”，且成品按演示库结构统一重建，故予以放行。
    objects = conn.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
    ).fetchall()
    for otype, name in objects:
        if otype == "view":
            raise ExportError(f"数据库含视图（{name}），与演示库结构不一致，拒绝导出")
        if otype == "trigger":
            raise ExportError(f"数据库含触发器（{name}），与演示库结构不一致，拒绝导出")
        if otype not in ("table", "index"):
            raise ExportError(f"数据库含额外对象（{otype}: {name}），拒绝导出")
    table_names = sorted(name for otype, name in objects if otype == "table")
    if table_names != sorted(EXPECTED_SCHEMA):
        extra = [n for n in table_names if n not in EXPECTED_SCHEMA]
        missing = [n for n in EXPECTED_SCHEMA if n not in table_names]
        detail = []
        if extra:
            detail.append("额外用户表：" + "、".join(extra))
        if missing:
            detail.append("缺少表：" + "、".join(missing))
        raise ExportError(
            "仅支持与演示库同结构的 sensors 和 sensor_samples 两表库（"
            + "；".join(detail) + "），拒绝导出"
        )

    # pragma_table_list 能照出 sqlite_master 看不到的影子表（如 FTS 附属表）。
    # 列顺序：schema, name, type, ncol, ...；sqlite_* 为系统表（含 sqlite_sequence）。
    shadow = [
        r[1]
        for r in conn.execute("PRAGMA table_list")
        if not r[1].startswith("sqlite_") and r[1] not in EXPECTED_SCHEMA
    ]
    if shadow:
        raise ExportError("数据库含隐藏附属表（" + "、".join(shadow) + "），拒绝导出")

    for table, spec in EXPECTED_SCHEMA.items():
        quoted = '"' + table.replace('"', '""') + '"'

        # 2) 列定义逐一比对：名称/类型/非空/主键序号/默认值，顺序也要一致
        cols = conn.execute(f"PRAGMA table_info({quoted})").fetchall()
        actual = [
            (r[1], _norm_type(r[2]), int(r[3]), int(r[5] or 0), r[4])
            for r in cols
        ]
        if actual != spec["columns"]:
            raise ExportError(
                f"表 {table} 的列结构与演示库不一致，拒绝导出（实际：{actual}）"
            )

        # 3) 外键关系比对（sensor_samples.sensor_id -> sensors.sensor_id 必须保留）
        fks = conn.execute(f"PRAGMA foreign_key_list({quoted})").fetchall()
        actual_fk = sorted(
            (int(r[0]), r[3], r[2], r[4]) for r in fks  # (序号, 本表列, 引用表, 引用列)
        )
        if actual_fk != sorted(spec["foreign_keys"]):
            raise ExportError(
                f"表 {table} 的外键关系与演示库不一致（实际：{actual_fk}），拒绝导出"
            )

        # 4) AUTOINCREMENT 标记与 WITHOUT ROWID 等表选项比对
        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()[0] or ""
        upper_sql = sql.upper()
        if spec["autoincrement"] != ("AUTOINCREMENT" in upper_sql):
            raise ExportError(f"表 {table} 的 AUTOINCREMENT 定义与演示库不一致，拒绝导出")
        if "WITHOUT ROWID" in upper_sql:
            raise ExportError(f"表 {table} 使用了 WITHOUT ROWID，与演示库结构不一致，拒绝导出")

    # 5) 整库完整性与外键现状：坏库/悬空外键不进入改写环节
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise ExportError(f"数据库完整性检查未通过（{integrity}），拒绝导出")
    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise ExportError(
            f"数据库存在 {len(fk_violations)} 处外键悬空数据，与演示库结构不一致，拒绝导出"
        )


# ---------------- 敏感信息扫描 ----------------

def _load_sensor_secrets(conn):
    """读取需要抹除的原编号与原位置清单（去重、去空值）。"""
    rows = conn.execute("SELECT sensor_id, location FROM sensors").fetchall()
    ids, locations = set(), set()
    for sid, loc in rows:
        if sid is not None and str(sid) != "":
            ids.add(str(sid))
        # 原位置若本来就等于隐藏标记，改写后无可抹除的旧值，不作为敏感词处理
        if loc is not None and str(loc) != "" and str(loc) != HIDDEN_LOCATION:
            locations.add(str(loc))
    if not rows:
        raise ExportError("sensors 表为空，没有可导出的传感器数据")
    return ids, locations


def scan_retained_leaks(conn, secret_ids, secret_locations):
    """保留字段中若出现任何原编号/原位置文本，拒绝导出（旁路防漏）。"""
    needles = [(n, "原设备编号") for n in secret_ids]
    needles += [(n, "原安装位置") for n in secret_locations]
    for table, columns in RETAINED_TEXT_COLUMNS.items():
        qtbl = '"' + table.replace('"', '""') + '"'
        for col in columns:
            qcol = '"' + col.replace('"', '""') + '"'
            for (raw,) in conn.execute(f"SELECT {qcol} FROM {qtbl}"):
                if raw is None:
                    continue
                text = raw if isinstance(raw, str) else str(raw)
                for needle, kind in needles:
                    if needle in text:
                        raise ExportError(
                            f"保留字段 {table}.{col} 中含有{kind}（{needle}），"
                            "无法在不泄密的前提下导出，已拒绝"
                        )


# ---------------- 代号化改写 ----------------

def _build_id_mapping(conn, forbidden):
    # forbidden 预置全部原编号：代号既不允许彼此重复，也不允许与任何原编号相同
    used = set(forbidden)
    mapping = {}
    for (sid,) in conn.execute("SELECT sensor_id FROM sensors ORDER BY sensor_id"):
        mapping[sid] = _new_code(used)
    if len(set(mapping.values())) != len(mapping):
        raise ExportError("代号生成出现重复，导出中止")  # 理论不可达，防御性检查
    return mapping


def anonymize_snapshot(snapshot_path, output_path):
    """在全新库文件中重建同构数据：代号改写 + 位置隐藏，其余字段原样保留。"""
    if os.path.exists(output_path):
        raise ExportError("导出目标路径已存在，中止以避免覆盖")  # 防御：调用方应给空目录

    src = _connect_ro(snapshot_path)
    try:
        validate_structure(src)
        secret_ids, secret_locations = _load_sensor_secrets(src)
        scan_retained_leaks(src, secret_ids, secret_locations)
        mapping = _build_id_mapping(src, secret_ids)

        # 改写前的基线统计：行数与重复次数必须在成品中逐一对上
        base_sensor_count = src.execute("SELECT COUNT(*) FROM sensors").fetchone()[0]
        base_sample_count = src.execute("SELECT COUNT(*) FROM sensor_samples").fetchone()[0]
        base_dup_groups = src.execute(
            "SELECT COUNT(*) FROM (SELECT 1 FROM sensor_samples "
            "GROUP BY sensor_id, ts HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        base_dup_extra = src.execute(
            "SELECT COALESCE(SUM(n - 1), 0) FROM ("
            "SELECT COUNT(*) AS n FROM sensor_samples GROUP BY sensor_id, ts HAVING n > 1)"
        ).fetchone()[0]

        dst = _connect_rw(output_path)
        try:
            dst.execute("PRAGMA foreign_keys = ON")
            dst.executescript(OUTPUT_SCHEMA_SQL)

            sensors = src.execute(
                "SELECT sensor_id, location, metric, unit FROM sensors ORDER BY sensor_id"
            ).fetchall()
            dst.executemany(
                "INSERT INTO sensors (sensor_id, location, metric, unit) VALUES (?,?,?,?)",
                [(mapping[sid], HIDDEN_LOCATION, metric, unit) for sid, _loc, metric, unit in sensors],
            )

            # 采样行逐列原样搬运，仅 sensor_id 同步改写；id/ts/value/ingested_at/batch_id 不变，
            # 因此同一 (sensor_id, ts) 的重复行与重复次数完整保留
            samples = src.execute(
                "SELECT id, sensor_id, ts, value, ingested_at, batch_id "
                "FROM sensor_samples ORDER BY id"
            ).fetchall()
            dst.executemany(
                "INSERT INTO sensor_samples "
                "(id, sensor_id, ts, value, ingested_at, batch_id) VALUES (?,?,?,?,?,?)",
                [(row_id, mapping[sid], ts, value, ingested_at, batch_id)
                 for row_id, sid, ts, value, ingested_at, batch_id in samples],
            )
            dst.commit()

            _verify_output(dst, mapping, base_sensor_count, base_sample_count,
                           base_dup_groups, base_dup_extra)
        finally:
            dst.close()
    except sqlite3.Error as exc:
        raise ExportError(f"代号化处理失败：{exc}") from exc
    except Exception:
        # 任何其他失败（含结构/泄密拒绝后的防御性兜底）都不允许留下半成品文件
        if os.path.exists(output_path):
            try:
                os.remove(output_path)
            except OSError:
                pass
        raise
    finally:
        src.close()

    # 文件级复核：全新文件 + 字节扫描，确保活动数据与空闲页都没有旧值残留
    _verify_output_bytes(output_path, secret_ids, secret_locations)
    return {
        "sensor_count": base_sensor_count,
        "sample_count": base_sample_count,
        "duplicate_groups": base_dup_groups,
        "duplicate_extra_rows": base_dup_extra,
        "mapping": mapping,
    }


def _verify_output(dst, mapping, n_sensors, n_samples, dup_groups, dup_extra):
    """成品库逻辑校验：结构关系、行数、重复次数、隐藏效果全部可验证。"""
    if dst.execute("PRAGMA foreign_key_check").fetchall():
        raise ExportError("成品库外键校验失败，拒绝提供下载")
    if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ExportError("成品库完整性检查失败，拒绝提供下载")
    if dst.execute("SELECT COUNT(*) FROM sensors").fetchone()[0] != n_sensors:
        raise ExportError("成品传感器行数与源库不一致，拒绝提供下载")
    if dst.execute("SELECT COUNT(*) FROM sensor_samples").fetchone()[0] != n_samples:
        raise ExportError("成品采样行数与源库不一致，拒绝提供下载")
    if dst.execute("SELECT COUNT(*) FROM sensors WHERE location <> ?",
                   (HIDDEN_LOCATION,)).fetchone()[0] != 0:
        raise ExportError("成品库仍存在未隐藏的位置，拒绝提供下载")
    if dst.execute("SELECT COUNT(DISTINCT sensor_id) FROM sensors").fetchone()[0] != len(mapping):
        raise ExportError("成品库代号不唯一，拒绝提供下载")
    orphan = dst.execute(
        "SELECT COUNT(*) FROM sensor_samples ss WHERE NOT EXISTS "
        "(SELECT 1 FROM sensors s WHERE s.sensor_id = ss.sensor_id)"
    ).fetchone()[0]
    if orphan != 0:
        raise ExportError("成品库存在脱离主表的采样行，拒绝提供下载")
    got_groups = dst.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM sensor_samples "
        "GROUP BY sensor_id, ts HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    got_extra = dst.execute(
        "SELECT COALESCE(SUM(n - 1), 0) FROM ("
        "SELECT COUNT(*) AS n FROM sensor_samples GROUP BY sensor_id, ts HAVING n > 1)"
    ).fetchone()[0]
    if got_groups != dup_groups or got_extra != dup_extra:
        raise ExportError(
            f"成品库重复次数与源库不一致（组 {got_groups}/{dup_groups}，"
            f"多余行 {got_extra}/{dup_extra}），拒绝提供下载"
        )


def _verify_output_bytes(output_path, secret_ids, secret_locations):
    """对成品文件做原始字节扫描：活动数据和空闲页都不得留下旧值。"""
    for suffix in ("-wal", "-shm", "-journal"):
        if os.path.exists(output_path + suffix):
            raise ExportError(f"成品库存在事务旁路文件（{os.path.basename(output_path)}{suffix}），拒绝提供下载")
    with open(output_path, "rb") as fh:
        blob = fh.read()
    if blob[:16] != SQLITE_MAGIC:
        raise ExportError("成品文件不是有效的 SQLite 数据库，拒绝提供下载")
    for needle in (*secret_ids, *secret_locations):
        if needle.encode("utf-8") in blob:
            raise ExportError(
                f"成品文件字节扫描发现旧值残留（{needle}），活动数据或空闲页未清理干净，拒绝提供下载"
            )


# ---------------- 对外入口 ----------------

def export_anonymized_db(src_path, work_dir):
    """完整导出流程：受理快照 -> 校验 -> 改写 -> 复核。

    成功返回 (成品路径, 统计信息)；任何失败抛 ExportError，且 work_dir 内无成品。
    源库全程只读访问，原副本仍可正常查询。
    """
    check_source_file(src_path)
    snapshot_path = os.path.join(work_dir, "snapshot.db")
    output_path = os.path.join(work_dir, "sensors_coded.db")
    make_snapshot(src_path, snapshot_path)
    stats = anonymize_snapshot(snapshot_path, output_path)
    if not os.path.isfile(output_path):
        raise ExportError("成品文件缺失，拒绝提供下载")
    return output_path, stats
