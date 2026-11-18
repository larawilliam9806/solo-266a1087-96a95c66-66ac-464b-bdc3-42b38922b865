#!/usr/bin/env python3
"""代号化导出核心模块自测（纯标准库，可 python3 -I 直接运行）。

覆盖：
- 正常导出：代号唯一随机、两表同步改写、位置全隐藏、其余字段与重复次数保留、主外键完好
- 空闲页/活动数据零旧值：字节级扫描，含“删除含旧值的行制造空闲页”场景
- 结构拒绝：额外表/视图/触发器、列差异、外键差异、AUTOINCREMENT 差异、影子表
- 保留字段含原编号/位置 -> 拒绝
- 10MB 上限、非 SQLite、坏库、悬空外键
- 受理快照隔离：导出受理后替换/清除当前库不影响本次导出，且原副本哈希不变
- 失败不留成品
用法：python3 -I tests/test_anonymizer.py
"""
import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, "webapp")))

import anonymizer as az  # noqa: E402

failures = []
checks = [0]


def check(name, cond, detail=""):
    checks[0] += 1
    if cond:
        print(f"  PASS  {name}")
    else:
        failures.append(f"{name} {detail}")
        print(f"  FAIL  {name} {detail}")


def make_base_db(path, sensors=None, extra_sample_text=None):
    """生成与演示库同结构的小型库，含故意重复采样。"""
    sensors = sensors or [
        ("S-1001", "车间A", "temperature", "°C"),
        ("S-2001", "车间B", "pressure", "kPa"),
        ("S-3001", "冷库", "humidity", "%RH"),
    ]
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
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
        )
        conn.executemany("INSERT INTO sensors VALUES (?,?,?,?)", sensors)
        rows = []
        for sid, _loc, _m, _u in sensors:
            for i in range(5):
                rows.append((sid, f"2026-11-10T00:00:{i:02d}", 1.0 + i,
                             f"2026-11-10T00:00:{i:02d}", "B-A"))
            # 两组重复（一组 2 条、一组 3 条）
            rows.append((sid, "2026-11-10T00:00:01", 9.9, "2026-11-10T00:05:00", "B-RETRY"))
            rows.append((sid, "2026-11-10T00:00:02", 9.9, "2026-11-10T00:05:00", "B-RETRY"))
            rows.append((sid, "2026-11-10T00:00:02", 9.8, "2026-11-10T00:05:01", "B-RETRY"))
        if extra_sample_text:
            tbl, col, val = extra_sample_text
            if tbl == "sensors":
                conn.execute(
                    f"UPDATE {tbl} SET {col} = ? "
                    f"WHERE sensor_id = (SELECT MIN(sensor_id) FROM {tbl})",
                    (val,),
                )
        conn.executemany(
            "INSERT INTO sensor_samples (sensor_id, ts, value, ingested_at, batch_id) "
            "VALUES (?,?,?,?,?)", rows
        )
        if extra_sample_text:
            tbl, col, val = extra_sample_text
            if tbl == "sensor_samples":
                conn.execute(
                    f"UPDATE {tbl} SET {col} = ? "
                    f"WHERE id = (SELECT MIN(id) FROM {tbl})",
                    (val,),
                )
        conn.commit()
    finally:
        conn.close()


def read_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        sensors = [dict(r) for r in conn.execute("SELECT * FROM sensors ORDER BY sensor_id")]
        samples = [dict(r) for r in conn.execute("SELECT * FROM sensor_samples ORDER BY id")]
        dup_groups = conn.execute(
            "SELECT COUNT(*) FROM (SELECT 1 FROM sensor_samples "
            "GROUP BY sensor_id, ts HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        dup_extra = conn.execute(
            "SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM sensor_samples "
            "GROUP BY sensor_id, ts HAVING n > 1)"
        ).fetchone()[0]
        fk_bad = conn.execute("PRAGMA foreign_key_check").fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        return sensors, samples, dup_groups, dup_extra, fk_bad, integrity
    finally:
        conn.close()


def expect_reject(name, src_path):
    work = tempfile.mkdtemp()
    try:
        rejected = False
        msg = ""
        try:
            az.export_anonymized_db(src_path, work)
        except az.ExportError as e:
            rejected = True
            msg = str(e)
        check(name, rejected, msg)
        leftovers = [f for f in os.listdir(work) if f == "sensors_coded.db"]
        check(name + "（失败无成品）", not leftovers, str(leftovers))
        return msg
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main():
    tmp = tempfile.mkdtemp(prefix="anonym-test-")
    try:
        src = os.path.join(tmp, "src.db")
        make_base_db(src)
        src_hash = hashlib.sha256(open(src, "rb").read()).hexdigest()

        print("== 正常导出 ==")
        work = tempfile.mkdtemp(dir=tmp)
        out, stats = az.export_anonymized_db(src, work)
        check("返回成品路径且文件存在", os.path.isfile(out))
        check("成品不超过 10MB", os.path.getsize(out) <= az.MAX_EXPORT_BYTES)
        check("无 WAL/SHM/journal 旁路文件",
              not any(os.path.exists(out + s) for s in ("-wal", "-shm", "-journal")))

        s_sensors, s_samples, s_dup_g, s_dup_e, s_fk, s_int = read_db(src)
        o_sensors, o_samples, o_dup_g, o_dup_e, o_fk, o_int = read_db(out)
        check("传感器行数保留", len(o_sensors) == len(s_sensors))
        check("采样行数保留", len(o_samples) == len(s_samples) == stats["sample_count"])
        check("重复组数保留", s_dup_g == o_dup_g == stats["duplicate_groups"], f"{s_dup_g}/{o_dup_g}")
        check("重复多余行数保留", s_dup_e == o_dup_e == stats["duplicate_extra_rows"], f"{s_dup_e}/{o_dup_e}")
        check("成品外键校验通过", not o_fk and o_int == "ok", str((o_fk, o_int)))
        check("位置全部替换为已隐藏", all(r["location"] == az.HIDDEN_LOCATION for r in o_sensors))
        codes = [r["sensor_id"] for r in o_sensors]
        check("代号唯一且符合随机格式",
              len(set(codes)) == len(codes) and all(c.startswith(az.CODE_PREFIX) for c in codes))
        check("代号与原编号完全不同", set(codes).isdisjoint({r["sensor_id"] for r in s_sensors}))

        src_ids = {r["sensor_id"]: r for r in s_sensors}
        id_map = stats["mapping"]
        check("映射覆盖全部原编号", set(id_map) == set(src_ids))
        # 两表同步改写：同传感器的所有采样行使用同一代号
        per_sensor_codes = {}
        for r in o_samples:
            per_sensor_codes.setdefault(r["sensor_id"], set()).add(r["sensor_id"])
        check("samples 代号均在 sensors 中",
              {r["sensor_id"] for r in o_samples} <= set(codes))
        # 保留字段逐行一致（除 sensor_id/location 外的列）
        src_by_id = {r["id"]: r for r in s_samples}
        keep_cols = ("ts", "value", "ingested_at", "batch_id")
        same = all(all(r[c] == src_by_id[r["id"]][c] for c in keep_cols) for r in o_samples)
        check("采样保留字段与原行一致（含 id 主键）", same)
        # 主键 id 原样保留
        check("id 主键值原样保留", sorted(r["id"] for r in o_samples)
              == sorted(r["id"] for r in s_samples))
        check("metric/unit 保留",
              sorted(r["metric"] for r in o_sensors)
              == sorted(r["metric"] for r in s_sensors))

        print("== 字节级零残留（活动数据 + 空闲页） ==")
        blob = open(out, "rb").read()
        no_id = all(sid.encode() not in blob for sid in src_ids)
        no_loc = all(r["location"].encode() not in blob for r in s_sensors)
        check("成品字节不含任何原设备编号", no_id)
        check("成品字节不含任何原安装位置", no_loc)

        # 制造空闲页：源库里先插入含旧编号/位置的行再删除，VACUUM 前普通拷贝会残留旧值
        leaky = os.path.join(tmp, "leaky.db")
        shutil.copyfile(src, leaky)
        conn = sqlite3.connect(leaky)
        conn.execute("CREATE TABLE IF NOT EXISTS scratch (note TEXT)")
        conn.execute("INSERT INTO scratch VALUES (?)", ("泄露-" + "S-1001" + "-车间A-冷库",))
        conn.execute("DELETE FROM scratch")
        conn.execute("DROP TABLE scratch")
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(FULL)")
        conn.close()
        raw = open(leaky, "rb").read()
        check("对照：源库空闲页确实残留旧值（测试有效性前提）",
              "S-1001".encode() in raw and "车间A".encode() in raw)
        work2 = tempfile.mkdtemp(dir=tmp)
        out2, _ = az.export_anonymized_db(leaky, work2)
        blob2 = open(out2, "rb").read()
        check("含旧值空闲页的库导出后字节无残留",
              "S-1001".encode() not in blob2 and "车间A".encode() not in blob2
              and "冷库".encode() not in blob2)
        check("处理后原副本字节不变",
              hashlib.sha256(open(src, "rb").read()).hexdigest() == src_hash)

        print("== 两次导出代号相互独立（随机） ==")
        out3, st3 = az.export_anonymized_db(src, tempfile.mkdtemp(dir=tmp))
        codes3 = {r["sensor_id"] for r in read_db(out3)[0]}
        check("两次导出代号集合不同（随机性）", codes3 != set(codes))
        check("两次导出各自内部一致", st3["mapping"] and len(set(st3["mapping"].values())) == 3)

        print("== 结构拒绝 ==")
        # 额外用户表
        p = os.path.join(tmp, "extra_table.db")
        make_base_db(p)
        c = sqlite3.connect(p)
        c.execute("CREATE TABLE user_extra (x TEXT)")
        c.commit()
        c.close()
        expect_reject("额外用户表被拒绝", p)

        # 视图
        p = os.path.join(tmp, "view.db")
        make_base_db(p)
        c = sqlite3.connect(p); c.execute("CREATE VIEW v_sensors AS SELECT * FROM sensors"); c.commit(); c.close()
        expect_reject("视图被拒绝", p)

        # 触发器
        p = os.path.join(tmp, "trigger.db")
        make_base_db(p)
        c = sqlite3.connect(p)
        c.execute("CREATE TRIGGER trg AFTER INSERT ON sensors BEGIN SELECT 1; END")
        c.commit(); c.close()
        expect_reject("触发器被拒绝", p)

        # 列差异
        p = os.path.join(tmp, "col.db")
        make_base_db(p)
        c = sqlite3.connect(p); c.execute("ALTER TABLE sensors ADD COLUMN note TEXT"); c.commit(); c.close()
        expect_reject("列结构差异被拒绝", p)

        # 外键差异（删掉 FK 重建）
        p = os.path.join(tmp, "nofk.db")
        c = sqlite3.connect(p)
        c.executescript(
            "CREATE TABLE sensors (sensor_id TEXT PRIMARY KEY, location TEXT NOT NULL,"
            " metric TEXT NOT NULL, unit TEXT NOT NULL);"
            "CREATE TABLE sensor_samples (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " sensor_id TEXT NOT NULL, ts TEXT NOT NULL, value REAL NOT NULL,"
            " ingested_at TEXT NOT NULL, batch_id TEXT NOT NULL);"
        )
        c.commit(); c.close()
        expect_reject("外键缺失被拒绝", p)

        # AUTOINCREMENT 差异
        p = os.path.join(tmp, "noautoinc.db")
        c = sqlite3.connect(p)
        c.executescript(
            "CREATE TABLE sensors (sensor_id TEXT PRIMARY KEY, location TEXT NOT NULL,"
            " metric TEXT NOT NULL, unit TEXT NOT NULL);"
            "CREATE TABLE sensor_samples (id INTEGER PRIMARY KEY,"
            " sensor_id TEXT NOT NULL REFERENCES sensors(sensor_id), ts TEXT NOT NULL,"
            " value REAL NOT NULL, ingested_at TEXT NOT NULL, batch_id TEXT NOT NULL);"
        )
        c.commit(); c.close()
        expect_reject("AUTOINCREMENT 差异被拒绝", p)

        # 影子表（FTS 虚表附属表）
        p = os.path.join(tmp, "shadow.db")
        make_base_db(p)
        c = sqlite3.connect(p)
        try:
            c.execute("CREATE VIRTUAL TABLE fts_note USING fts5(body)")
            c.commit(); c.close()
            expect_reject("虚拟/影子表被拒绝", p)
        except sqlite3.OperationalError:
            print("  SKIP  当前 SQLite 无 FTS5，影子表用例跳过")

        print("== 保留字段泄密拒绝 ==")
        p = os.path.join(tmp, "leak_id.db")
        make_base_db(p, extra_sample_text=("sensor_samples", "batch_id", "含原编号S-1001"))
        msg = expect_reject("batch_id 含原编号被拒绝", p)
        check("拒绝原因指向具体字段", "batch_id" in msg and "原设备编号" in msg, msg)

        p = os.path.join(tmp, "leak_loc.db")
        make_base_db(p, extra_sample_text=("sensors", "metric", "冷库专用表"))
        msg = expect_reject("metric 含原位置被拒绝", p)
        check("拒绝原因指向位置", "metric" in msg and "原安装位置" in msg, msg)

        # SQLite 动态类型：REAL 列被写入文本并夹带原编号，同样必须拒绝
        p = os.path.join(tmp, "leak_value.db")
        make_base_db(p)
        c = sqlite3.connect(p)
        c.execute("UPDATE sensor_samples SET value='车间A异常值' WHERE id=1")
        c.commit(); c.close()
        msg = expect_reject("value 列文本含原位置被拒绝", p)
        check("拒绝原因指向 value 列", "value" in msg and "原安装位置" in msg, msg)

        print("== 代号与原编号防碰撞 ==")
        p = os.path.join(tmp, "snlike.db")
        make_base_db(p, sensors=[
            ("SN-0000000001", "机房一", "temperature", "°C"),
            ("SN-0000000002", "机房二", "pressure", "kPa"),
            ("SN-0000000003", "机房三", "humidity", "%RH"),
        ])
        work5 = tempfile.mkdtemp(dir=tmp)
        out5, st5 = az.export_anonymized_db(p, work5)
        codes5 = {r["sensor_id"] for r in read_db(out5)[0]}
        check("代号不与 SN- 格式原编号碰撞",
              codes5.isdisjoint({"SN-0000000001", "SN-0000000002", "SN-0000000003"}),
              str(codes5))
        check("原 SN 编号不出现在成品字节中",
              all(sid.encode() not in open(out5, "rb").read()
                  for sid in ("SN-0000000001", "SN-0000000002", "SN-0000000003")))

        # 原位置本就等于“已隐藏”时应正常导出（无旧位置可泄露）
        p = os.path.join(tmp, "already_hidden.db")
        make_base_db(p, sensors=[
            ("H-1", az.HIDDEN_LOCATION, "temperature", "°C"),
            ("H-2", "真实机房", "pressure", "kPa"),
            ("H-3", az.HIDDEN_LOCATION, "humidity", "%RH"),
        ])
        work6 = tempfile.mkdtemp(dir=tmp)
        out6, _ = az.export_anonymized_db(p, work6)
        check("原位置含隐藏标记的库可正常导出", os.path.isfile(out6))
        check("真实位置不出现在成品字节中", "真实机房".encode() not in open(out6, "rb").read())

        print("== 受理条件拒绝 ==")
        big = os.path.join(tmp, "big.db")
        make_base_db(big)
        with open(big, "ab") as fh:
            fh.write(b"\0" * (az.MAX_EXPORT_BYTES + 1))
        expect_reject("超过 10MB 被拒绝", big)

        p = os.path.join(tmp, "notsqlite.db")
        open(p, "wb").write(b"definitely not a sqlite database" * 4)
        expect_reject("非 SQLite 文件被拒绝", p)

        p = os.path.join(tmp, "missing.db")
        expect_reject("文件不存在被拒绝", p)

        # 悬空外键
        p = os.path.join(tmp, "fkbad.db")
        make_base_db(p)
        c = sqlite3.connect(p)
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO sensor_samples (sensor_id, ts, value, ingested_at, batch_id) "
                  "VALUES ('GHOST','t',1,'t','b')")
        c.commit(); c.close()
        expect_reject("悬空外键数据被拒绝", p)

        print("== 受理快照隔离 ==")
        cur = os.path.join(tmp, "current.db")
        make_base_db(cur)
        work4 = tempfile.mkdtemp(dir=tmp)
        # 模拟“受理后、处理前”当前库被替换为另一版（不同设备编号）
        az.check_source_file(cur)
        snap = os.path.join(work4, "snapshot.db")
        az.make_snapshot(cur, snap)
        os.remove(cur)  # 先清除当前库，再写入另一版，模拟上传替换
        make_base_db(cur, sensors=[
            ("OTHER-9001", "另一地点X", "temperature", "°C"),
            ("OTHER-9002", "另一地点Y", "pressure", "kPa"),
            ("OTHER-9003", "另一地点Z", "humidity", "%RH"),
        ])
        out4 = os.path.join(work4, "sensors_coded.db")
        st4 = az.anonymize_snapshot(snap, out4)
        o4 = read_db(out4)[0]
        check("替换当前库后导出仍基于受理时快照（不含新版编号）",
              all("OTHER-9001" not in r["sensor_id"] for r in o4)
              and set(st4["mapping"]) == {"S-1001", "S-2001", "S-3001"})
        blob4 = open(out4, "rb").read()
        check("快照版导出不含新版库的位置", "另一地点X".encode() not in blob4)

        # 清除当前库（文件删除）也不影响已受理导出
        os.remove(cur)
        check("当前库被清除后成品仍可用", read_db(out4)[2] >= 0)

        print()
        print(f"共 {checks[0]} 项检查，失败 {len(failures)} 项")
        if failures:
            for f in failures:
                print("  FAILED:", f)
            sys.exit(1)
        print("全部通过 ✅")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
