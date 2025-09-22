# fast_fix_ids_unique_match.py
# -*- coding: utf-8 -*-
import sqlite3
from settings import database_path
import re
import random

def split_into_sentences(text):
    """
    将 evidence 按句子分割，常见标点符号作为分隔符。
    """
    # 使用常见句号、问号、感叹号等符号分句
    sentences = re.split(r'[.!?。！？；;]', text)
    return [s.strip() for s in sentences if s.strip()]

def clean_and_split_evidence(evidence):
    """
    清理标点符号，保留字母数字空格，并切分为词列表。
    """
    cleaned = re.sub(r"[^\w\s]", " ", evidence)
    return cleaned.split()


def find_unique_partial_match(ev, candidates, rs_info_lookup=None):
    """
    ev: 当前 evidence 文本
    candidates: [(rsid, rid, contents, company_name, section_name)] 或 [(rsid, rid, contents)] 简单模式
    rs_info_lookup: 可选字典 {rsid: (company_name, section_name)}，如果 candidates 不包含公司信息时使用

    返回 (rsid, rid) 或 (None, None)
    """
    sentences = split_into_sentences(ev)
    if not sentences:
        return None, None

    # ---- Step 1: 逐句完整匹配 ----
    sentence_match_dict = {}  # {句子索引: 匹配到的候选集合}
    for idx, sentence in enumerate(sentences):
        words = clean_and_split_evidence(sentence)
        if len(words) < 6:  # 句子太短跳过
            continue

        matches = set()
        for item in candidates:
            if len(item) >= 5:
                rsid, rid, contents, comp, sec = item
            else:
                rsid, rid, contents = item
                comp, sec = rs_info_lookup.get(rsid, ("", "")) if rs_info_lookup else ("", "")
            if sentence in contents:
                matches.add((rsid, rid, comp, sec))
        sentence_match_dict[idx] = matches

        # 单句唯一 → 立即返回
        if len(matches) == 1:
            rsid, rid, _, _ = list(matches)[0]
            return rsid, rid

    # ---- Step 2: 相邻句交集逻辑 ----
    if len(sentence_match_dict) > 1:
        keys = sorted(sentence_match_dict.keys())
        for i in range(len(keys) - 1):
            first_set = sentence_match_dict[keys[i]]
            second_set = sentence_match_dict[keys[i + 1]]
            inter = first_set & second_set
            if len(inter) == 1:
                rsid, rid, _, _ = list(inter)[0]
                return rsid, rid

    # ---- Step 3: 相邻句交集无法唯一 → 检查并集 ----
    # 将所有句子的匹配结果取并集
    all_union = set()
    for s in sentence_match_dict.values():
        all_union |= s

    if len(all_union) > 1:
        companies = {comp for _, _, comp, _ in all_union}
        sections = {sec for _, _, _, sec in all_union}
        # 如果所有候选属于同一个公司和同一个 section_name，则随机返回其中一个
        if len(companies) == 1 and len(sections) == 1:
            chosen = random.choice(list(all_union))
            rsid, rid, _, _ = chosen
            return rsid, rid

    # ---- Step 4: 滑动6词窗口兜底 ----
    match_results = set()
    for sentence in sentences:
        words = clean_and_split_evidence(sentence)
        if len(words) < 6:
            continue
        for i in range(len(words) - 5):
            segment = " ".join(words[i:i + 6])
            for item in candidates:
                if len(item) >= 5:
                    rsid, rid, contents, _, _ = item
                else:
                    rsid, rid, contents = item
                if segment in contents:
                    match_results.add((rsid, rid))
            if len(match_results) > 1:
                return None, None  # 仍不唯一则放弃

    if len(match_results) == 1:
        return list(match_results)[0]
    return None, None


def build_indexes(conn):
    # report_id -> company_name
    rep_company = dict(conn.execute(
        "SELECT id, company_name FROM report"
    ).fetchall())

    # section_name -> [(rsid, rid, contents)]
    rs_dict = {}
    for rsid, rid, sname, contents in conn.execute(
        "SELECT id, report_id, section_name, contents FROM report_sections"
    ):
        rs_dict.setdefault(sname, []).append((rsid, rid, contents or ""))
    return rs_dict, rep_company

import random

def choose_if_same_company(pairs, rep_company):
    """
    pairs: iterable of (rsid, rid)
    rep_company: {rid: company_name}
    若所有 rid 映射的公司名一致 -> 随机返回其中一个 (rsid, rid)，否则 None
    """
    pairs = list(pairs)
    if not pairs:
        return None
    companies = {rep_company.get(rid) for _, rid in pairs}
    if len(companies) == 1:
        return random.choice(pairs)
    return None


def match_by_value_first(value, period, rs_candidates, rep_company):
    if value is None:
        return None, None
    val = str(value).strip()
    if not val:
        return None, None

        # ---------- Step 1: value + period 联合匹配 ----------
    hits = []
    if period:
        period_str = str(period).strip()
        if period_str:
            hits = [
                (rsid, rid)
                for rsid, rid, contents in rs_candidates
                if val in contents and period_str in contents
            ]

    # ---------- Step 2: 如果联合匹配失败，退回只用 value ----------
    if not hits:
        hits = [(rsid, rid) for rsid, rid, contents in rs_candidates if val in contents]

    if not hits:
        return None, None

    uniq = set(hits)
    if len(uniq) == 1:
        return next(iter(uniq))

    # ---------- Step 3: 多条匹配，检查是否同公司 ----------
    picked = choose_if_same_company(uniq, rep_company)
    return picked if picked else (None, None)



def build_rs_indexes(conn):
    """
    返回两个索引：
    - rs_dict: {section_name: [(id, report_id, contents), ...]}
    - rs_by_id: {rs_id: (report_id, section_name, contents)}
    """
    rs_dict = {}
    rs_by_id = {}
    cur = conn.execute("SELECT id, report_id, section_name, contents FROM report_sections")
    for rsid, rid, sname, contents in cur.fetchall():
        contents = contents or ""
        rs_by_id[rsid] = (rid, sname, contents)
        rs_dict.setdefault(sname, []).append((rsid, rid, contents))
    return rs_dict, rs_by_id

def find_and_remove_match(evidence, candidates):
    """
    在候选列表中找到第一个匹配记录，返回并从列表中移除，保证唯一使用。
    """
    for idx, (rsid, rid, contents) in enumerate(candidates):
        if evidence in contents:
            return rsid, rid
    return None, None

def is_already_correct(cur_sid, cur_rid, rs_by_id):
    """
    只判断当前记录的 section_id 和 report_id 是否在主表中有效
    """
    if not cur_sid or cur_sid not in rs_by_id:
        return False
    rs_rid, _, _ = rs_by_id[cur_sid]
    # report_id 对应得上
    return cur_rid == rs_rid

def process_table(conn, table, rs_dict, rs_by_id):
    """
    处理 event / factor / variable 表。
    - 首先：若已有 section_id，则用它纠正 report_id（以 report_sections 为准）
    - variable 表优先用 value(+period) 匹配；失败再用 evidence 匹配
    - 每10行提交并打印进度
    """
    # 映射：report_id -> company_name（用于“同公司”判断）
    rep_company = dict(conn.execute("SELECT id, company_name FROM report").fetchall())

    # 动态选择查询列
    if table == "variable":
        cur = conn.execute(
            f"SELECT id, section_name, evidence, value, period, section_id, report_id FROM {table}"
        )
    else:
        cur = conn.execute(
            f"SELECT id, section_name, evidence, section_id, report_id FROM {table}"
        )

    rows = cur.fetchall()

    total = len(rows)
    matched = 0
    unmatched = 0
    already_ok = 0
    skipped = 0
    value_matched = 0
    fixed_by_sid = 0   # 统计：通过 section_id 修正了 report_id 的行数

    update_cur = conn.cursor()

    for idx, row in enumerate(rows, start=1):
        # 解包
        if table == "variable":
            row_id, sname, evidence, value, period, cur_sid, cur_rid = row
        else:
            row_id, sname, evidence, cur_sid, cur_rid = row
            value, period = None, None

        ev = (evidence or "").strip()
        sname_norm = (sname or "").strip()

        # ---------- Step 0: 若已有 section_id，先用它纠正 report_id ----------
        if cur_sid and cur_sid in rs_by_id:
            rs_true_rid = rs_by_id[cur_sid][0]  # report_sections 中该 section 的 report_id
            if cur_rid != rs_true_rid:
                update_cur.execute(
                    f"UPDATE {table} SET report_id = ? WHERE id = ?",
                    (rs_true_rid, row_id)
                )
                cur_rid = rs_true_rid
                fixed_by_sid += 1

        # ---------- Step 1: 已经正确就跳过（section_id 有且与主表对应的 report_id 一致） ----------
        if cur_sid and cur_sid in rs_by_id and cur_rid == rs_by_id[cur_sid][0]:
            already_ok += 1
            # 已经都有正确的 section_id / report_id，则无需再匹配
            continue

        # ---------- Step 2: 无候选则跳过 ----------
        if sname_norm not in rs_dict or not rs_dict[sname_norm]:
            skipped += 1
            continue

        # ---------- Step 3: variable 表先用 value(+period) 匹配 ----------
        rsid = rid = None
        if table == "variable" and value is not None:
            # 注意：match_by_value_first(value, rs_candidates, rep_company, period=None)
            rsid, rid = match_by_value_first(value,period, rs_dict[sname_norm], rep_company)
            if rsid is not None:
                value_matched += 1

        # ---------- Step 4: 若 value 失败，再用 evidence（句子/交叉/窗口） ----------
        if rsid is None and ev:
            rsid, rid = find_unique_partial_match(ev, rs_dict[sname_norm])

        # ---------- Step 5: 更新或记为未匹配 ----------
        if rsid is not None:
            update_cur.execute(
                f"UPDATE {table} SET section_id = ?, report_id = ? WHERE id = ?",
                (rsid, rid, row_id)
            )
            matched += 1
        else:
            unmatched += 1
            print(f"[UNMATCHED] {table} | RowID={row_id} | Value='{value}' | Period='{period}' | Evidence='{ev[:80]}'")

        # ---------- Step 6: 每10行提交并打印进度 ----------
        if idx % 10 == 0:
            conn.commit()
            print(
                f"[{table}] 已处理 {idx}/{total} 行... "
                f"先修正report_id={fixed_by_sid}, 已正确={already_ok}, "
                f"Value匹配={value_matched}, 更新={matched}, 未匹配={unmatched}, 跳过={skipped}"
            )

    conn.commit()
    print(
        f"[{table}] 完成: 总={total}, 先修正report_id={fixed_by_sid}, 已正确={already_ok}, "
        f"Value匹配={value_matched}, 更新={matched}, 未匹配={unmatched}, 跳过={skipped}"
    )



def fix_all_report_ids(conn):
    """
    统一修正三张表(event, factor, variable)中report_id的值。
    通过section_id到report_sections表查询真实report_id，如果不一致则更新。
    """
    # 构建 {section_id: report_id}
    cur = conn.execute("SELECT id, report_id FROM report_sections")
    rs_by_id = {row[0]: row[1] for row in cur.fetchall()}

    # 需要修正的三张表
    tables = ["event", "factor", "variable"]
    total_fixed = 0

    update_cur = conn.cursor()

    for table in tables:
        # 查询当前表的所有 section_id 和 report_id
        cur = conn.execute(f"SELECT id, section_id, report_id FROM {table} WHERE section_id IS NOT NULL")
        rows = cur.fetchall()

        fixed_count = 0
        for row_id, section_id, current_rid in rows:
            if section_id in rs_by_id:
                true_rid = rs_by_id[section_id]
                # 只有当当前report_id和真实值不一致时才更新
                if current_rid != true_rid:
                    update_cur.execute(
                        f"UPDATE {table} SET report_id = ? WHERE id = ?",
                        (true_rid, row_id)
                    )
                    fixed_count += 1

        conn.commit()
        total_fixed += fixed_count
        print(f"[{table}] 修正完成: 共 {len(rows)} 行, 修正 {fixed_count} 行。")

    print(f"全部修正完成，总共修正 {total_fixed} 行。")



def main():
    conn = sqlite3.connect(database_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    fix_all_report_ids(conn)
    # Step 1: 加载 report_sections
    print("加载 report_sections 到内存中...")
    rs_dict, rs_by_id = build_rs_indexes(conn)
    total_rs = sum(len(v) for v in rs_dict.values())
    print(f"已加载 {total_rs} 条 report_sections 记录.\n")

    # Step 2: 逐表处理
    for tbl in ["event", "factor", "variable"]:
        print(f"开始处理表: {tbl}")
        process_table(conn, tbl, rs_dict, rs_by_id)

    conn.close()
    print("所有表处理完成.")

if __name__ == "__main__":
    main()
