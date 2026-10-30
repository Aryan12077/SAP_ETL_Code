"""
Which part of the failing read creates the duplicate keys?  (read-only, SAP only)

The plain key-only read matches SE16N exactly, so we compare four reads of the same table:
    A  keys only          no WHERE        (known good)
    B  keys + columns     no WHERE
    C  keys only          WHERE range windows
    D  keys + columns     WHERE range windows   (what the extractor does)

    python diagnose_duplicates.py --table MAKT --expected 51028
    python diagnose_duplicates.py --table MARA --expected 51028
Output: console + diagnose_<table>.txt  (send me the file)
"""
import argparse, logging, os, sys, time
from collections import Counter, defaultdict

_d = os.path.dirname(os.path.abspath(__file__))
for _p in (_d, os.path.dirname(_d), os.path.dirname(os.path.dirname(_d))):
    if _p not in sys.path:
        sys.path.append(_p)
from sap_material_master_fixed import SAPConnection

ap = argparse.ArgumentParser()
ap.add_argument("--table", default="MAKT")
ap.add_argument("--matnr", default="COENGTOL0080")
ap.add_argument("--expected", type=int, help="row count from SE16N")
ap.add_argument("--window", type=int, default=20000)
ap.add_argument("--fm", default="BBP_RFC_READ_TABLE")
a = ap.parse_args()
T = a.table.upper()
KEYS = {"MAKT": ["MANDT", "MATNR", "SPRAS"], "MARA": ["MANDT", "MATNR"],
        "MARC": ["MANDT", "MATNR", "WERKS"]}[T]
EXTRA = {"MAKT": ["MAKTX", "MAKTG"], "MARA": ["ERSDA", "ERNAM", "LAEDA", "MTART", "MATKL", "MEINS"],
         "MARC": ["EKGRP", "MAABC", "DISMM"]}[T]
FULL = KEYS + EXTRA

out = open(f"diagnose_{T.lower()}.txt", "w", encoding="utf-8")
def P(*x):
    s = " ".join(str(i) for i in x); print(s, flush=True); out.write(s + "\n")

log = logging.getLogger("diag"); log.addHandler(logging.NullHandler())
sap = SAPConnection("HANA", log)
q = lambda s: s.replace("'", "''")

def call(fields, where=None, rowcount=0):
    r = sap.conn.call(a.fm, QUERY_TABLE=T, FIELDS=[{"FIELDNAME": f} for f in fields],
                      OPTIONS=[{"TEXT": w} for w in (where or [])], ROWCOUNT=rowcount, ROWSKIPS=0)
    lay = [(m["FIELDNAME"].strip(), int(m["OFFSET"]), int(m["LENGTH"])) for m in r["FIELDS"]]
    return [(rec["WA"], {n: rec["WA"][o:o + l] for n, o, l in lay}) for rec in r["DATA"]]

key_of = lambda d: tuple(d[k].strip() for k in KEYS)
surplus = lambda c: sum(v - 1 for v in c.values() if v > 1)

def range_where(lo, hi):
    return [f"MATNR >= '{q(lo)}'", f"AND MATNR <= '{q(hi)}'"]

def run(fields, wins=None, variant=None):
    t, calls, total, seen, sur, bad = time.time(), 0, 0, set(), 0, None
    for w in (wins or [None]):
        if w is None:
            where = None
        elif variant == "between":
            where = [f"MATNR BETWEEN '{q(w[0])}' AND '{q(w[1])}'"]
        else:
            where = range_where(*w)
        rr = call(fields, where); calls += 1
        c = Counter(key_of(d) for _, d in rr)
        total += len(rr); seen |= set(c); sur += surplus(c)
        if surplus(c) and bad is None: bad = (w, rr)
    return dict(calls=calls, total=total, sur=sur, distinct=len(seen), secs=time.time() - t, bad=bad)

def show(label, r):
    P(f"    {label:<34} {r['calls']:>3} calls | {r['total']:>7} rows | {r['sur']:>5} surplus dup rows | "
      f"{r['distinct']:>7} distinct keys | {r['secs']:.0f}s")

P(f"=== {T} via {a.fm} ===")
scan = call(KEYS)
sc = Counter(key_of(d) for _, d in scan)
P(f"[0] A: unfiltered key scan: {len(scan)} rows, {len(sc)} distinct keys, {surplus(sc)} surplus"
  + (f" | SE16N says {a.expected}" if a.expected else ""))

per_matnr = Counter(k[1] for k in sc)
wins, first, last, n = [], None, None, 0
for v in sorted(per_matnr):
    if n and n + per_matnr[v] > a.window:
        wins.append((first, last)); first, n = None, 0
    if first is None: first = v
    last = v; n += per_matnr[v]
if n: wins.append((first, last))
P(f"    windows of <= {a.window} rows: {len(wins)}; first = {wins[0]}")

P("\n[1] the 2x2 matrix  (surplus dup rows = same key returned more than once)")
res = {}
res["A"] = run(KEYS);              show("A keys only, no WHERE", res["A"])
res["B"] = run(FULL);              show("B keys+columns, no WHERE", res["B"])
res["C"] = run(KEYS, wins);        show("C keys only, WHERE windows", res["C"])
res["D"] = run(FULL, wins);        show("D keys+columns, WHERE windows", res["D"])

bad = res["D"]["bad"] or res["C"]["bad"]
P("\n[2] first window that returned duplicates")
if not bad:
    P("    none: no duplicates in C or D")
else:
    w, rr = bad
    P("    window:", w)
    pos = defaultdict(list)
    c = Counter(key_of(d) for _, d in rr)
    for i, (wa, d) in enumerate(rr):
        if c[key_of(d)] > 1: pos[key_of(d)].append((i, wa))
    for k, v in list(pos.items())[:6]:
        same = len({wa for _, wa in v}) == 1
        P(f"    key {k}: positions {[i for i, _ in v]} | rows byte-identical: {same} | "
          f"is window lo/hi: {k[1] == w[0]}/{k[1] == w[1]} | rows in response: {len(rr)}")
    P("    duplicated keys in this window:", len(pos),
      "| position gaps:", Counter(v[1][0] - v[0][0] for v in pos.values()).most_common(4))

P(f"\n[3] narrow reads around {a.matnr} (keys+columns)")
for label, where in (("MATNR = x", [f"MATNR = '{q(a.matnr)}'"]),
                     ("MATNR LIKE prefix%", [f"MATNR LIKE '{q(a.matnr[:8])}%'"])):
    rr = call(FULL, where)
    c = Counter(key_of(d) for _, d in rr)
    P(f"    {label:<20} {len(rr)} rows, {surplus(c)} surplus | raw:", [wa[:60] for wa, _ in rr[:3]])

P("\n[4] same first window, different WHERE spelling (keys+columns)")
w0 = [wins[0]]
for label, var in (("two lines: >= / AND <=", None), ("one line: BETWEEN", "between")):
    try: show(label, run(FULL, w0, var))
    except Exception as e: P(f"    {label}: failed -> {str(e)[:120]}")

P("\n[5] repeatability of the first window (3 calls)")
sets = []
for _ in range(3):
    c = Counter(key_of(d) for _, d in call(FULL, range_where(*wins[0])))
    sets.append(frozenset(k for k, v in c.items() if v > 1)); P(f"    duplicated keys: {len(sets[-1])}")
P("    same duplicated keys every time:", len(set(sets)) == 1)

P("""
HOW TO READ
  A ok, B ok, C ok, D dup   -> extra columns + WHERE together trigger it
  A ok, B dup               -> asking for more columns is enough (no WHERE needed)
  C dup                     -> the WHERE range itself duplicates rows (compare [4] spellings)
  [2] identical rows + fixed position gaps -> function repeats rows at internal boundaries
  distinct keys below the A figure          -> rows are being LOST as well""")
out.close(); sap.close()