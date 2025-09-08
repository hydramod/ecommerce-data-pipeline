# scripts/trino_bootstrap.py
import json, os, sys, time, urllib.request

TRINO = os.getenv("TRINO_URL", "http://trino:8080")
HDR   = {"X-Trino-User": os.getenv("TRINO_USER","admin")}
LAKE  = os.getenv("LAKEHOUSE_URI","s3a://delta-lake")

TABLES = [
    ("silver", "orders_clean",             f"{LAKE}/silver/orders_clean"),
    ("silver", "payments_clean",           f"{LAKE}/silver/payments_clean"),
    ("silver", "order_payments_enriched",  f"{LAKE}/silver/order_payments_enriched"),
]

def trino(sql, catalog=None, schema=None):
    h = dict(HDR)
    if catalog: h["X-Trino-Catalog"] = catalog
    if schema:  h["X-Trino-Schema"]  = schema
    req = urllib.request.Request(TRINO + "/v1/statement", data=sql.encode(), headers=h, method="POST")
    resp = json.load(urllib.request.urlopen(req))
    while resp.get("nextUri"):
        resp = json.load(urllib.request.urlopen(resp["nextUri"]))
    if resp.get("error"):
        raise RuntimeError(resp["error"]["message"])
    return resp.get("data", [])

def wait():
    for _ in range(120):
        try:
            info = json.load(urllib.request.urlopen(TRINO + "/v1/info"))
            if info and not info.get("starting"):
                return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit("Trino not ready in time")

def main():
    wait()
    catalogs = [r[0] for r in trino("SHOW CATALOGS")]
    if "delta" not in catalogs:
        raise SystemExit("No 'delta' catalog. Did you mount /etc/trino/catalog/delta.properties?")
    trino(f"CREATE SCHEMA IF NOT EXISTS delta.silver WITH (location = '{LAKE}/silver')", catalog="delta")
    for schema, name, path in TABLES:
        try:
            trino(
                f"CALL delta.system.register_table(schema_name => '{schema}', table_name => '{name}', table_location => '{path}')",
                catalog="delta", schema=schema
            )
        except Exception as e:
            if "Procedure not found" in str(e) or "Unknown function" in str(e):
                trino(f"CREATE TABLE IF NOT EXISTS {schema}.{name} WITH (location='{path}')", catalog="delta", schema=schema)
            else:
                raise
    # quick sanity check (non-fatal if tables are empty)
    for schema, name, _ in TABLES:
        try:
            cnt = trino(f"SELECT count(*) FROM {schema}.{name}", catalog="delta", schema=schema)[0][0]
            print(f"delta.{schema}.{name} => count={cnt}")
        except Exception as e:
            print(f"Check failed for {schema}.{name}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
