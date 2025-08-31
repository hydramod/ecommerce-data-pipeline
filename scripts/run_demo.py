#!/usr/bin/env python3
"""
run_demo.py - End-to-end demo for the e-commerce microservices stack
- Registers/logs in admin & customer
- Creates category/product, restocks inventory
- Customer adds to cart, checks out (creates shipment draft)
- Simulates payment success (advances shipment to READY_TO_SHIP)
- Dispatches shipment
- Prints notification emails from MailHog (if available)
"""

import requests
import json
import time
import os
import sys
from typing import Dict, Any, Optional, List

class DemoRunner:
    def __init__(self):
        # Get base URL from environment or use default
        self.base_url = os.getenv("DEMO_BASE_URL", "http://localhost")
        self.auth_url = f"{self.base_url}/auth"
        self.catalog_url = f"{self.base_url}/catalog"
        self.cart_url = f"{self.base_url}/cart"
        self.order_url = f"{self.base_url}/order"
        self.payment_url = f"{self.base_url}/payment"
        self.shipping_url = f"{self.base_url}/shipping"
        self.notifications_url = f"{self.base_url}/notifications"
        self.mailhog_api = os.getenv("MAILHOG_URL", "http://localhost:8025/api/v2/messages")

        # Health endpoints
        self.health_endpoints = {
            "auth": f"{self.auth_url}/health",
            "catalog": f"{self.catalog_url}/health",
            "cart": f"{self.cart_url}/health",
            "order": f"{self.order_url}/health",
            "payment": f"{self.payment_url}/health",
            "shipping": f"{self.shipping_url}/health",
            "notifications": f"{self.notifications_url}/health",
        }

        self.admin_email = os.getenv("DEMO_ADMIN_EMAIL", "admin@example.com")
        self.admin_pass = os.getenv("DEMO_ADMIN_PASS", "P@ssw0rd!")
        self.cust_email = os.getenv("DEMO_CUST_EMAIL", "cust@example.com")
        self.cust_pass = os.getenv("DEMO_CUST_PASS", "P@ssw0rd!")

        # Internal key (Catalog reserve)
        self.internal_key = os.getenv("SVC_INTERNAL_KEY", "devkey")

        # Tokens
        self.admin_access_token: Optional[str] = None
        self.cust_access_token: Optional[str] = None

    # ---------- helpers ----------
    def show_step(self, title: str):
        print(f"\n=== {title} ===")

    def mask_token(self, token: str) -> str:
        if not token:
            return "<none>"
        return token if len(token) <= 12 else f"{token[:8]}...{token[-6:]}"

    def call_api(
        self,
        method: str,
        url: str,
        headers: Optional[Dict] = None,
        data: Optional[Any] = None,
        expected_status: List[int] = [200, 201, 202, 204],
        quiet: bool = False,
        timeout: int = 30,
    ):
        if not quiet:
            print(f"\n-> {method} {url}")
            if headers:
                ph = headers.copy()
                if "Authorization" in ph:
                    tok = ph["Authorization"].replace("bearer ", "")
                    ph["Authorization"] = f"bearer {self.mask_token(tok)}"
                print(f"   Headers: {json.dumps(ph, indent=2)}")
            else:
                print("   Headers: <none>")
            if data is not None:
                print(f"   Body: {json.dumps(data, indent=2)}")

        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data if isinstance(data, (dict, list)) else None,
                timeout=timeout,
            )
            if not quiet:
                status_color = "\033[92m" if resp.status_code in expected_status else "\033[93m"
                print(f"   Status: {status_color}{resp.status_code}\033[0m")

            try:
                js = resp.json()
                if not quiet:
                    print("   JSON:")
                    print(json.dumps(js, indent=2))
                return {"status": resp.status_code, "data": js, "raw": resp.text}
            except json.JSONDecodeError:
                if resp.text and not quiet:
                    print("   Content:")
                    print(resp.text)
                return {"status": resp.status_code, "data": None, "raw": resp.text}
        except requests.exceptions.RequestException as e:
            if not quiet:
                print(f"   Error: \033[91m{e}\033[0m")
            return {"status": None, "data": None, "raw": None, "error": str(e)}

    # ---------- flow ----------
    def preflight_health_checks(self):
        self.show_step("Preflight: service health")
        all_healthy = True
        
        for svc, url in self.health_endpoints.items():
            result = self.call_api("GET", url, expected_status=[200], quiet=True)
            ok = result.get("status") == 200
            color = "\033[92m" if ok else "\033[91m"
            status = "OK" if ok else f"FAIL ({result.get('status')})"
            print(f"  - {svc.ljust(14)} -> {color}{status}\033[0m")
            if not ok:
                all_healthy = False

        # Optional: MailHog
        try:
            r = requests.get(self.mailhog_api, timeout=3)
            mh_ok = r.status_code == 200
        except requests.exceptions.RequestException:
            mh_ok = False
        color = "\033[92m" if mh_ok else "\033[93m"
        print(f"  - {'mailhog'.ljust(14)} -> {color}{'OK' if mh_ok else 'SKIP'}\033[0m")
        
        return all_healthy

    def run_demo(self):
        print("Starting E-commerce Microservices Demo")
        print("=" * 50)

        # Preflight
        if not self.preflight_health_checks():
            print("\n\033[91mSome services are not healthy. Demo may fail.\033[0m")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                print("Demo aborted.")
                return

        # 1) Admin register + login
        self.show_step("Admin: register")
        self.call_api(
            "POST",
            f"{self.auth_url}/register",
            data={"email": self.admin_email, "password": self.admin_pass, "role": "admin"},
            expected_status=[201, 409],
        )

        self.show_step("Admin: login")
        lr = self.call_api(
            "POST", f"{self.auth_url}/login", data={"email": self.admin_email, "password": self.admin_pass}
        )
        if lr.get("data"):
            self.admin_access_token = lr["data"].get("access_token")
            print(f"Admin access token: {self.mask_token(self.admin_access_token)}")

        # 2) Admin creates category + product
        admin_hdrs = {"Authorization": f"bearer {self.admin_access_token}"} if self.admin_access_token else {}

        self.show_step("Admin: create category")
        self.call_api(
            "POST",
            f"{self.catalog_url}/v1/categories/",
            headers=admin_hdrs,
            data={"name": "Shoes"},
            expected_status=[201, 409],
        )

        self.show_step("Admin: create product")
        self.call_api(
            "POST",
            f"{self.catalog_url}/v1/products/",
            headers=admin_hdrs,
            data={
                "title": "Air Zoom",
                "description": "Runner",
                "price_cents": 12999,
                "currency": "USD",
                "sku": "SKU-001",
                "category_id": 1,
                "active": True,
            },
            expected_status=[201, 409],
        )

        # Quick sanity
        self.show_step("Sanity: fetch product #1")
        self.call_api("GET", f"{self.catalog_url}/v1/products/1", expected_status=[200, 404])

        # 3) Restock inventory (internal)
        self.show_step("Admin: restock inventory (X-Internal-Key + Authorization)")
        int_hdrs = admin_hdrs.copy()
        int_hdrs["X-Internal-Key"] = self.internal_key
        self.call_api(
            "POST",
            f"{self.catalog_url}/v1/inventory/restock",
            headers=int_hdrs,
            data={"items": [{"product_id": 1, "qty": 50}]},
        )

        # 4) Customer register + login
        self.show_step("Customer: register")
        self.call_api(
            "POST",
            f"{self.auth_url}/register",
            data={"email": self.cust_email, "password": self.cust_pass},
            expected_status=[201, 409],
        )

        self.show_step("Customer: login")
        clr = self.call_api(
            "POST", f"{self.auth_url}/login", data={"email": self.cust_email, "password": self.cust_pass}
        )
        if clr.get("data"):
            self.cust_access_token = clr["data"].get("access_token")
            print(f"Customer access token: {self.mask_token(self.cust_access_token)}")

        # 5) Add to cart
        cust_hdrs = {"Authorization": f"bearer {self.cust_access_token}"} if self.cust_access_token else {}

        self.show_step("Customer: add to cart")
        cart_res = self.call_api(
            "POST",
            f"{self.cart_url}/v1/cart/items",
            headers=cust_hdrs,
            data={"product_id": 1, "qty": 2},
            expected_status=[200, 201, 404],
        )
        if cart_res.get("status") == 404:
            print(
                "\n\033[93mHint: Cart 404 often means cart cannot reach catalog. "
                "Check CATALOG_BASE for the cart container.\033[0m"
            )

        # 6) Checkout (with shipping address body)
        self.show_step("Customer: checkout (creates shipment draft)")
        shipping_body = {
            "address_line1": "1 Demo Street",
            "address_line2": "",
            "city": "Dublin",
            "country": "IE",
            "postcode": "D01XYZ",
        }
        co = self.call_api(
            "POST",
            f"{self.order_url}/v1/orders/checkout",
            headers=cust_hdrs,
            data=shipping_body,
            expected_status=[200, 201],
        )
        order_id = co.get("data", {}).get("order_id") if co else None
        amount = co.get("data", {}).get("total_cents") if co else None
        print(f"Order ID: {order_id}; Amount: {amount}")

        # 7) Shipping: wait for shipment draft (PENDING_PAYMENT)
        self.show_step("Shipping: wait for shipment draft (PENDING_PAYMENT)")
        shipment_id = None
        status = None
        for _ in range(30):  # up to ~15s
            time.sleep(0.5)
            q = self.call_api(
                "GET",
                f"{self.shipping_url}/v1/shipments?order_id={order_id}",
                expected_status=[200],
                quiet=True,
            )
            rows = (q or {}).get("data") or []
            if rows:
                shipment = rows[0] if isinstance(rows, list) else rows
                shipment_id = shipment.get("id")
                status = shipment.get("status")
                print(f"  - Shipment {shipment_id} status: {status}")
                if status in ("PENDING_PAYMENT", "READY_TO_SHIP"):
                    break
            else:
                print(f"  - No shipments found yet for order {order_id}")

        if not shipment_id:
            print("\033[93mNo shipment found for order; check Shipping logs.\033[0m")
            if q and q.get("data"):
                print(f"Shipping response: {q.get('data')}")


        # 8) Payment: mock succeed (after shipment exists to avoid race)
        self.show_step("Payment: mock succeed")
        if order_id and amount:
            payment_headers = {"Authorization": f"bearer {self.cust_access_token}"}
            self.call_api(
                "POST",
                f"{self.payment_url}/v1/payments/mock-succeed",
                headers=payment_headers,
                data={"order_id": order_id, "amount_cents": amount, "currency": "USD"},
            )
        else:
            print("Skipping payment - no order/amount")


       # 9) Shipping: wait for READY_TO_SHIP
        self.show_step("Shipping: wait for READY_TO_SHIP")
        for _ in range(30):  # up to ~15s
            time.sleep(0.5)
            q = self.call_api(
                "GET",
                f"{self.shipping_url}/v1/shipments?order_id={order_id}",
                expected_status=[200],
                quiet=True,
            )
            rows = (q or {}).get("data") or []
            if rows:
                shipment = rows[0] if isinstance(rows, list) else rows
                shipment_id = shipment.get("id") or shipment_id
                status = shipment.get("status")
                print(f"  - Shipment {shipment_id} status: {status}")
                if status == "READY_TO_SHIP":
                    break
            else:
                print(f"  - No shipments found yet for order {order_id}")


        # 10) Check order status
        time.sleep(1)
        self.show_step("Order: check status")
        if order_id:
            self.call_api("GET", f"{self.order_url}/v1/orders/{order_id}", expected_status=[200, 404])
        else:
            print("Skipping order status check - no order ID")

        # 11) Show last few emails from MailHog (optional)
        self.show_step("Notifications: fetch emails from MailHog (optional)")
        try:
            r = requests.get(self.mailhog_api + "?limit=5", timeout=5)
            if r.status_code == 200:
                data = r.json()
                total = data.get("total", 0)
                items = data.get("items", [])
                print(f"Found {total} emails. Showing up to 5 recent:")
                for i, m in enumerate(items, 1):
                    # Build "to" robustly (root .To or header "To")
                    to_field = m.get("To") or []
                    if to_field:
                        def as_email(x):
                            if isinstance(x, dict):
                                return f"{x.get('Mailbox','')}@{x.get('Domain','')}".strip('@')
                            return str(x)
                        to = ", ".join(as_email(x) for x in to_field)
                    else:
                        headers_to = (m.get("Content", {}).get("Headers", {}).get("To") or [])
                        if isinstance(headers_to, list):
                            to = ", ".join(headers_to)
                        elif isinstance(headers_to, str):
                            to = headers_to
                        else:
                            to = ""

                    # Subject can be a list[str] or a str in MailHog
                    subj_field = m.get("Content", {}).get("Headers", {}).get("Subject", [])
                    if isinstance(subj_field, list):
                        subj = subj_field[0] if subj_field else ""
                    elif isinstance(subj_field, str):
                        subj = subj_field
                    else:
                        subj = ""

                    print(f"  {i}. To: {to} | Subject: {subj}")
            else:
                print("MailHog not reachable or returned non-200.")
        except requests.exceptions.RequestException:
            print("MailHog not reachable. Skipping.")

        print("\n\033[92m=== DEMO COMPLETE ===\033[0m")


if __name__ == "__main__":
    # Check if requests is installed
    try:
        import requests
    except ImportError:
        print("Error: requests module is required. Install it with: pip install requests")
        sys.exit(1)
        
    DemoRunner().run_demo()