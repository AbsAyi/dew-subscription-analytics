"""
Synthetic Data Generator for DTC Subscription Analytics Portfolio Project
=========================================================================
Generates realistic multi-platform eCommerce data for a fictional DTC skincare brand
called "Dew" (subscription skincare, think Rogaine/Curology/Hims model).

Sources simulated:
  - Shopify Orders (raw_shopify.orders, raw_shopify.order_line_items, raw_shopify.customers)
  - Recharge Subscriptions (raw_recharge.subscriptions, raw_recharge.charges, raw_recharge.customers)
  - Stripe Payments (raw_stripe.charges, raw_stripe.refunds)

Intentional data quality issues embedded (the kind you'd actually find):
  - Duplicate order IDs from webhook retries
  - Refund records that don't match to orders (timing/system lag)
  - GA4-style attribution nulls
  - Subscription status mismatches between Shopify and Recharge
  - Payment failure records in Stripe with no corresponding Recharge update
  - Timezone inconsistencies between sources
"""

import csv
import random
import os
from datetime import datetime, timedelta
from collections import defaultdict

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "seeds")

# ============================================================
# CONFIG
# ============================================================
NUM_CUSTOMERS = 1200
DATE_START = datetime(2024, 1, 1)
DATE_END = datetime(2025, 12, 31)
BRAND_NAME = "Dew Skincare"

PRODUCTS = [
    {"id": "PROD_001", "title": "Daily Hydration Serum", "variant": "30-day supply", "price": 34.99, "sku": "DHS-30"},
    {"id": "PROD_002", "title": "Daily Hydration Serum", "variant": "90-day supply", "price": 89.99, "sku": "DHS-90"},
    {"id": "PROD_003", "title": "Renewal Night Cream", "variant": "30-day supply", "price": 44.99, "sku": "RNC-30"},
    {"id": "PROD_004", "title": "Renewal Night Cream", "variant": "90-day supply", "price": 109.99, "sku": "RNC-90"},
    {"id": "PROD_005", "title": "Brightening Vitamin C Drops", "variant": "30-day supply", "price": 29.99, "sku": "BVC-30"},
    {"id": "PROD_006", "title": "Complete Routine Bundle", "variant": "30-day supply", "price": 89.99, "sku": "CRB-30"},
    {"id": "PROD_007", "title": "Complete Routine Bundle", "variant": "90-day supply", "price": 229.99, "sku": "CRB-90"},
]

# Weight toward 30-day products (more common), 90-day = higher LTV (your Rogaine story)
PRODUCT_WEIGHTS = [0.25, 0.10, 0.20, 0.08, 0.15, 0.15, 0.07]

CHANNELS = ["google_paid", "meta_paid", "tiktok_paid", "organic_search", "direct", "email", "referral", "influencer"]
CHANNEL_WEIGHTS = [0.22, 0.20, 0.08, 0.18, 0.12, 0.10, 0.06, 0.04]

US_STATES = ["NJ", "NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI", "WA", "AZ", "MA", "CO",
             "VA", "TN", "OR", "MN", "MD", "WI", "MO", "IN", "CT", "SC"]

FIRST_NAMES = ["Emma", "Liam", "Olivia", "Noah", "Ava", "Sophia", "Jackson", "Isabella", "Lucas",
               "Mia", "Aiden", "Harper", "Elijah", "Amelia", "James", "Evelyn", "Benjamin", "Abigail",
               "Mason", "Emily", "Logan", "Elizabeth", "Alexander", "Sofia", "Ethan", "Avery", "Jacob",
               "Ella", "Michael", "Scarlett", "Daniel", "Grace", "Henry", "Chloe", "Sebastian", "Victoria",
               "Mateo", "Riley", "Owen", "Aria", "Camila", "Jayden", "Penelope", "Layla", "Nathan",
               "Zoey", "Nora", "Lily", "Aaliyah", "Savannah"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
              "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor",
              "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez",
              "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright",
              "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson", "Baker",
              "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts"]


def random_date(start, end):
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86399)
    return start + timedelta(days=random_days, seconds=random_seconds)


def generate_email(first, last, cust_id):
    domains = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "hotmail.com"]
    sep = random.choice([".", "_", ""])
    num = random.choice(["", str(random.randint(1, 99))])
    return f"{first.lower()}{sep}{last.lower()}{num}@{random.choice(domains)}"


def write_csv(filename, rows, headers):
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓ {filename}: {len(rows)} rows")


# ============================================================
# GENERATE CUSTOMERS
# ============================================================
print("\n🧪 Generating synthetic data for Dew Skincare...\n")

customers = []
shopify_customers = []
recharge_customers = []

for i in range(1, NUM_CUSTOMERS + 1):
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    cust_id = f"CUST_{i:05d}"
    email = generate_email(first, last, cust_id)
    state = random.choice(US_STATES)
    created = random_date(DATE_START, DATE_END - timedelta(days=60))

    # Simulate Shopify customer ID and Recharge customer ID being different
    shopify_cust_id = 7000000000 + i
    recharge_cust_id = 9000000000 + i

    customers.append({
        "internal_id": cust_id,
        "shopify_customer_id": shopify_cust_id,
        "recharge_customer_id": recharge_cust_id,
        "first_name": first,
        "last_name": last,
        "email": email,
        "state": state,
        "created_at": created,
        "channel": random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0],
    })

    shopify_customers.append({
        "id": shopify_cust_id,
        "email": email,
        "first_name": first,
        "last_name": last,
        "state": state,
        "created_at": created.strftime("%Y-%m-%dT%H:%M:%S-05:00"),  # EST
        "orders_count": 0,  # will update
        "total_spent": "0.00",  # will update
        "tags": "",
        "accepts_marketing": random.choice(["true", "true", "true", "false"]),
    })

    # Recharge sometimes has slightly different timestamps (UTC vs EST mismatch)
    recharge_offset = timedelta(hours=random.choice([0, 0, 0, 5]))  # sometimes UTC, sometimes EST
    recharge_customers.append({
        "id": recharge_cust_id,
        "shopify_customer_id": shopify_cust_id,
        "email": email,
        "first_name": first,
        "last_name": last,
        "created_at": (created + recharge_offset).strftime("%Y-%m-%dT%H:%M:%SZ"),  # UTC
        "status": "active",
        "has_payment_method_in_dunning": "false",
    })


# ============================================================
# GENERATE ORDERS + SUBSCRIPTIONS + PAYMENTS
# ============================================================
shopify_orders = []
shopify_line_items = []
recharge_subscriptions = []
recharge_charges = []
stripe_charges = []
stripe_refunds = []

order_counter = 5000000000
subscription_counter = 3000000000
charge_counter = 1000000000
stripe_charge_counter = 0

for cust in customers:
    created = cust["created_at"]
    shopify_id = cust["shopify_customer_id"]
    recharge_id = cust["recharge_customer_id"]
    email = cust["email"]
    state = cust["state"]
    channel = cust["channel"]

    # Decide customer behavior archetype
    archetype = random.choices(
        ["one_and_done", "short_sub", "loyal_sub", "high_value"],
        weights=[0.30, 0.30, 0.25, 0.15],
        k=1
    )[0]

    if archetype == "one_and_done":
        num_orders = 1
        is_subscriber = False
    elif archetype == "short_sub":
        num_orders = random.randint(2, 4)
        is_subscriber = True
    elif archetype == "loyal_sub":
        num_orders = random.randint(5, 12)
        is_subscriber = True
    else:  # high_value
        num_orders = random.randint(8, 20)
        is_subscriber = True

    # Pick primary product (higher value customers lean toward 90-day / bundles)
    if archetype == "high_value":
        product = random.choices(PRODUCTS, weights=[0.10, 0.20, 0.10, 0.15, 0.05, 0.15, 0.25], k=1)[0]
    else:
        product = random.choices(PRODUCTS, weights=PRODUCT_WEIGHTS, k=1)[0]

    # Subscription setup
    sub_id = None
    sub_interval_days = 30 if "30-day" in product["variant"] else 90

    if is_subscriber:
        subscription_counter += 1
        sub_id = subscription_counter

        # Determine subscription end state
        if archetype == "short_sub":
            sub_status = random.choice(["cancelled", "cancelled", "expired"])
            cancel_reason = random.choice(["too_expensive", "not_seeing_results", "switched_product", "customer_request", None])
        elif archetype == "loyal_sub":
            sub_status = random.choice(["active", "active", "active", "cancelled"])
            cancel_reason = None if sub_status == "active" else random.choice(["customer_request", "switched_product"])
        else:
            sub_status = random.choice(["active", "active", "active", "active", "cancelled"])
            cancel_reason = None if sub_status == "active" else "customer_request"

        cancelled_at = ""
        if sub_status in ("cancelled", "expired"):
            cancel_date = created + timedelta(days=sub_interval_days * num_orders + random.randint(0, 15))
            if cancel_date > DATE_END:
                cancel_date = DATE_END - timedelta(days=random.randint(1, 30))
            cancelled_at = cancel_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        recharge_subscriptions.append({
            "id": sub_id,
            "customer_id": recharge_id,
            "shopify_customer_id": shopify_id,
            "email": email,
            "product_title": product["title"],
            "variant_title": product["variant"],
            "sku": product["sku"],
            "price": f"{product['price']:.2f}",
            "quantity": 1,
            "status": sub_status,
            "created_at": (created + timedelta(minutes=random.randint(1, 30))).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "cancelled_at": cancelled_at,
            "cancellation_reason": cancel_reason if cancel_reason else "",
            "charge_interval_frequency": sub_interval_days,
            "order_interval_unit": "day",
            "next_charge_scheduled_at": "" if sub_status != "active" else (
                created + timedelta(days=sub_interval_days * num_orders)
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    # Generate orders
    order_total_spent = 0.0
    orders_for_customer = 0

    for order_num in range(num_orders):
        order_counter += 1
        order_date = created + timedelta(days=sub_interval_days * order_num + random.randint(0, 3))
        if order_date > DATE_END:
            break

        orders_for_customer += 1
        order_id = order_counter
        order_number = 10000 + order_counter - 5000000000

        # Price with occasional discount
        base_price = product["price"]
        discount = 0.0
        discount_code = ""
        if order_num == 0 and random.random() < 0.40:
            discount = round(base_price * random.choice([0.10, 0.15, 0.20, 0.25]), 2)
            discount_code = random.choice(["WELCOME10", "WELCOME15", "WELCOME20", "FIRST25", "TRYDEW"])
        elif order_num > 0 and random.random() < 0.10:
            discount = round(base_price * 0.10, 2)
            discount_code = random.choice(["LOYAL10", "COMEBACK10", "SAVE10"])

        subtotal = round(base_price - discount, 2)
        tax = round(subtotal * random.choice([0.0, 0.06625, 0.07, 0.08, 0.08875]), 2)
        shipping = 0.0 if (is_subscriber or subtotal > 50) else random.choice([0.0, 4.99, 7.99])
        total = round(subtotal + tax + shipping, 2)
        order_total_spent += total

        # Determine financial status
        financial_status = "paid"
        fulfillment_status = "fulfilled"

        # Some payment failures (especially later orders)
        is_payment_failure = False
        if order_num > 2 and random.random() < 0.08:
            is_payment_failure = True
            financial_status = "pending"
            fulfillment_status = "unfulfilled"

        # Some refunds
        is_refunded = False
        refund_amount = 0.0
        if not is_payment_failure and random.random() < 0.05:
            is_refunded = True
            if random.random() < 0.6:
                refund_amount = total  # full refund
                financial_status = "refunded"
            else:
                refund_amount = round(total * random.uniform(0.2, 0.5), 2)  # partial
                financial_status = "partially_refunded"

        # Attribution - sometimes null (the real-world GA4 problem)
        utm_source = channel.split("_")[0] if "_" in channel else channel
        utm_medium = channel.split("_")[1] if "_" in channel else "none"
        if order_num > 0:
            # Repeat orders often lose attribution
            if random.random() < 0.60:
                utm_source = ""
                utm_medium = ""

        # Timezone: Shopify stores in shop timezone (EST)
        order_timestamp_est = order_date.strftime("%Y-%m-%dT%H:%M:%S-05:00")

        source_name = "subscription_contract" if (is_subscriber and order_num > 0) else "web"

        shopify_orders.append({
            "id": order_id,
            "order_number": order_number,
            "customer_id": shopify_id,
            "email": email,
            "created_at": order_timestamp_est,
            "financial_status": financial_status,
            "fulfillment_status": fulfillment_status,
            "subtotal_price": f"{subtotal:.2f}",
            "total_tax": f"{tax:.2f}",
            "total_shipping": f"{shipping:.2f}",
            "total_price": f"{total:.2f}",
            "total_discounts": f"{discount:.2f}",
            "discount_codes": discount_code,
            "source_name": source_name,
            "landing_site": f"/?utm_source={utm_source}&utm_medium={utm_medium}" if utm_source else "/",
            "referring_site": "",
            "cancel_reason": "",
            "cancelled_at": "",
            "tags": "subscription" if is_subscriber else "",
            "billing_address_province": state,
        })

        # Line items
        shopify_line_items.append({
            "id": order_id * 10 + 1,
            "order_id": order_id,
            "product_id": product["id"],
            "title": product["title"],
            "variant_title": product["variant"],
            "sku": product["sku"],
            "quantity": 1,
            "price": f"{base_price:.2f}",
            "total_discount": f"{discount:.2f}",
        })

        # Occasionally add a second product (cross-sell)
        if random.random() < 0.12 and order_num > 1:
            addon = random.choice([p for p in PRODUCTS if p["id"] != product["id"] and "Bundle" not in p["title"]])
            addon_price = addon["price"]
            shopify_line_items.append({
                "id": order_id * 10 + 2,
                "order_id": order_id,
                "product_id": addon["id"],
                "title": addon["title"],
                "variant_title": addon["variant"],
                "sku": addon["sku"],
                "quantity": 1,
                "price": f"{addon_price:.2f}",
                "total_discount": "0.00",
            })

        # Recharge charge record
        if is_subscriber:
            charge_counter += 1

            # Recharge timestamps in UTC (vs Shopify in EST) — the real-world mismatch
            charge_timestamp = (order_date + timedelta(hours=5, minutes=random.randint(0, 15))).strftime("%Y-%m-%dT%H:%M:%SZ")

            charge_status = "SUCCESS"
            charge_error = ""
            if is_payment_failure:
                charge_status = random.choice(["ERROR", "DECLINED", "DECLINED"])
                charge_error = random.choice([
                    "Card declined",
                    "Insufficient funds",
                    "Card expired",
                    "Payment method requires update",
                ])

            recharge_charges.append({
                "id": charge_counter,
                "subscription_id": sub_id,
                "customer_id": recharge_id,
                "shopify_order_id": order_id if not is_payment_failure else "",  # failed charges often have no Shopify order
                "email": email,
                "created_at": charge_timestamp,
                "processed_at": charge_timestamp if charge_status == "SUCCESS" else "",
                "type": "RECURRING" if order_num > 0 else "CHECKOUT",
                "status": charge_status,
                "total_price": f"{total:.2f}",
                "subtotal_price": f"{subtotal:.2f}",
                "tax_lines": f"{tax:.2f}",
                "discount_codes": discount_code,
                "error_message": charge_error,
                "payment_processor": "stripe",
            })

        # Stripe charge
        stripe_charge_counter += 1
        stripe_charge_id = f"ch_{stripe_charge_counter:012d}"

        stripe_status = "succeeded"
        if is_payment_failure:
            stripe_status = "failed"

        # Stripe timestamps in UTC
        stripe_timestamp = (order_date + timedelta(hours=5, minutes=random.randint(0, 5))).strftime("%Y-%m-%dT%H:%M:%SZ")

        stripe_charges.append({
            "id": stripe_charge_id,
            "amount": int(total * 100),  # Stripe uses cents
            "amount_refunded": int(refund_amount * 100) if is_refunded else 0,
            "currency": "usd",
            "status": stripe_status,
            "created": stripe_timestamp,
            "customer_email": email,
            "metadata_shopify_order_id": order_id if not is_payment_failure else "",
            "metadata_recharge_charge_id": charge_counter if is_subscriber else "",
            "payment_method_type": random.choice(["card", "card", "card", "apple_pay", "google_pay"]),
            "failure_code": random.choice(["card_declined", "insufficient_funds", "expired_card", ""]) if is_payment_failure else "",
            "failure_message": "",
        })

        # Stripe refund record
        if is_refunded:
            refund_date = order_date + timedelta(days=random.randint(1, 30))
            stripe_refunds.append({
                "id": f"re_{stripe_charge_counter:012d}",
                "charge_id": stripe_charge_id,
                "amount": int(refund_amount * 100),
                "currency": "usd",
                "status": "succeeded",
                "created": refund_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "reason": random.choice(["requested_by_customer", "duplicate", "fraudulent", ""]),
                "metadata_shopify_order_id": order_id,
            })

    # Update Shopify customer totals
    for sc in shopify_customers:
        if sc["id"] == shopify_id:
            sc["orders_count"] = orders_for_customer
            sc["total_spent"] = f"{order_total_spent:.2f}"
            break

# ============================================================
# INJECT DATA QUALITY ISSUES (the realistic stuff)
# ============================================================
print("  💥 Injecting realistic data quality issues...")

# 1. Duplicate orders from webhook retries (~2% of orders)
num_dupes = int(len(shopify_orders) * 0.02)
for _ in range(num_dupes):
    original = random.choice(shopify_orders)
    dupe = original.copy()
    # Duplicate has same ID but slightly different timestamp (webhook retry)
    orig_time = datetime.strptime(original["created_at"][:19], "%Y-%m-%dT%H:%M:%S")
    dupe["created_at"] = (orig_time + timedelta(seconds=random.randint(1, 30))).strftime("%Y-%m-%dT%H:%M:%S-05:00")
    shopify_orders.append(dupe)

# 2. Orphaned refunds (refund in Stripe but no matching Shopify refund status)
for _ in range(15):
    orphan_charge = random.choice([sc for sc in stripe_charges if sc["status"] == "succeeded"])
    refund_amt = int(int(orphan_charge["amount"]) * random.uniform(0.3, 1.0))
    stripe_refunds.append({
        "id": f"re_orphan_{random.randint(100000, 999999)}",
        "charge_id": orphan_charge["id"],
        "amount": refund_amt,
        "currency": "usd",
        "status": "succeeded",
        "created": (datetime.strptime(orphan_charge["created"][:19], "%Y-%m-%dT%H:%M:%S") + timedelta(days=random.randint(5, 45))).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reason": "requested_by_customer",
        "metadata_shopify_order_id": orphan_charge["metadata_shopify_order_id"],
    })

# 3. Payment failures in Stripe with no Recharge record (~1%)
for _ in range(12):
    stripe_charge_counter += 1
    ghost_cust = random.choice(customers)
    ghost_date = random_date(DATE_START + timedelta(days=90), DATE_END)
    stripe_charges.append({
        "id": f"ch_ghost_{random.randint(100000, 999999)}",
        "amount": int(random.choice([34.99, 44.99, 89.99]) * 100),
        "amount_refunded": 0,
        "currency": "usd",
        "status": "failed",
        "created": ghost_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "customer_email": ghost_cust["email"],
        "metadata_shopify_order_id": "",
        "metadata_recharge_charge_id": "",
        "payment_method_type": "card",
        "failure_code": random.choice(["card_declined", "expired_card", "processing_error"]),
        "failure_message": "",
    })

# 4. Recharge subscriptions with status mismatch (says active but has a cancelled_at date)
mismatch_count = 0
for sub in recharge_subscriptions:
    if sub["status"] == "active" and random.random() < 0.03:
        sub["cancelled_at"] = (datetime.strptime(sub["created_at"][:19], "%Y-%m-%dT%H:%M:%S") + timedelta(days=random.randint(60, 200))).strftime("%Y-%m-%dT%H:%M:%SZ")
        mismatch_count += 1

print(f"    → {num_dupes} duplicate orders (webhook retries)")
print(f"    → 15 orphaned refunds")
print(f"    → 12 ghost Stripe failures")
print(f"    → {mismatch_count} subscription status mismatches")


# ============================================================
# WRITE CSV FILES
# ============================================================
print("\n📁 Writing seed files...\n")

write_csv("raw_shopify__customers.csv", shopify_customers,
    ["id", "email", "first_name", "last_name", "state", "created_at", "orders_count", "total_spent", "tags", "accepts_marketing"])

write_csv("raw_shopify__orders.csv", shopify_orders,
    ["id", "order_number", "customer_id", "email", "created_at", "financial_status", "fulfillment_status",
     "subtotal_price", "total_tax", "total_shipping", "total_price", "total_discounts", "discount_codes",
     "source_name", "landing_site", "referring_site", "cancel_reason", "cancelled_at", "tags", "billing_address_province"])

write_csv("raw_shopify__order_line_items.csv", shopify_line_items,
    ["id", "order_id", "product_id", "title", "variant_title", "sku", "quantity", "price", "total_discount"])

write_csv("raw_recharge__customers.csv", recharge_customers,
    ["id", "shopify_customer_id", "email", "first_name", "last_name", "created_at", "status", "has_payment_method_in_dunning"])

write_csv("raw_recharge__subscriptions.csv", recharge_subscriptions,
    ["id", "customer_id", "shopify_customer_id", "email", "product_title", "variant_title", "sku", "price",
     "quantity", "status", "created_at", "cancelled_at", "cancellation_reason", "charge_interval_frequency",
     "order_interval_unit", "next_charge_scheduled_at"])

write_csv("raw_recharge__charges.csv", recharge_charges,
    ["id", "subscription_id", "customer_id", "shopify_order_id", "email", "created_at", "processed_at",
     "type", "status", "total_price", "subtotal_price", "tax_lines", "discount_codes", "error_message", "payment_processor"])

write_csv("raw_stripe__charges.csv", stripe_charges,
    ["id", "amount", "amount_refunded", "currency", "status", "created", "customer_email",
     "metadata_shopify_order_id", "metadata_recharge_charge_id", "payment_method_type", "failure_code", "failure_message"])

write_csv("raw_stripe__refunds.csv", stripe_refunds,
    ["id", "charge_id", "amount", "currency", "status", "created", "reason", "metadata_shopify_order_id"])


# ============================================================
# SUMMARY
# ============================================================
print(f"""
✅ Data generation complete!

📊 Summary:
   Customers:              {NUM_CUSTOMERS}
   Shopify Orders:         {len(shopify_orders)} (incl. {num_dupes} duplicates)
   Shopify Line Items:     {len(shopify_line_items)}
   Recharge Subscriptions: {len(recharge_subscriptions)}
   Recharge Charges:       {len(recharge_charges)}
   Stripe Charges:         {len(stripe_charges)}
   Stripe Refunds:         {len(stripe_refunds)}

   Date Range: {DATE_START.strftime('%Y-%m-%d')} to {DATE_END.strftime('%Y-%m-%d')}
   Brand: {BRAND_NAME}

🗂️  Files written to: {OUTPUT_DIR}/

💡 Data quality issues embedded:
   - Duplicate Shopify orders from webhook retries
   - Orphaned Stripe refunds with no Shopify status update
   - Ghost Stripe payment failures with no Recharge record
   - Subscription status mismatches (active + cancelled_at)
   - Timezone inconsistencies (Shopify=EST, Recharge/Stripe=UTC)
   - Attribution nulls on repeat subscription orders (~60%)
""")
