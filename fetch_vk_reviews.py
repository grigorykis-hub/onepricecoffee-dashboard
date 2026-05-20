import os, json, urllib.request, urllib.parse

token = os.environ.get("VK_TOKEN", "")
if not token:
    print("NO TOKEN")
    exit(1)

def vk(method, params):
    params["access_token"] = token
    params["v"] = "5.199"
    url = "https://api.vk.com/method/" + method + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read())

GROUP_ID = 173874815

# Try reviews widget methods
methods_to_try = [
    ("reviews.getList", {"owner_id": f"-{GROUP_ID}", "count": 100, "sort": "date_desc"}),
    ("reviews.getGroupReviews", {"group_id": GROUP_ID, "count": 100}),
    ("widgetReviews.get", {"group_id": GROUP_ID}),
]

for method, params in methods_to_try:
    try:
        resp = vk(method, params)
        print(f"\n=== {method} ===")
        print(json.dumps(resp, ensure_ascii=False, indent=2)[:2000])
    except Exception as e:
        print(f"  Error: {e}")

# Also try getting wall posts with reviews hashtag
try:
    resp = vk("wall.search", {"owner_id": f"-{GROUP_ID}", "query": "отзыв", "count": 20})
    print("\n=== wall.search отзыв ===")
    print(json.dumps(resp, ensure_ascii=False, indent=2)[:2000])
except Exception as e:
    print(f"  wall.search error: {e}")

