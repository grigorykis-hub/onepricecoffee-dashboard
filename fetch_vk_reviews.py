import os, json, urllib.request, urllib.parse

token = os.environ.get("VK_TOKEN", "")
if not token:
    print("NO TOKEN"); exit(1)

BASE = "https://api.vk.com/method"
V = "5.199"

def vk(method, params):
    params["access_token"] = token
    params["v"] = V
    url = BASE + "/" + method + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=15) as r:
        d = json.loads(r.read())
    if "error" in d:
        print(f"  ERR [{method}]: {d['error']['error_msg']}")
        return None
    return d.get("response")

GROUP_ID = 236450024

# Try to get reviews list via groups.getReviews (undocumented but exists)
for method in ["groups.getReviews", "groups.reviews"]:
    r = vk(method, {"group_id": GROUP_ID, "count": 100, "sort": "date_desc"})
    print(f"\n=== {method} ===")
    print(json.dumps(r, ensure_ascii=False, indent=2)[:1500] if r else "None")

# Try execute with getReviews
code = f"return API.groups.getReviews({{\"group_id\": {GROUP_ID}, \"count\": 100}});"
r = vk("execute", {"code": code})
print(f"\n=== execute getReviews ===")
print(json.dumps(r, ensure_ascii=False, indent=2)[:2000] if r else "None")

# Also try board.getComments on the reviews topic (id=55892727)
r = vk("board.getComments", {
    "group_id": GROUP_ID,
    "topic_id": 55892727,
    "count": 100,
    "extended": 1
})
print(f"\n=== board.getComments (ОТЗЫВЫ topic) ===")
print(json.dumps(r, ensure_ascii=False, indent=2)[:3000] if r else "None")
