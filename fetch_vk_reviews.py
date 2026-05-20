import os, json, urllib.request, urllib.parse, datetime

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
        print(f"  VK error [{method}]: {d['error']['error_msg']}")
        return None
    return d.get("response")

# 1. Resolve group
resp = vk("utils.resolveScreenName", {"screen_name": "onepricecoffee_ivanovo"})
group_id = resp["object_id"] if resp else 173874815
print(f"Group ID: {group_id}")

# 2. Try board topics
print("\n=== board.getTopics ===")
r = vk("board.getTopics", {"group_id": group_id, "count": 10})
print(json.dumps(r, ensure_ascii=False, indent=2)[:1000] if r else "None")

# 3. Try groups.getById with reviews field
print("\n=== groups.getById ===")
r = vk("groups.getById", {"group_id": group_id, "fields": "reviews,rating,counters"})
print(json.dumps(r, ensure_ascii=False, indent=2)[:1500] if r else "None")

# 4. Try wall.get looking for review posts
print("\n=== wall.get (owner posts) ===")
r = vk("wall.get", {"owner_id": f"-{group_id}", "count": 5, "filter": "owner"})
if r and r.get("items"):
    for item in r["items"][:3]:
        print(f"  id={item['id']} date={item['date']} text={item.get('text','')[:100]}")

# 5. Try to get reviews via newsfeed or mentions
print("\n=== wall.search reviews ===")
r = vk("wall.search", {"owner_id": f"-{group_id}", "query": "★", "count": 20})
if r and r.get("items"):
    for item in r["items"][:5]:
        print(f"  id={item['id']} text={item.get('text','')[:150]}")
        
# 6. Try execute to get reviews data
print("\n=== Trying reviews via execute ===")
code = f"""
var g = API.groups.getById({{"group_id": {group_id}, "fields": "reviews"}});
return g;
"""
r = vk("execute", {"code": code})
print(json.dumps(r, ensure_ascii=False, indent=2)[:1000] if r else "None")

