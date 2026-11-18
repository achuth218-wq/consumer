"""Reconcile the two managed tiers for explicitly declared AI consumers."""
import json
import os
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlencode, urlsplit
from urllib.request import Request, urlopen
from uuid import UUID

import yaml


GROUPS = {"default-consumer-group", "custom-consumer-group"}


class ManifestLoader(yaml.SafeLoader):
    pass


ManifestLoader.add_constructor("!env", lambda loader, node: os.environ[loader.construct_scalar(node)])


def assignments(manifest):
    result = {}
    for item in manifest["ai_gateway_consumers"]:
        group, _, suffix = item["ref"].partition("--")
        name = item["name"]
        if group not in GROUPS or suffix != name or name in result:
            raise ValueError(f"Invalid or duplicate assignment for {name!r}")
        result[name] = group
    return result


def request(method, path, body=None):
    gateway = UUID(os.environ["AI_GATEWAY_ID"])
    url = f"https://in.api.konghq.com/v1/ai-gateways/{gateway}/{path}"
    headers = {"Authorization": "Bearer " + os.environ["KONNECT_TOKEN"]}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    with urlopen(Request(url, data=data, headers=headers, method=method), timeout=30) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def current_groups(name, preview):
    path = "consumers/" + quote(name, safe="")
    try:
        request("GET", path)
    except HTTPError as error:
        if error.code == 404 and preview:
            return set()
        raise
    collection = path + "/consumer-groups"
    page_path = collection
    groups = set()
    seen = set()
    while True:
        page = request("GET", page_path)
        groups.update(item["name"] for item in page["data"])
        following = page["meta"]["page"]["next"]
        if not following:
            return groups
        # Only reuse the cursor. All authenticated requests stay on the India host.
        cursor = parse_qs(urlsplit(following).query)["page[after]"][0]
        if cursor in seen:
            raise ValueError("Repeated membership page cursor")
        seen.add(cursor)
        page_path = collection + "?" + urlencode({"page[after]": cursor})


def reconcile(manifest, preview):
    desired = assignments(manifest)
    if manifest["ai_gateways"] != [{"ref": "target", "_external": {"id": os.environ["AI_GATEWAY_ID"]}}]:
        raise ValueError("Consumer manifest must target the configured AI_GATEWAY_ID")
    if any(item["ai_gateway"] != "target" for item in manifest["ai_gateway_consumers"]):
        raise ValueError("Every consumer must use the target AI Gateway")
    ids = {g: str(UUID(request("GET", "consumer-groups/" + g)["id"])) for g in sorted(GROUPS)}
    changed = False
    for name, target in desired.items():
        current = current_groups(name, preview)
        if target not in current:
            changed = True
            print(f"+ {name} → {target}")
            if not preview:
                request("POST", f"consumer-groups/{ids[target]}/consumers", {"consumer": name})
        for old in sorted((current & GROUPS) - {target}):
            changed = True
            print(f"- {name} from {old}")
            if not preview:
                request("DELETE", f"consumer-groups/{ids[old]}/consumers/{quote(name, safe='')}")
    if not changed:
        print("Group memberships are up to date.")


if __name__ == "__main__":
    mode, = sys.argv[1:]
    if mode not in {"apply", "preview"}:
        raise SystemExit("Use preview or apply")
    document = yaml.load(Path("konnect/consumer.yaml").read_text(), Loader=ManifestLoader)
    reconcile(document, preview=mode == "preview")
