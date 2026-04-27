import re


def extract_material_name(ad_name: str) -> str:
 
    if not ad_name:
        return ""
    parts = ad_name.split("_")
    if len(parts) < 2:
        return ""
    name_with_number = parts[1]
    return re.sub(r"\d+$", "", name_with_number)


def get_title(page: dict) -> str:

    properties = page.get("properties", {})
    for prop in properties.values():
        if prop.get("type") == "title":
            title_array = prop.get("title", [])
            return "".join(rt.get("plain_text", "") for rt in title_array)
    return ""


def filter_exact_material(pages: list, target_name: str) -> list:
 
    if not target_name:
        return []
    return [
        page for page in pages
        if extract_material_name(get_title(page)) == target_name
    ]
