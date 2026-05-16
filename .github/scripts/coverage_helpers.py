def is_trackable(filename: str) -> bool:
    norm = filename.replace('\\', '/').lstrip('./')
    if not norm.endswith('.cs'):
        return False
    if norm.startswith('obj/') or '/obj/' in norm:
        return False
    if norm.endswith('Tests.cs') or '.Tests/' in norm or '/Tests/' in norm:
        return False
    return True
