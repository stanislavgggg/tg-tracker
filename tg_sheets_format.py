"""Оформление вкладок Google Sheets (ширины, заголовок, проценты, цвета, подсказки на заголовках).
Вызывается из трекера после записи данных; любая ошибка форматирования не должна ломать запись данных."""

HEADER_BG = (0.12, 0.23, 0.37)
WHITE = (1.0, 1.0, 1.0)


def _color(rgb):
    return {"red": rgb[0], "green": rgb[1], "blue": rgb[2]}


def col_letter(i: int) -> str:
    s = ""
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return s


def apply_view(sh, ws, spec: dict, n_rows: int):
    """spec: ncols, widths[px], freeze_rows, freeze_cols, tab_color(rgb), index, percent_cols, pp_cols,
    notes[str per header], filter(bool), wrap_all(bool), rules[{formula, c0, c1, bg, fg}]"""
    sid = ws.id
    ncols = spec["ncols"]
    grid_rows = max(ws.row_count, n_rows, 2)
    last = grid_rows  # фильтр на весь лист, чтобы он покрывал и накопленные строки

    def rng(r0, r1, c0, c1):
        return {"sheetId": sid, "startRowIndex": r0, "endRowIndex": r1,
                "startColumnIndex": c0, "endColumnIndex": c1}

    # 1) убрать прошлые правила условного форматирования, чтобы они не множились
    n_old = 0
    for s in sh.fetch_sheet_metadata().get("sheets", []):
        if s["properties"]["sheetId"] == sid:
            n_old = len(s.get("conditionalFormats", []))
    reqs = [{"deleteConditionalFormatRule": {"sheetId": sid, "index": 0}} for _ in range(n_old)]

    # 2) закрепление, цвет и положение вкладки
    props = {"sheetId": sid, "gridProperties": {
        "frozenRowCount": spec.get("freeze_rows", 1), "frozenColumnCount": spec.get("freeze_cols", 0)}}
    fields = "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
    if spec.get("tab_color"):
        props["tabColor"] = _color(spec["tab_color"])
        fields += ",tabColor"
    if spec.get("index") is not None:
        props["index"] = spec["index"]
        fields += ",index"
    reqs.append({"updateSheetProperties": {"properties": props, "fields": fields}})

    # 3) ширины колонок и высота заголовка
    for i, w in enumerate(spec.get("widths", [])):
        reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": w}, "fields": "pixelSize"}})
    reqs.append({"updateDimensionProperties": {
        "range": {"sheetId": sid, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
        "properties": {"pixelSize": spec.get("header_height", 52)}, "fields": "pixelSize"}})

    # 4) заголовок
    reqs.append({"repeatCell": {
        "range": rng(0, 1, 0, ncols),
        "cell": {"userEnteredFormat": {
            "backgroundColor": _color(HEADER_BG),
            "textFormat": {"bold": True, "foregroundColor": _color(WHITE)},
            "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE", "horizontalAlignment": "CENTER"}},
        "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment,horizontalAlignment)"}})

    # 5) числовые форматы
    for c in spec.get("percent_cols", []):
        reqs.append({"repeatCell": {
            "range": rng(1, grid_rows, c, c + 1),
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": '0.0"%"'}}},
            "fields": "userEnteredFormat.numberFormat"}})
    for c in spec.get("pp_cols", []):
        reqs.append({"repeatCell": {
            "range": rng(1, grid_rows, c, c + 1),
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "+0.0;-0.0;0.0"}}},
            "fields": "userEnteredFormat.numberFormat"}})
    if spec.get("wrap_all"):
        reqs.append({"repeatCell": {
            "range": rng(1, grid_rows, 0, ncols),
            "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"}},
            "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)"}})

    # 6) фильтр и подсказки на заголовках
    if spec.get("filter"):
        reqs.append({"setBasicFilter": {"filter": {"range": rng(0, last, 0, ncols)}}})
    if spec.get("notes"):
        reqs.append({"updateCells": {
            "range": rng(0, 1, 0, ncols),
            "rows": [{"values": [{"note": n} for n in spec["notes"]]}],
            "fields": "note"}})

    # 7) условные правила (порядок в spec = приоритет)
    for pos, r in enumerate(spec.get("rules", [])):
        fmt = {}
        if r.get("bg"):
            fmt["backgroundColor"] = _color(r["bg"])
        if r.get("fg"):
            fmt["textFormat"] = {"foregroundColor": _color(r["fg"])}
        reqs.append({"addConditionalFormatRule": {"index": pos, "rule": {
            "ranges": [rng(1, grid_rows, r["c0"], r["c1"])],
            "booleanRule": {"condition": {"type": "CUSTOM_FORMULA",
                                          "values": [{"userEnteredValue": r["formula"]}]},
                            "format": fmt}}}})

    sh.batch_update({"requests": reqs})
