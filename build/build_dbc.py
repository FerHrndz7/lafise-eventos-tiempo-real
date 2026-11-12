"""Arma build/lafise-eventos-tiempo-real.dbc desde los notebooks fuente (.py) de lafise-eventos-tiempo-real/.

Uso: python3 build/build_dbc.py   (desde la raíz del paquete)
El taller no usa librerías externas: todo el árbol viaja en el .dbc.
"""
import json, pathlib, random, re, uuid, zipfile

RAIZ = pathlib.Path(__file__).resolve().parent.parent
SRC = RAIZ / "lafise-eventos-tiempo-real"
OUT = RAIZ / "build" / "lafise-eventos-tiempo-real.dbc"
ROOT = "lafise-eventos-tiempo-real"

COMANDO_BASE = {
    "version": "CommandV1", "subtype": "command", "commandType": "auto", "commandVersion": 1, "state": "finished",
    "results": None, "resultDbfsStatus": "INLINED_IN_TREE", "resultDbfsErrorMessage": None, "errorSummary": None,
    "errorTraceType": None, "error": None, "errorDetails": None, "baseErrorDetails": None, "workflows": [],
    "startTime": 0, "submitTime": 0, "finishTime": 0, "collapsed": False, "bindings": {}, "inputWidgets": {},
    "displayType": "table", "width": "auto", "height": "auto", "xColumns": None, "yColumns": None,
    "pivotColumns": None, "pivotAggregation": None, "useConsistentColors": False, "customPlotOptions": {},
    "commentThread": [], "commentsVisible": False, "parentHierarchy": [], "diffInserts": [], "diffDeletes": [],
    "globalVars": {}, "latestUser": "a user", "latestUserId": None, "latestAssumeRoleInfo": None,
    "commandTitle": "", "showCommandTitle": False, "hideCommandCode": False, "hideCommandResult": False,
    "isLockedInExamMode": False, "iPythonMetadata": None, "metadata": {}, "streamStates": {},
    "datasetPreviewNameToCmdIdMap": {}, "tableResultIndex": None, "listResultMetadata": [],
    "subcommandOptions": None, "contentSha256Hex": None, "tableResultSettingsMap": {},
}


def oid():
    return random.randint(10**15, 10**16 - 1)


def celdas(texto):
    assert texto.startswith("# Databricks notebook source"), "no es un notebook fuente"
    cuerpo = texto.split("\n", 1)[1]
    for c in (c.strip("\n") for c in cuerpo.split("# COMMAND ----------")):
        if not c.strip():
            continue
        lineas = c.split("\n")
        if all(l.startswith("# MAGIC") for l in lineas if l.strip()):
            c = "\n".join(re.sub(r"^# MAGIC ?", "", l) for l in lineas)
        yield c


def notebook(nombre, texto):
    base = oid()
    cmds = []
    for i, c in enumerate(celdas(texto)):
        x = dict(COMANDO_BASE, command=c, position=float(i + 1), guid=str(uuid.uuid4()),
                 nuid=str(uuid.uuid4()), origId=base + i + 1)
        cmds.append(x)
    return {"version": "NotebookV1", "origId": base, "name": nombre, "language": "python", "commands": cmds,
            "dashboards": [], "guid": str(uuid.uuid4()), "globalVars": {}, "iPythonMetadata": None,
            "inputWidgets": {}, "notebookMetadata": {}, "reposExportFormat": "SOURCE",
            "environmentMetadata": None, "computePreferences": None, "inputWidgetPreferences": None,
            "environmentSerializationPreferences": None}


def main():
    with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.mf", json.dumps({"guid": str(uuid.uuid4()), "version": "Manifest", "origId": -1,
                                              "name": "manifest.mf"}))
        carpetas = {SRC} | {p.parent for p in SRC.rglob("*.py")}
        for d in sorted(carpetas):
            rel = ROOT if d == SRC else f"{ROOT}/{d.relative_to(SRC)}"
            z.writestr(rel + "/", json.dumps({"origId": oid(), "name": d.name if d != SRC else ROOT,
                                              "guid": str(uuid.uuid4())}))
        n = 0
        for f in sorted(SRC.rglob("*.py")):
            rel = f"{ROOT}/{f.relative_to(SRC).with_suffix('')}.python"
            z.writestr(rel, json.dumps(notebook(f.stem, f.read_text(encoding="utf-8")), ensure_ascii=False))
            n += 1
    print(f"✅ {OUT.name}: {n} notebooks")


if __name__ == "__main__":
    main()
