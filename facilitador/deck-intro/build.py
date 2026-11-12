"""Ensambla el deck: src/head.html (estilos del catálogo de patrones) + src/slides.html + src/tail.html (deck-stage.js).

Uso: python3 build.py   (desde facilitador/deck-intro). assets/ y fonts/ viajan junto al HTML.
"""
import pathlib
here = pathlib.Path(__file__).parent
partes = [(here / "src" / n).read_text(encoding="utf-8") for n in ("head.html", "slides.html", "tail.html")]
out = here / "lafise-intro-eventos-tiempo-real.html"
out.write_text("\n".join(partes), encoding="utf-8")
print(f"✅ {out.name}")
