# Brand Asset Library

Curated official Databricks brand assets bundled with the skill. **Do not load this file during slide generation** — it's reference material, read only when a slide explicitly calls for a product icon, partner logo, brand illustration, or the hexagon halftone.

The default during generation is **text + custom CSS**, not embedded imagery. See `BRAND_SYSTEM.md` "Designer mode" section for why.

## Inventory

| Path | Contents | Use for |
|---|---|---|
| `icons/products/` | 40 product SVGs (Agent Bricks, AI/BI, Lakeflow & family, Lakebase, Genie & Genie Code, Delta Lake/Sharing, Unity Catalog, Photon, MLflow, Mosaic AI, Databricks SQL, Databricks One, Apache Spark, Apps, Notebooks, Marketplace, Clean Rooms, Lakebridge, Lakewatch, Data Streaming) | Architecture diagrams that name specific products; product reference cards. **Not** decoration on generic content slides. |
| `icons/brands/` | 43 partner / sub-brand assets (Apache Spark, Azure Databricks, Mosaic Research, Delta Sharing, Delta Lake, MLflow, Unity Catalog OSS, Iceberg, PostgreSQL) — light + dark variants | Partner logos in capability slides; OSS callouts. |
| `illustrations/` | 6 brand illustrations: `illo_lakehouse.png`, `illo_pipelines.png`, `illo_customers.png`, `illo_company.png`, `illo_careers.png`, `illo_company_collage.png` | Hero panels on title or section dividers when a custom CSS layout doesn't fit. Not on content slides. |
| `logos/` | 4 Databricks wordmark PNGs: `logo_db_dark` (red brick + dark wordmark, light bg), `logo_db_main` (red brick + white wordmark, dark bg), `logo_db_white` (alt on dark), `logo_db_alt2` (outline brick) | Fallback when the inline SVG wordmark won't fit (PPT export, email previews). **Prefer the inline SVGs in `../assets/lockup-primary-{navy,white}.svg` for slides — they scale crisply.** |
| `hexagon_pattern.svg` | Single tiled SVG of the Databricks hexagon halftone (240×208 px tile, ~6% white opacity strokes) | **Title / section divider / closer slides only**, tiled over Navy 800 (`#1B3139`). The `.hex-tile` utility in `the inline CSS in pattern-catalog.html` wires it up. **Never on content slides.** |

Self-hosted DM Sans + DM Mono TTFs live one level up at `../fonts/` and are loaded automatically by `the inline CSS in pattern-catalog.html` `@font-face` rules.

## Embedding into a deck

Single-file decks must inline imagery — no external `<img src>`. Pick one of:

```bash
# Inline SVG markup directly
cat icons/products/agent-bricks.svg
# → paste the <svg>...</svg> into the slide HTML

# Or base64-encode for <img>
base64 -i icons/products/agent-bricks.svg | tr -d '\n'
# → wrap as src="data:image/svg+xml;base64,..."
```

For PNG illustrations, base64 is the only option:
```bash
base64 -i illustrations/illo_lakehouse.png | tr -d '\n'
# → src="data:image/png;base64,..."
```

## When to use these (and when not to)

**Default: don't use them.** A keynote-quality content slide doesn't need an icon. The bullet markers, the eyebrow, the slide title, and the bespoke layout do all the visual work. Adding an icon to a slide that doesn't structurally need one reads as template-decoration, not design.

**Reach for an asset only when**:

- The slide is **explicitly about** a specific product → `icons/products/<that-product>.svg`
- The slide names partner brands or open-source projects → `icons/brands/<that-brand>.svg`
- It's a title / section divider / closer slide and you want the hexagon halftone → set `class="slide slide--title hex-tile"` (no manual embed needed)
- It's a hero panel on a title slide and a CSS layout would feel underweight → one of the 6 illustrations
- An export target needs a wordmark PNG instead of inline SVG → `logos/`

**Don't**:

- Decorate every bullet with an icon. The red square markers from `.db-bullets` already do that work.
- Drop product icons onto generic content slides for "visual interest". Design custom CSS for the slide's content instead.
- Substitute competitor icons (AWS, Azure, GCP, Snowflake, Fabric) when representing Databricks features.
- Invent fake Databricks-style icons in SVG. If the right icon isn't in `products/` or `brands/`, **use clean text instead.**
- Use emoji as icons in body copy or cards. Ever.

## When the icon doesn't exist

The bundled set covers Databricks products and partner brands but is not exhaustive (the original system has 352+ icons; we shipped the highest-leverage 83). For an icon outside this set:

1. **First, try to express the concept with text** — a short label in `--db-navy-700` is brand-correct and almost always sufficient.
2. If you absolutely need an icon, fetch the official PNG from `go/brand-icons` or use a neutral system icon (Lucide, Phosphor) — never an icon styled to look like Databricks.
