# Zhuoyue Scraper · for you and your misses

[中文](README.md) | **English**

> A **self-hosted JAV metadata scraper**: build the library → semantic classification → three-lane scraping (site / uncensored / AI) → Jellyfin-ready NFO & cover art.
> Single-file HTTP service + SQLite. **No cloud dependency** — everything stays on your machine.

<p align="center">
  <b>Library scan</b> · <b>Semantic classify</b> · <b>javbus / javdb / MissAV / 123AV / FC2</b> · <b>local LLM scraping</b> · <b>AI video watching</b> · <b>frame-grab covers</b> · <b>TV series (tvshow)</b> · <b>manual review for bad rows</b>
</p>

## Screenshots

![Main UI (top control bar / stat cards / quality & semantic split / port & LAN settings)](docs/ui-01-main.png)

![Quality / source statistics (real library, 1469 items)](docs/ui-02-stats.png)

![Job log and filters](docs/ui-03-log.png)

<sub>On the Jellyfin side, enable "save image paths in the NFO file":</sub>

![Jellyfin NFO settings](docs/jellyfin-nfo.png)

> All screenshots are **the real UI** (running against a 1,469-item library; nothing masked).

---

## ⚠️ Disclaimer (read first)

- This project is **only for organizing metadata of media files you already legally own locally**. It contains, provides, and distributes no video content.
- Users must be **18 years or older** and are responsible for complying with the **laws of their own country/region**.
- Users must confirm they have the legal right to the content they scrape, and bear all consequences of use.
- The author is not responsible for any user's actions or their consequences.
- This project **ships no crawler accounts, cookies, or keys**; sites like JavDB require credentials you provide yourself.

---

## Contents

- [Features](#features)
- [Requirements](#requirements)
- [Install](#install)
- [Run](#run)
- [Model support](#model-support)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Pipeline](#pipeline)
  - [Quality / Source / Kind](#quality--source--kind)
  - [Codes and version suffixes](#codes-and-version-suffixes)
  - [UI and operations](#ui-and-operations)
  - [Sites](#sites)
- [Directory layout](#directory-layout)
- [Database](#database)
- [FAQ](#faq)
- [Credits](#credits)
- [License](#license)

---

## Features

| | |
|---|---|
| 🗂 **Library scan / incremental** | Walks the library tree, reads existing `movie.nfo` / `tvshow.nfo` to detect prior state; supports full rebuild |
| 🧠 **Semantic classification** | Local LLM decides per item: censored / uncensored / no code. **20 items per batch** to cut round-trips |
| 🌐 **Site lane** | javbus (censored **+ `/uncensored`**) → javdb → MissAV → **123AV**, run **in parallel** with the AI lane |
| 🎌 **FC2 official** | Direct lookup for FC2-PPV codes: official title / sample cover / seller / rating / price |
| 🕵️ **AI lane** | When sites miss, the local LLM writes title / plot / tags. **5 items per batch** |
| 🎬 **AI video watching** | When the filename says nothing (e.g. `226`, `227`): ffmpeg grabs frames → **vision model** looks at them → title / plot / tags |
| 🏷 **AI tag fill** | Items with a plot but no tags get tags generated from the plot (selection can force-overwrite old tags) |
| 🔀 **Two lanes in parallel** | Site lane (network I/O) and AI lane (GPU) run at the same time without starving each other |
| 🖼 **Covers** | Site poster > folder image > **frame grab** (ffmpeg + OpenCV face/sharpness scoring); **long videos pick a random start in [2min, end]**, and **a random video when there are several**; **Refresh thumbs** can auto-build a cover |
| 🧹 **Delete cover** | Removes cover files + thumbnail cache in one go, back to "no cover" |
| 📺 **Series** | Multiple videos in one folder are detected as a series and get **one merged `tvshow.nfo`** (no per-episode split) |
| 🚧 **Nested-folder check** | A subfolder that still contains videos (or ≥2 subfolders) is marked `有误` (bad) immediately — **no scraping**, hand it to a human |
| 🧹 **Flatten nested folders** | `kind=nested` folders never reach the auto pipeline — flatten them in one click: `P_` for images, `V_` for videos, `序号_名字_` for named dirs; **preview + confirm dialog + one-click undo**, unknown file types are left untouched |
| 🔌 **Port / LAN** | Change the **listening port** and toggle **LAN access** right in the UI — **hot-swapped live**, no restart |
| ✏️ **Quality/Source/Semantic** | **Double-click any cell to edit**; select rows for **bulk actions at the top** |
| 🔃 **Sorting** | Sort by **ID (default) / added / last edited** × **asc / desc**, remembered together with filters; hover the ID cell to see added & last-edited times |
| 🚨 **Manual review** | Both site lanes missed, or nested folders → `有误`: skipped by the auto pipeline, handled by human + LLM (see [`有误处理提示词.md`](有误处理提示词.md), Chinese) |
| 📇 **Code reference** | Common code-prefix table auto-generated from your own DB: [`番号参考表.md`](番号参考表.md) (Chinese) |
| 💾 **Fully persisted** | Queue, log and settings all survive restarts (SQLite WAL) |

> Note: the app UI ships in **5 languages** — 中文 / English / 日本語 / Español / 繁體中文 — switchable from the top bar. The **metadata output language** (the language of titles, plots and tags written into `.nfo`) is a **separate** setting, so you can run an English UI while writing Japanese titles.

---

## 🐳 Docker (recommended for NAS / no-local-install)

No Python, no ffmpeg, no config — one command:

```bash
docker run -d --name zhuoyue-scraper \
  -p 8811:8800 \
  -e MS_ROOT=/media \
  -e MS_DB=/data \
  -e MS_OLLAMA=http://host.docker.internal:11434 \
  -e PUID=1000 -e PGID=1000 \
  -v /your/media:/media \
  -v ./zhuoyue-data:/data \
  --add-host host.docker.internal:host-gateway \
  --restart unless-stopped \
  ghcr.io/ceiling0518/zhuoyue-scraper:latest
```

Then open `http://<host-ip>:8811`. Or use the bundled `docker-compose.yml` (recommended):
 edit the media path, then `docker compose up -d`.

Available for **linux/amd64 and linux/arm64** (Synology/QNAP ARM models included).

### Environment variables

| Variable | Default | Notes |
|---|---|---|
| `MS_ROOT` | `/media` | Media library root (path inside the container) |
| `MS_DB` | `/data` | DB / cache / thumbnails (**must be persisted**) |
| `MS_OLLAMA` | `http://host.docker.internal:11434` | Ollama endpoint (can point to another machine) |
| `MS_HOST` / `MS_PORT` | `0.0.0.0` / `8800` | Listen address and port inside the container |
| `PUID` / `PGID` | `0` | Which uid writes files; set this to your media owner or the .nfo files end up root-owned |
| `MS_JAVDB` | empty | javdb mirror domain (when your region is blocked) |
| `MS_COVER_MIN_SEC` | `120` | Earliest frame time (s) for covers; long videos pick a **random** start between here and the end |
| `MS_COVER_WINDOW_SEC` | `120` | Sampling window (s) per grab; the best-scoring frame inside the window wins |
| `MS_COVER_TAIL_SEC` | `5` | Safety margin (s) kept away from the end of the video |

### ❗ Works without Ollama

The **main pipeline never needs Ollama**: scan → code detection → multi-site scraping →
`.nfo` + covers/thumbnails → queue & logs.

| Capability | no Ollama | with Ollama |
|---|---|---|
| Scan / code detection / rebuild | ✅ | ✅ |
| Site scraping (JavBus/MissAV/avsox/123AV/NJAV/Netflav/JAVTrailers/FC2) | ✅ | ✅ |
| `.nfo` / covers / thumbnails / series merge | ✅ | ✅ |
| Semantic classification (coded/uncensored/none) | ❌ | ✅ |
| AI watching (frame extraction for unnamed files) | ❌ | ✅ |
| AI tag fill | ❌ | ✅ |
| AI fallback when all sites miss | ❌ | ✅ |

> That is why an **ARM NAS (which usually has no Ollama) is still worth installing**:
> the pure-scraping pipeline is already complete metadata for Jellyfin/Emby.

### Behaviour differences inside a container (auto-degraded)

- **Open folder** / **open full image** are **hidden automatically** (no desktop on the server)
- **Delete** = **permanent** (no recycle bin on Linux/containers); the red warning stays
- Port / LAN hot-switch still works, but in a container you normally just use `-p`

### Build it yourself

```bash
docker build -t zhuoyue-scraper:latest .
docker build --build-arg PIP_INDEX_URL=https://your.pip.mirror/simple -t zhuoyue-scraper:latest .
```

---

## Requirements

| Component | Notes |
|---|---|
| **Python** | 3.10+ (developed on 3.14) |
| **ollama** | Local LLM runtime. **Must be started via `ollama serve`** (the tray app can't see `OLLAMA_MODELS`) |
| **ffmpeg / ffprobe** | On `PATH` — frame grabs, durations, AI watching |
| **OS** | Windows (recycle-bin delete and Explorer "open folder" are Windows implementations; other platforms need adapting) |

Python deps:

```bash
pip install requests opencv-python numpy zhconv
```

---

## Install

```bash
git clone https://github.com/ceiling0518/zhuoyue-scraper.git
cd zhuoyue-scraper
pip install requests opencv-python numpy zhconv
```

Make sure ollama is up:

```bash
ollama serve                # keep it running in another terminal
ollama list                 # your models should be listed
```

---

## Run

```bash
python app.py
```

or double-click `run.cmd`. Then open:

```
http://127.0.0.1:8800/
```

- **Change port / enable LAN**: edit **Port** and tick **LAN** in the top bar, then **Save settings** — it takes effect **immediately** (the listener is hot-swapped, no restart).
- With LAN on, the available URLs are listed (e.g. `http://192.168.1.100:8800/`).
- Environment variables also work (they win over UI settings): `set MS_HOST=0.0.0.0`, `set MS_PORT=8801`.

---

## Model support

### 2. Site health checks

The **Sites** dialog has two buttons:

- **Check connectivity** — quick request to each site entry: `pass / fail(reason)`. Reasons: `geo-block` (your proxy's exit country is blocked), `cloudflare?` (challenge, hint only), `spa-shell` (small homepage; normal for SPAs, hint only), `origin-down` (e.g. 522), `timeout`.
- **Deep check** — **performs one real lookup per site** with a fixed test code, reporting `hit <code> · N fields · ms`. This is the authoritative answer.

Measured 2026-10-12: **javbus / MissAV / avsox / 123AV / NJAV / Netflav / JAVTrailers / FC2 official — 8 sites working**.
`javdb` returns **geo-block**: switch your proxy exit region, or point `MS_JAVDB` at a working mirror.
`jav321` origin is down (522) and stays disabled.

### 1. Language model (semantic classification + AI scraping + tag fill)

Classification and AI scraping go through [ollama](https://ollama.com) — **any ollama model works**, as long as it returns stable JSON for the given prompts. Pick it from the dropdown in the top bar.

Verified / recommended:

| Model | Size | Use | Notes |
|---|---|---|---|
| `qwen38-27b-uncen` | 27B | **Default**: semantic + AI scrape + **AI watching** | Strong Chinese, uncensored fine-tune, best tags |
| `qwen38-27b-q4` | 27B (Q4) | same | fits in 24 GB VRAM |
| `qwopus38-27b-q4` | 27B (Q4) | backup | |
| `muse-glimmer:30b-q4_K_M` | 30B (Q4) | backup | |
| `opus-distill-q5` | — | backup | |
| `qwen38-9b-v2` | 9B | low VRAM | fast, slightly worse |

> **AI video watching** needs a model with the **`vision`** capability (`ollama show <model>` lists capabilities). The 27B models above all do.
> Tip: the model must **not refuse** (adult content gets rejected en masse by safety-aligned models), so community uncensored fine-tunes are recommended.

### 2. Covers / images

| Component | Use |
|---|---|
| **ffmpeg / ffprobe** | Frame grabs, video duration, sampling frames for AI watching |
| **OpenCV (with YuNet DNN face detection)** | Scores grabbed frames: face count, sharpness, colourfulness, skin ratio, brightness, composition |
| **javbus / javdb / MissAV / 123AV / FC2 official** | Poster download |

**Cover priority**: scraped poster → image already in the folder → frame grab.

- **Frame-grab timing**: for a series where `duration / 3 > 120`, grab **after 120 s**; otherwise the grab time is **unrestricted**.
- **Refresh thumbs**: if there is no cover it **builds one** — the biggest image in the folder if any, otherwise **a frame cut from the video**, saved as `poster.jpg`.
- **Delete cover**: `poster.*` / `fanart.*` / `folder.jpg` + the app's cached copy + the thumbnail cache, cleaned in one shot.

---

## Configuration

"Save settings" in the top bar writes to the `config` table:

| Item | Notes |
|---|---|
| **Library root** | Media library root (UNC paths supported, e.g. `\\NAS\share\movie`) |
| **Model** | ollama model currently in use |
| **Port** | Listening port (default `8800`), effective **immediately** |
| **LAN** | Ticked = listen on `0.0.0.0` (reachable on the LAN); unticked = `127.0.0.1` only |

Environment variables (they win over UI settings):

| Variable | Default | Notes |
|---|---|---|
| `MS_HOST` | `127.0.0.1` | Listen address (takes precedence when set) |
| `MS_PORT` | `8800` | Listen port (takes precedence when set) |
| `OLLAMA_MODELS` | — | ollama model dir (read when `ollama serve` starts) |
| `FFMPEG` / `FFPROBE` | `ffmpeg` / `ffprobe` | Full paths allowed |
| `JAV_PROXY` | `http://127.0.0.1:7897` | HTTP proxy used for scraping |

Run parameters:

| Parameter | Meaning | Value 0 means |
|---|---|---|
| **Limit** | Max items processed this run | **all** |
| **Semantic group** | Items per LLM batch for classification | **all in one batch** (auto-split at 60) |
| **AI group** | Items per LLM batch for AI scraping | **all in one batch** (same) |
| **Mode** | `auto` / `ai` / `website` | — |
| **force** | Ignore quality, re-scrape everything | — |

---

## Usage

### Pipeline

```
scan → semantic classification → fetch (site lane ∥ AI lane)
```

- Classification routes each item: `coded` / `uncensored` → site lane; `none` → AI lane
- Both lanes run **in parallel**; if the site lane fails the item is marked `有误` (bad)
- Select rows to re-run them in bulk from the top bar
- **Nested folders are checked before scraping**: a subfolder containing videos → `有误` right away, no wasted LLM calls

### Quality / Source / Kind

**quality**

| Value | Meaning | Behaviour |
|---|---|---|
| `有` (ok) | Complete (title + plot + tags) | **All scraping skipped** |
| `缺` (partial) | ① title+plot but **no tags** (MissAV has none by nature; likewise AI sources) ② **useless filename** (bare digits like `226` — only AI watching can save it) | Treated as done; use **AI tag fill / AI watching** later |
| `无` (none) | Has data but the title isn't cleaned (equals the raw folder name), or nothing usable yet | **Normal scraping runs** |
| `有误` (bad) | ① **both site lanes missed** ② **nested folders** (a subfolder has videos, or ≥2 subfolders) | **Skipped**, hand to human + LLM |
| `删除` (deleted) | Deleted, or the folder vanished at scan time | Hidden by default, filterable |

**source**: `code` (javbus/javdb) · `Uncen` (MissAV/123AV) · `AI` (local model)

**kind**

| Value | Detected by | Handling |
|---|---|---|
| `single` | 1 video | Normal movie flow |
| `series` | Several videos (episodes, flat) | Scraped once, **one merged `tvshow.nfo`** |
| `nested` | Subfolders still contain videos | Marked `有误`, re-run after fixing the layout |
| `novideo` | No video in the folder | Not processed |

> ⚠️ `kind` follows the **actual on-disk layout**: writing the NFO auto-corrects stale values, so you don't have to fix them by hand after reorganising.

### Codes and version suffixes

Code format:

```
  <series> - <number> [ - <version suffix> ]
  e.g. ABP-123        e.g. SSIS-001-HD        e.g. MIDA-456-C
```

**Version suffixes** (trailing letters; they describe *this copy's edition* and **never take part in code matching**):

| Suffix | Meaning |
|---|---|
| `C` | Chinese subtitles |
| `J` / `X` | Cracked / decensored release |
| `HD` | High quality |
| `SD` | Standard quality |
| `4K` | 4K |

> Suffixes are stripped before searching: `ABP-123-HD` is looked up as `ABP-123`.
> The common-prefix table built from your own library lives in [`番号参考表.md`](番号参考表.md) (Chinese).

### UI and operations

**Top bar**: library root · model · **port** · **LAN** · save settings · docs · JavDB cookie · sites

**Buttons**: ① full pipeline · rebuild library (**double confirm**) ② incremental scan · classify · fetch · run queue · clear queue · stop

**Bulk action bar** (acts on selected rows)

| Action | Notes |
|---|---|
| Re-run · sites | Site lane only (javbus / javdb / MissAV / 123AV / FC2) |
| Re-run · AI | AI lane only (local model, from the filename) |
| **AI tag fill** | Generate tags from the **plot** in bulk (selection allows overwriting old tags) |
| **AI watching** | **Watch the video via frames** to produce title/plot/tags (for useless filenames) |
| Re-run · full | Both lanes |
| Re-run queue | Push into the background queue by the dropdown (auto / website / ai) — **persisted** |
| Open folder | Opens the item folder (**raised to front**) |
| Refresh thumbs | Builds a cover if missing (pick image / cut a frame) → clears thumbs → rebuilds |
| Delete cover | Removes cover files + thumbnail cache |
| Delete | Deletes folder + videos to the recycle bin (**two-step confirm**); also archives the DB row when the folder is already gone |
| Select page / clear | Bulk selection |

> ⚠️ **When the library is on a NAS (UNC path) Windows cannot really recycle — it equals permanent deletion** (the confirm dialog warns you).

**Selection**: a **Select** button on each row — click to select (**whole row highlighted**), click again to deselect.

**Inline editing**: **double-click** a cell

| Column | Editor |
|---|---|
| quality | dropdown 有 / 缺 / 有误 / 无 / 删除 |
| source | dropdown code / Uncen / AI |
| semantic | dropdown coded / uncensored / none |
| code | text box |

Editing semantic/code sets `sem_manual=1` (the classify stage stops overwriting manual values). Enter confirms, Esc cancels.

**Filters**: quality / source / semantic / search (excludes `删除` by default)
**Paging**: previous / page N of M (dropdown) / next — **page and filters survive a refresh**.
**Thumbnails**: 160px cached in `thumbs/`, not in the DB; clicking opens the **largest original image** in the folder with your local viewer.
**JavDB cookie**: top-bar button — **paste** or **upload a file**; saved to `javdb_cookie.txt` and effective immediately.
**Site settings**: top-bar "sites" button
- Lists every source site (below)
- **Tick** the ones to use (tried in this order)
- **Check connectivity** sends a **real request** per site (same session/retries/proxy as the scraper) and shows status + latency
- All scraping goes through the local proxy `JAV_PROXY` (default `http://127.0.0.1:7897`)

### Sites

| Site | Type | Status |
|---|---|---|
| JavBus | censored **+ uncensored** (`/uncensored`) | ✅ integrated |
| JavDB | censored (full fields: studio/genres/actors/date/runtime) | ✅ integrated (needs cookie) |
| MissAV | uncensored | ✅ integrated |
| **123AV** | uncensored | ✅ integrated (title/cover/runtime/genres) |
| **FC2 official** | FC2 | ✅ integrated (title/sample/seller/rating/price) |
| JAV321 | censored | ⏳ origin returns 522, currently unusable |
| avsox | uncensored | ⏳ SPA, needs its API |
| NJAV | uncensored | ⏳ 123AV mirror, needs its API |
| Netflav | aggregator | ⏳ SPA (`__NEXT_DATA__`) |
| JavTrailers | info | ⏳ SPA (`__NUXT__`) |

> On JavBus an uncensored code matches on the **digits only** (`1pondo-012817_473` → search `012817_473`); the code generates variants automatically.

---

## Directory layout

```
library root/
├── single-video item/      → single
│   ├── xxx.mp4
│   ├── movie.nfo           (generated)
│   └── poster.jpg
├── multi-video item/       → series
│   ├── 1.mp4  2.mp4  3.mp4
│   └── tvshow.nfo          (generated, merged)
└── item with subfolders/   → nested
    └── subfolder/
        └── xxx.mp4
```

The NFO **writes image paths** (`<thumb>` / `<fanart>`), matching Jellyfin's "save image paths in NFO file".

---

## Database

SQLite (`mlib.db`, WAL):

| Table | Purpose |
|---|---|
| `items` | Main: path / name / code / quality / source / kind / title / plot / tags / actors / studio / year / sem_kind / sem_code / sem_manual / cover / nfo / present … |
| `archive` | Archived rows: rebuilds, vanished folders, deletions |
| `queue` | Re-run queue (persisted) |
| `joblog` | Job log (persisted, last 3000 lines) |
| `config` | Settings: `root`, `model`, `port`, `lan` |
| `runs` | Per-scan added/removed records |

---

## FAQ

**Q: ollama has models but the UI says it sees none.**
A: The tray build `ollama app.exe` cannot read `OLLAMA_MODELS`. Start it with **`ollama serve`** instead.

**Q: MissAV items have no tags.**
A: MissAV simply provides none. Such items become `缺` (treated as done, no re-scrape); use **AI tag fill** later.

**Q: Why do some items stay at `无`?**
A: `无` = has data but the title isn't cleaned (equals the raw folder name). If a site lane never finds it, mark it `有误` or fill it in by hand.

**Q: The folder is named `226` / `227` — now what?**
A: Those become `缺`, meaning "the filename carries no information, only AI watching can help". Select them and hit **AI watching**.

**Q: What counts as `有误`?**
A: Two cases only — ① both site lanes found nothing; ② nested folders (a subfolder contains videos). **Nested folders are checked before scraping**, so no useless LLM calls are made.

**Q: Do port / LAN changes need a restart?**
A: No. They are **hot-swapped** on save (in-flight requests are not cut) and the UI prints the reachable URLs.

**Q: Others on my LAN can't open it.**
A: Make sure **LAN** is ticked, then check Windows Firewall allows the port (a prompt may appear the first time it binds `0.0.0.0`).

**Q: My series isn't recognised as a season in Jellyfin.**
A: This tool only writes `tvshow.nfo`; **episode naming / season folders** (`Title S01E01.mp4`) are up to you. A wrong layout is marked `有误` at scan time.

**Q: No cover was generated.**
A: Frame grabbing needs a decodable video; single videos `< 2 min` are skipped by rule. Use **Refresh thumbs** to build one, or **Open folder** to check for images.

**Q: Does delete go to the recycle bin?**
A: On a **NAS / network path** Windows can't recycle — it is a **permanent delete**.

---

## Credits

- LLM runtime: [ollama](https://ollama.com) and the community uncensored fine-tune authors
- Face detection: [OpenCV](https://opencv.org) YuNet
- Metadata sources: [javbus](https://www.javbus.com) · [javdb](https://javdb.com) · [MissAV](https://missav.ws) · [123AV](https://123av.com) · [FC2 official](https://adult.contents.fc2.com)
- Media server: [Jellyfin](https://jellyfin.org)

Questions / ideas: open an Issue. **Never paste cookies, keys, or video screenshots into an Issue.**

---

## License

MIT License

Copyright (c) 2026 ceiling0518

> Once again: use this project **in compliance with your local laws**; all consequences are the user's.
