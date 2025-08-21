import os
import json
import csv
import uuid
import shutil
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# =========================
# Config / Carga segura
# =========================

CFG_PATH = Path("C:/qcalt/config.json")

def load_cfg(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"No se encontró config.json en {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

CFG = load_cfg(CFG_PATH)

def cfg_get(key, default=None):
    return CFG.get(key, default)

VIDEO_ROOT  = Path(cfg_get("video_root", "C:/qcalt/videos"))
EVID_DIR    = Path(cfg_get("evidence_dir", "C:/qcalt/evidencia"))
TEMP_DIR    = Path(cfg_get("temp_dir", "C:/qcalt/temp"))

FFMPEG      = cfg_get("ffmpeg", "")
FFPROBE     = cfg_get("ffprobe", "")

SEG_MIN     = int(cfg_get("segment_minutes", 10))
HLS_SEC     = int(cfg_get("hls_segment_seconds", 4))
TTL_MIN     = int(cfg_get("ttl_minutes", 30))
TZ_OFFSET   = cfg_get("timezone_offset", "-06:00")
MAP_CSV     = Path(cfg_get("machine_map_csv", "C:/qcalt/machine_to_camera.csv"))
LOG_LEVEL   = cfg_get("log_level", "info")
SIMULATE    = bool(cfg_get("allow_simulation", True))

DIRECTORY_TEMPLATE = cfg_get("directory_template", "{camera_id}\\{YYYY}\\{MM}\\{DD}")
FILENAME_PATTERN   = cfg_get("filename_pattern", "{camera_id}_{YYYY}{MM}{DD}_{HH}{mm}{SS}.mp4")

# Carpetas base
for d in (VIDEO_ROOT, EVID_DIR, TEMP_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Validación FFmpeg si no simulamos
if not SIMULATE:
    missing = []
    if not (FFMPEG and Path(FFMPEG).exists()):
        missing.append("ffmpeg")
    if not (FFPROBE and Path(FFPROBE).exists()):
        missing.append("ffprobe")
    if missing:
        raise RuntimeError(
            f"Faltan binarios: {', '.join(missing)}. "
            "Activa 'allow_simulation': true en config.json para arrancar sin FFmpeg o corrige rutas."
        )

# =========================
# Utilidades
# =========================

def segment_anchor(dt: datetime, seg_min: int = SEG_MIN) -> datetime:
    """Redondea hacia abajo al inicio del segmento (p. ej., cada 10 o 60 min)."""
    mm = (dt.minute // seg_min) * seg_min
    return dt.replace(minute=mm, second=0, microsecond=0)

def offset_within_segment(dt: datetime, anchor: datetime) -> int:
    """Segundos desde el inicio del segmento hasta dt."""
    return int((dt - anchor).total_seconds())

def parse_iso_ts(ts: str) -> datetime:
    # Acepta '2025-08-01T13:16:14' y aplica TZ offset
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Timestamp ISO inválido. Ej: 2025-08-01T13:16:14")
    # Si no trae tzinfo, asignamos offset del config
    if dt.tzinfo is None:
        sign = 1 if TZ_OFFSET.startswith("+") else -1
        try:
            hh, mm = TZ_OFFSET[1:].split(":")
            offset = timezone(sign * timedelta(hours=int(hh), minutes=int(mm)))
        except Exception:
            offset = timezone(timedelta(hours=-6))
        dt = dt.replace(tzinfo=offset)
    return dt

def load_machine_map(csv_path: Path) -> List[Tuple[str, str, int]]:
    if not csv_path.exists():
        return []
    out = []
    with csv_path.open("r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            try:
                out.append((
                    (row.get("Machine_ID") or "").strip(),
                    (row.get("camera_id") or "").strip(),
                    int((row.get("priority") or "1").strip())
                ))
            except Exception:
                continue
    # Ordena por Machine_ID y priority asc (1 = principal)
    out.sort(key=lambda r: (r[0], r[2]))
    return out

MACHINE_MAP = load_machine_map(MAP_CSV)

def camera_for_machine(machine: str) -> Optional[str]:
    candidates = [c for m,c,p in MACHINE_MAP if m == machine]
    return candidates[0] if candidates else None

def build_video_path(camera_id: str, dt: datetime) -> Path:
    """Construye la ruta del archivo usando el INICIO DEL SEGMENTO para el nombre."""
    dt0 = segment_anchor(dt, SEG_MIN)
    repl = {
        "camera_id": camera_id,
        "YYYY": dt.strftime("%Y"),
        "MM": dt.strftime("%m"),
        "DD": dt.strftime("%d"),
        "HH": dt0.strftime("%H"),
        "mm": dt0.strftime("%M"),
        "SS": dt0.strftime("%S"),
    }
    dir_rel = DIRECTORY_TEMPLATE.format(**repl)
    fname = FILENAME_PATTERN.format(**repl)
    return VIDEO_ROOT / dir_rel / fname

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def cleanup_old_temp(ttl_minutes: int = TTL_MIN):
    now = datetime.now()
    for item in TEMP_DIR.glob("*"):
        try:
            mtime = datetime.fromtimestamp(item.stat().st_mtime)
            if now - mtime > timedelta(minutes=ttl_minutes):
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    item.unlink(missing_ok=True)
        except Exception:
            continue

async def periodic_cleanup():
    while True:
        cleanup_old_temp()
        await asyncio.sleep(60)  # cada minuto

def ffmpeg_cmd_exists() -> bool:
    return bool(FFMPEG and Path(FFMPEG).exists())

def run_ffmpeg(cmd: List[str]) -> int:
    from subprocess import run, DEVNULL
    return run(cmd, stdout=DEVNULL, stderr=DEVNULL).returncode

def calc_anchor_and_offset(dt: datetime) -> tuple[datetime, int]:
    """Devuelve (segment_start_datetime, offset_seconds)."""
    anchor = segment_anchor(dt, SEG_MIN)
    offset = offset_within_segment(dt, anchor)
    return anchor, offset

# =========================
# FastAPI app
# =========================

app = FastAPI(title="QC ALT Video Gateway", version="1.0")

# Servimos el directorio TEMP para HLS (m3u8/ts)
app.mount("/temp", StaticFiles(directory=str(TEMP_DIR)), name="temp")

@app.on_event("startup")
async def on_start():
    # refresca mapa por si lo actualizan antes de reiniciar
    global MACHINE_MAP
    MACHINE_MAP = load_machine_map(MAP_CSV)
    # inicia limpieza periódica
    asyncio.create_task(periodic_cleanup())

# -------------------------
# Endpoints de salud / debug
# -------------------------

@app.get("/health")
def health():
    return {
        "ok": True,
        "simulate": SIMULATE,
        "has_ffmpeg": ffmpeg_cmd_exists(),
        "temp_dir": str(TEMP_DIR),
        "ttl_minutes": TTL_MIN
    }

@app.get("/debug/config")
def debug_config():
    return {
        "video_root": str(VIDEO_ROOT),
        "evidence_dir": str(EVID_DIR),
        "temp_dir": str(TEMP_DIR),
        "ffmpeg": FFMPEG,
        "ffprobe": FFPROBE,
        "directory_template": DIRECTORY_TEMPLATE,
        "filename_pattern": FILENAME_PATTERN,
        "timezone_offset": TZ_OFFSET,
        "map_csv": str(MAP_CSV),
        "simulate": SIMULATE
    }

@app.get("/debug/resolve")
def debug_resolve(machine: str, ts: str):
    dt = parse_iso_ts(ts)
    cam = camera_for_machine(machine)
    if not cam:
        raise HTTPException(404, f"No hay cámara para Machine_ID={machine} en el CSV")
    path = build_video_path(cam, dt)
    anchor, offset = calc_anchor_and_offset(dt)
    return {
        "machine": machine,
        "camera": cam,
        "ts": dt.isoformat(),
        "segment_start": anchor.isoformat(),
        "offset_seconds": offset,
        "path": str(path),
        "exists": path.exists()
    }

# -------------------------
# Core: /view (HLS temporal)
# -------------------------

def gen_hls_from_source(src: Optional[Path], start_s: int, dur_s: int, out_dir: Path) -> tuple[Path, List[Path]]:
    """
    Genera HLS (master.m3u8 + segmentos) en out_dir.
    - Si src es None o no existe y SIMULATE=True, genera señal de prueba.
    - start_s = offset (segundos dentro del segmento).
    """
    ensure_dir(out_dir)
    m3u8_path = out_dir / "master.m3u8"
    seg_pat = str(out_dir / "seg_%03d.ts")

    if not SIMULATE and (src is None or not src.exists()):
        raise HTTPException(404, "Archivo de video no encontrado y simulación desactivada.")

    if SIMULATE and (src is None or not src.exists()):
        # Señal de prueba (no requiere archivo)
        if not ffmpeg_cmd_exists():
            raise HTTPException(500, "FFmpeg requerido incluso en modo simulado para generar HLS.")
        cmd = [
            FFMPEG, "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc=size=1280x720:rate=30",
            "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=44100",
            "-t", str(dur_s),
            "-map", "0:v:0", "-map", "1:a:0",
            "-c:v", "libx264", "-preset", "veryfast", "-profile:v", "baseline", "-level", "3.0",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "hls",
            "-hls_time", str(HLS_SEC),
            "-hls_playlist_type", "event",
            "-hls_segment_filename", seg_pat,
            str(m3u8_path)
        ]
        rc = run_ffmpeg(cmd)
        if rc != 0 or not m3u8_path.exists():
            raise HTTPException(500, "No se pudo generar HLS simulado.")
    else:
        # Fuente real
        if not ffmpeg_cmd_exists():
            raise HTTPException(500, "FFmpeg no encontrado (ver config.json).")
        cmd = [
            FFMPEG, "-hide_banner", "-loglevel", "error",
            "-ss", str(start_s), "-i", str(src),
            "-t", str(dur_s),
            "-map", "0",
            "-c:v", "libx264", "-preset", "veryfast", "-profile:v", "baseline", "-level", "3.0",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "hls",
            "-hls_time", str(HLS_SEC),
            "-hls_playlist_type", "event",
            "-hls_segment_filename", seg_pat,
            str(m3u8_path)
        ]
        rc = run_ffmpeg(cmd)
        if rc != 0 or not m3u8_path.exists():
            raise HTTPException(500, "FFmpeg no pudo generar HLS del archivo.")

    segs = sorted(out_dir.glob("seg_*.ts"))
    return m3u8_path, segs

def html_player(hls_url: str, title: str = "QC ALT Player") -> str:
    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>{title}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body {{ margin:0; background:#0b0b0b; color:#fff; font-family:ui-sans-serif,system-ui; }}
.header {{ padding:10px 14px; background:#111; border-bottom:1px solid #222; }}
main {{ padding: 10px; }}
video {{ width: 100%; max-width: 1080px; height: auto; background:#000; outline:none; border:1px solid #222; border-radius:8px; }}
small {{ color:#aaa; }}
</style>
</head>
<body>
  <div class="header">
    <div><b>{title}</b></div>
    <small>Si no reproduce, intenta abrir en Edge o Safari, o verifica acceso a <code>hls.js</code>.</small>
  </div>
  <main>
    <video id="v" controls autoplay playsinline></video>
  </main>
<script>
(function() {{
  var video = document.getElementById('v');
  var src = "{hls_url}";
  function native() {{
    if (video.canPlayType('application/vnd.apple.mpegurl')) {{
      video.src = src;
      video.play().catch(()=>{{}});
      return true;
    }}
    return false;
  }}
  if (!native()) {{
    var s = document.createElement('script');
    s.src = 'https://cdn.jsdelivr.net/npm/hls.js@latest';
    s.onload = function() {{
      if (window.Hls) {{
        var hls = new Hls({{lowLatencyMode:false}});
        hls.loadSource(src);
        hls.attachMedia(video);
        hls.on(Hls.Events.MANIFEST_PARSED, function() {{
          video.play().catch(()=>{{}});
        }});
      }}
    }};
    document.head.appendChild(s);
  }}
}})();
</script>
</body>
</html>
"""

@app.get("/view", response_class=HTMLResponse)
def view(
    machine: str = Query(..., description="Machine_ID"),
    ts: str = Query(..., description="Timestamp ISO, ej. 2025-08-01T13:16:14"),
    dur: int = Query(30, ge=5, le=600, description="Duración en segundos (5-600)")
):
    dt = parse_iso_ts(ts)
    cam = camera_for_machine(machine)
    if not cam:
        raise HTTPException(404, f"No hay cámara mapeada para Machine_ID={machine}")

    src = build_video_path(cam, dt)
    anchor, start_s = calc_anchor_and_offset(dt)

    session_id = f"{machine}_{dt.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
    out_dir = TEMP_DIR / "hls" / session_id
    ensure_dir(out_dir)

    m3u8, _ = gen_hls_from_source(src if src.exists() else None, start_s, dur, out_dir)

    rel = m3u8.relative_to(TEMP_DIR).as_posix()  # e.g. hls/abc/master.m3u8
    hls_url = f"/temp/{rel}"
    title = f"QC ALT — {machine} @ {dt.strftime('%Y-%m-%d %H:%M:%S')}"
    return HTMLResponse(content=html_player(hls_url, title=title))

# -------------------------
# Snapshot (preview) y export
# -------------------------

def gen_snapshot(src: Optional[Path], at_s: int, out_path: Path):
    ensure_dir(out_path.parent)
    if SIMULATE and (src is None or not src.exists()):
        if not ffmpeg_cmd_exists():
            raise HTTPException(500, "Se requiere FFmpeg para generar snapshot simulado.")
        cmd = [
            FFMPEG, "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc=size=1280x720:rate=1",
            "-frames:v", "1", str(out_path)
        ]
        rc = run_ffmpeg(cmd)
        if rc != 0 or not out_path.exists():
            raise HTTPException(500, "No se pudo generar snapshot simulado.")
        return

    if not ffmpeg_cmd_exists():
        raise HTTPException(500, "FFmpeg no encontrado (ver config.json).")
    if src is None or not src.exists():
        raise HTTPException(404, "Archivo de video no encontrado.")

    cmd = [
        FFMPEG, "-hide_banner", "-loglevel", "error",
        "-ss", str(at_s), "-i", str(src),
        "-frames:v", "1",
        "-q:v", "2",
        str(out_path)
    ]
    rc = run_ffmpeg(cmd)
    if rc != 0 or not out_path.exists():
        raise HTTPException(500, "FFmpeg no pudo generar snapshot.")

@app.get("/snapshot")
def snapshot(machine: str, ts: str):
    dt = parse_iso_ts(ts)
    cam = camera_for_machine(machine)
    if not cam:
        raise HTTPException(404, f"No hay cámara para Machine_ID={machine}")
    src = build_video_path(cam, dt)
    anchor, at_s = calc_anchor_and_offset(dt)

    out = TEMP_DIR / "snap" / f"{machine}_{dt.strftime('%Y%m%dT%H%M%S')}.jpg"
    gen_snapshot(src if src.exists() else None, at_s, out)
    return FileResponse(str(out), media_type="image/jpeg", filename=out.name)

@app.get("/export/snapshot")
def export_snapshot(machine: str, ts: str):
    dt = parse_iso_ts(ts)
    cam = camera_for_machine(machine)
    if not cam:
        raise HTTPException(404, f"No hay cámara para Machine_ID={machine}")
    src = build_video_path(cam, dt)
    anchor, at_s = calc_anchor_and_offset(dt)
    out = EVID_DIR / "snapshots" / f"{machine}_{dt.strftime('%Y%m%dT%H%M%S')}.jpg"
    gen_snapshot(src if src.exists() else None, at_s, out)
    return JSONResponse({"ok": True, "file": str(out)})

def export_clip_ffmpeg(src: Optional[Path], start_s: int, dur_s: int, out_path: Path):
    ensure_dir(out_path.parent)
    if SIMULATE and (src is None or not src.exists()):
        if not ffmpeg_cmd_exists():
            raise HTTPException(500, "Se requiere FFmpeg para exportar clip simulado.")
        cmd = [
            FFMPEG, "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc=size=1280x720:rate=30",
            "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=44100",
            "-t", str(dur_s),
            "-map", "0:v:0", "-map", "1:a:0",
            "-c:v", "libx264", "-preset", "veryfast", "-profile:v", "baseline", "-level", "3.0",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k",
            str(out_path)
        ]
        rc = run_ffmpeg(cmd)
        if rc != 0 or not out_path.exists():
            raise HTTPException(500, "No se pudo exportar clip simulado.")
        return

    if not ffmpeg_cmd_exists():
        raise HTTPException(500, "FFmpeg no encontrado (ver config.json).")
    if src is None or not src.exists():
        raise HTTPException(404, "Archivo de video no encontrado.")

    cmd = [
        FFMPEG, "-hide_banner", "-loglevel", "error",
        "-ss", str(start_s), "-i", str(src),
        "-t", str(dur_s),
        "-map", "0",
        "-c:v", "libx264", "-preset", "veryfast", "-profile:v", "baseline", "-level", "3.0",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k",
        str(out_path)
    ]
    rc = run_ffmpeg(cmd)
    if rc != 0 or not out_path.exists():
        raise HTTPException(500, "FFmpeg no pudo exportar el clip.")

@app.get("/export/clip")
def export_clip(machine: str, ts: str, dur: int = 30):
    dt = parse_iso_ts(ts)
    cam = camera_for_machine(machine)
    if not cam:
        raise HTTPException(404, f"No hay cámara para Machine_ID={machine}")
    src = build_video_path(cam, dt)
    anchor, start_s = calc_anchor_and_offset(dt)

    out = EVID_DIR / "clips" / f"{machine}_{dt.strftime('%Y%m%dT%H%M%S')}_{dur}s.mp4"
    export_clip_ffmpeg(src if src.exists() else None, start_s, dur, out)
    return JSONResponse({"ok": True, "file": str(out)})

# -------------------------
# Página raíz simple
# -------------------------

@app.get("/", response_class=HTMLResponse)
def home():
    return """<!doctype html>
<html lang="es"><meta charset="utf-8">
<title>QC ALT</title>
<body style="font-family:ui-sans-serif; padding:20px">
<h2>QC ALT — Gateway de Video</h2>
<ul>
  <li><a href="/docs">/docs</a> (Swagger)</li>
  <li><a href="/health">/health</a></li>
  <li>Ejemplo /view: <code>/view?machine=P703_L1&ts=2025-08-01T13:16:14&dur=30</code></li>
  <li>Ejemplo /snapshot: <code>/snapshot?machine=P703_L1&ts=2025-08-01T13:16:14</code></li>
  <li>Ejemplo /export/clip: <code>/export/clip?machine=P703_L1&ts=2025-08-01T13:16:14&dur=30</code></li>
</ul>
</body></html>"""
# =========================
# Fin del archivo
# =========================
