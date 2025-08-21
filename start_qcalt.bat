@echo off
setlocal

REM --- carpetas base ---
set QCALT_DIR=C:\qcalt
set VENV_DIR=%QCALT_DIR%\venv

REM --- crear carpetas si faltan ---
if not exist "%QCALT_DIR%\evidencia" mkdir "%QCALT_DIR%\evidencia"
if not exist "%QCALT_DIR%\temp" mkdir "%QCALT_DIR%\temp"

REM --- activar venv si existe (opcional) ---
if exist "%VENV_DIR%\Scripts\activate.bat" (
  call "%VENV_DIR%\Scripts\activate.bat"
)

REM --- verificar python y libs clave ---
python -c "import fastapi,uvicorn,pydantic;print('OK')" 1>nul 2>nul
if errorlevel 1 (
  echo [QC ALT] Faltan dependencias. Instalando...
  python -m pip install -U pip
  python -m pip install fastapi uvicorn[standard] pydantic python-multipart
)

REM --- lanzar servidor ---
cd /d "%QCALT_DIR%"
echo [QC ALT] Iniciando servidor en http://localhost:8000 ...
python -m uvicorn server:app --host 0.0.0.0 --port 8000 --reload

endlocal
