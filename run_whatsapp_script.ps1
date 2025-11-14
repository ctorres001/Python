# Launcher para 02.8.AvanceVentasCortesImagen.py
# Previene bloqueos de antivirus y acelera carga

Write-Host "`n=== LAUNCHER: Script de WhatsApp ===" -ForegroundColor Cyan
Write-Host "Aplicando optimizaciones..." -ForegroundColor Green

# Desactivar bytecode compilation (evita locks del antivirus)
$env:PYTHONDONTWRITEBYTECODE = "1"

# Desactivar warnings que pueden ralentizar
$env:PYTHONWARNINGS = "ignore"

# Ejecutar con Python del venv
Write-Host "`nEjecutando script...`n" -ForegroundColor Yellow
& "D:\FNB\Proyectos\Python\.venv1\Scripts\python.exe" "D:\FNB\Proyectos\Python\02.8.AvanceVentasCortesImagen.py"

# Capturar código de salida
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "`n✓ Script finalizado correctamente" -ForegroundColor Green
} else {
    Write-Host "`n✗ Script terminó con errores (código: $exitCode)" -ForegroundColor Red
}

# Pausa para ver resultado
Read-Host "`nPresiona Enter para cerrar"
