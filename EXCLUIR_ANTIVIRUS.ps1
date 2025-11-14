# Script para excluir el venv de Windows Defender
# IMPORTANTE: Ejecutar PowerShell como ADMINISTRADOR

Write-Host "`n=== CONFIGURACIÓN DE WINDOWS DEFENDER ===" -ForegroundColor Cyan
Write-Host "Este script excluirá el venv de los escaneos en tiempo real`n" -ForegroundColor Yellow

$venvPath = "D:\FNB\Proyectos\Python\.venv1"

Write-Host "Verificando permisos de administrador..." -ForegroundColor Green

# Verificar si se ejecuta como administrador
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "`n❌ ERROR: Este script requiere permisos de administrador" -ForegroundColor Red
    Write-Host "`nPara ejecutar:" -ForegroundColor Yellow
    Write-Host "1. Click derecho en PowerShell" -ForegroundColor White
    Write-Host "2. Selecciona 'Ejecutar como administrador'" -ForegroundColor White
    Write-Host "3. Ejecuta nuevamente este script`n" -ForegroundColor White
    Read-Host "Presiona Enter para cerrar"
    exit 1
}

Write-Host "✓ Permisos de administrador confirmados`n" -ForegroundColor Green

try {
    Write-Host "Agregando exclusión: $venvPath" -ForegroundColor Cyan
    Add-MpPreference -ExclusionPath $venvPath -ErrorAction Stop
    
    Write-Host "`n✅ ÉXITO: El venv ha sido excluido de Windows Defender" -ForegroundColor Green
    Write-Host "`nBeneficios:" -ForegroundColor Yellow
    Write-Host "  • Importaciones de Python 10-20x más rápidas" -ForegroundColor White
    Write-Host "  • No más KeyboardInterrupt al importar módulos" -ForegroundColor White
    Write-Host "  • Scripts se ejecutan sin demoras`n" -ForegroundColor White
    
    # Verificar exclusión
    $exclusions = Get-MpPreference | Select-Object -ExpandProperty ExclusionPath
    if ($exclusions -contains $venvPath) {
        Write-Host "✓ Verificación: Exclusión aplicada correctamente`n" -ForegroundColor Green
    }
    
} catch {
    Write-Host "`n❌ ERROR al agregar exclusión:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host "`nIntenta agregar la exclusión manualmente:" -ForegroundColor Yellow
    Write-Host "1. Abre 'Seguridad de Windows'" -ForegroundColor White
    Write-Host "2. Ve a 'Protección contra virus y amenazas'" -ForegroundColor White
    Write-Host "3. Click en 'Administrar configuración'" -ForegroundColor White
    Write-Host "4. Desplázate a 'Exclusiones' y haz click en 'Agregar o quitar exclusiones'" -ForegroundColor White
    Write-Host "5. Agrega esta carpeta: $venvPath`n" -ForegroundColor White
}

Read-Host "Presiona Enter para cerrar"
