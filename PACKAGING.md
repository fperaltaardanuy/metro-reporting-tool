# Empaquetado de la aplicacion

Esta guia describe como generar ejecutables de la aplicacion con PyInstaller.

La aplicacion debe empaquetarse en el mismo sistema operativo en el que se va a
usar:

- Para Windows 11, generar el ejecutable en Windows.
- Para Fedora Linux, generar el ejecutable en Fedora.

PyInstaller no genera ejecutables Windows desde Linux de forma directa.

## Recomendacion inicial

Para este proyecto se recomienda empezar con `--onedir`.

`--onedir` genera una carpeta completa dentro de `dist/`, con el ejecutable y
sus dependencias. Es menos compacto que `--onefile`, pero suele ser mas estable
para aplicaciones con `pandas`, `numpy`, `openpyxl`, `SQLAlchemy`, `sqlite` y
`tkinter`.

Cuando el empaquetado este probado, se puede valorar `--onefile`, pero no es la
primera opcion para distribuir internamente.

## Preparacion en Windows 11

1. Instalar Python 3 desde python.org.

   Durante la instalacion, activar la opcion:

   ```text
   Add python.exe to PATH
   ```

2. Clonar el repositorio:

   ```powershell
   git clone <URL_DEL_REPOSITORIO>
   cd metro-reporting-tool
   ```

3. Crear el entorno virtual:

   ```powershell
   python -m venv .venv
   ```

4. Activar el entorno:

   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

   Si PowerShell bloquea la activacion por politica de ejecucion, usar:

   ```powershell
   Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
   .\.venv\Scripts\Activate.ps1
   ```

5. Instalar dependencias:

   ```powershell
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   python -m pip install pyinstaller
   ```

6. Probar la aplicacion sin empaquetar:

   ```powershell
   python -m app.main
   ```

## Generar ejecutable en Windows 11

Desde la raiz del repositorio, con el entorno virtual activado:

```powershell
python -m PyInstaller --onedir --windowed --name MetroReportingTool app/main.py
```

El resultado queda en:

```text
dist\MetroReportingTool\
```

El ejecutable principal sera:

```text
dist\MetroReportingTool\MetroReportingTool.exe
```

Para distribuirlo a otros usuarios, comprimir la carpeta completa:

```text
dist\MetroReportingTool\
```

No distribuir solo el `.exe`, porque en modo `--onedir` necesita el resto de
archivos de la carpeta.

## Preparacion en Fedora Linux

Desde la raiz del proyecto:

```bash
python -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install pyinstaller
```

Probar la aplicacion:

```bash
.venv/bin/python -m app.main
```

## Generar ejecutable en Fedora Linux

```bash
.venv/bin/python -m PyInstaller --onedir --windowed --name metro-reporting-tool app/main.py
```

El resultado queda en:

```text
dist/metro-reporting-tool/
```

El ejecutable principal sera:

```text
dist/metro-reporting-tool/metro-reporting-tool
```

## Configuracion incluida

El fichero `app_config.json` se usa para activar o desactivar opciones de
desarrollo. Al empaquetar, si se quiere distribuir una configuracion concreta,
conviene dejarlo en:

```json
{
  "show_developer_actions": false
}
```

Actualmente la aplicacion busca `app_config.json` en la raiz del proyecto. Si
en el futuro se quiere distribuir la aplicacion sin depender de ficheros junto
al codigo fuente, habra que ajustar la resolucion de rutas para ejecutables
empaquetados.

## Base de datos y ficheros de trabajo

La aplicacion crea y usa `metro_requests.db` en el directorio desde el que se
lanza el programa.

Para uso de equipo, se recomienda:

- Mantener los Excel de entrada en una carpeta conocida del usuario.
- Mantener el Cuadro de Mando de salida en una carpeta donde el usuario tenga
  permisos de escritura.
- Cerrar el Excel de salida antes de pulsar botones que escriben sobre el
  fichero.

## Limpieza de builds

PyInstaller crea estas carpetas y ficheros:

```text
build/
dist/
MetroReportingTool.spec
```

En Fedora, con el nombre indicado:

```text
build/
dist/
metro-reporting-tool.spec
```

Si se quiere rehacer el empaquetado desde cero, se pueden borrar `build/`,
`dist/` y el fichero `.spec` generado.

## Posibles problemas

### Windows muestra aviso de seguridad

Es normal que Windows SmartScreen avise con ejecutables internos no firmados.
Para evitarlo en distribuciones formales haria falta firmar el ejecutable con
un certificado de codigo.

### El ejecutable no abre

Generar temporalmente una version con consola ayuda a ver errores:

```powershell
python -m PyInstaller --onedir --name MetroReportingTool app/main.py
```

Despues ejecutar desde PowerShell:

```powershell
.\dist\MetroReportingTool\MetroReportingTool.exe
```

### Falta Tkinter

En Windows, Python de python.org normalmente incluye Tkinter.

En Fedora, si falta Tkinter:

```bash
sudo dnf install python3-tkinter
```

Despues recrear el entorno virtual si fuese necesario.

