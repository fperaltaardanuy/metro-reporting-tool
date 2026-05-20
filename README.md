# Metro Reporting Tool

Aplicacion de escritorio para importar datos desde ficheros Excel, almacenarlos en una base SQLite y generar indicadores mensuales sobre una plantilla de Cuadro de Mando.

## Que hace

- Importa tres ficheros Excel de entrada:
  - Solicitudes ST.
  - Solicitudes de cambio.
  - Planificacion.
- Regenera la base de datos local `metro_requests.db`.
- Calcula indicadores mensuales a partir de la base SQLite.
- Escribe los resultados en un Excel de salida o plantilla de Cuadro de Mando.
- Incluye una pestana de soporte reservada para futuras exportaciones de datos para graficas.

## Requisitos

- Python 3.
- Dependencias instaladas desde `requirements.txt`.
- Entorno con soporte para Tkinter, ya que la interfaz es de escritorio.

## Preparacion

Desde la raiz del proyecto:

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Si el entorno virtual `.venv` ya existe, no hace falta recrearlo.

## Ejecucion

```bash
cd /home/fperalta/dev/metro-reporting-tool
.venv/bin/python -m app.main
```

## Uso

### 1. Importar datos

En la pestana `Datos y reportes`, selecciona los tres Excel de entrada:

- Excel de solicitudes ST.
- Excel de solicitudes de cambio.
- Excel de planificacion.

Despues pulsa `Procesar ficheros`.

Este proceso recrea la base de datos `metro_requests.db` e importa los datos de los Excel seleccionados.

### 2. Generar indicadores mensuales

En la misma pestana, selecciona el Excel base de reporte, por ejemplo:

```text
output/CM.xlsx
```

Despues elige el mes y el ano del reporte y pulsa `Generar mensual`.

La aplicacion calcula los indicadores disponibles y escribe los valores en la hoja correspondiente del Cuadro de Mando.

### 3. Soporte para graficas

La pestana `Soporte para graficas` queda reservada para futuras exportaciones de datos, por ejemplo CSVs por indicador, agregados por periodo o datasets preparados para PowerBI u otras herramientas de visualizacion.

Por ahora no genera graficas directamente.

## Configuracion

El fichero `app_config.json` permite activar opciones de desarrollo:

```json
{
  "show_developer_actions": false
}
```

Con `show_developer_actions` en `false`, la interfaz muestra solo las acciones normales de usuario.

Si se cambia a `true`, se muestra una accion adicional de desarrollo para generar mensuales desde enero de 2024 hasta el mes seleccionado. Esta opcion esta pensada para pruebas y no para uso ordinario.

## Archivos importantes

- `app/main.py`: punto de entrada de la aplicacion.
- `app/ui/main_window.py`: interfaz grafica.
- `app/importers/`: importadores de Excel.
- `app/db/`: modelos, sesion y recreacion de base de datos.
- `app/services/monthly_indicator_service.py`: calculo de indicadores.
- `app/services/monthly_template_writer.py`: escritura de resultados en el Excel de salida.
- `metro_requests.db`: base SQLite local generada por la aplicacion.
- `inputs/`: carpeta habitual para los Excel de entrada.
- `output/`: carpeta habitual para el Cuadro de Mando generado o actualizado.

## Notas de uso

- El boton `Procesar ficheros` recrea la base de datos local. Si habia datos previos en `metro_requests.db`, se sustituyen por los datos importados.
- Antes de generar indicadores, asegurese de haber procesado los Excel correctos.
- Cierre el Excel de salida antes de generar el reporte para evitar bloqueos de escritura.
- Los ficheros reales de entrada, salida, base de datos y PDFs estan ignorados por Git.
