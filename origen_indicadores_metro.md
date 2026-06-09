# Origen de los indicadores del cuadro de mando

## Objetivo de este documento

Este documento resume, de forma práctica, **de dónde sale cada indicador que ya está implementado** en la herramienta.

La idea es que sirva para explicar al equipo:

- qué **Excel de entrada** alimenta cada dato,
- en qué **tablas de la base de datos** queda guardado,
- y cuál es la **lógica general de cálculo** de cada cifra.

No pretende detallar el código, sino dejar claro el origen funcional de cada resultado.

---

## 1. Ficheros de entrada y cómo se traducen a la base de datos

Actualmente la herramienta trabaja con tres grandes fuentes de información:

### 1.1. Excel de solicitudes / ST

Es la fuente principal de las **solicitudes de trabajo**.

De aquí salen, entre otros, datos como:

- identificador de la solicitud,
- fechas de solicitud,
- estados de la solicitud,
- estados de entrega,
- aprobación,
- fechas planificadas,
- fecha de entrega,
- importe/presupuesto de la solicitud,
- y algunos flags específicos usados por indicadores.

En la base de datos, esta información se guarda principalmente en:

- `requests`
- tablas auxiliares relacionadas como:
  - `request_statuses`
  - `work_statuses`
  - `approval_statuses`
  - `priorities`
  - `functional_areas`
  - etc.

### 1.2. Excel de planificación

Es la fuente principal para todo lo relacionado con **planificación estimada y real**.

De aquí salen, entre otros, datos como:

- líneas de planificación estimada y real,
- horas imputadas por semana,
- responsables,
- códigos de informe / perfiles (`report_code`),
- costes unitarios de cada perfil,
- horas de referencia por perfil,
- importes de solicitudes,
- flags como `less_48h`, `different_profiles`, `specific_profiles`,
- presupuesto mensual del contrato.

En la base de datos, esta información se traduce sobre todo en:

- `planning_items`
- `planning_lines`
- `planning_time_values`
- `report_codes`
- `responsibles`
- `monthly_budgets`

Además, parte de la información de este Excel actualiza campos en `requests`, por ejemplo:

- `amount`
- `less_48h`
- `different_profiles`
- `specific_profiles`

### 1.3. Excel de solicitudes de cambio

Es la fuente de las **change requests** o solicitudes de cambio.

De aquí salen datos como:

- identificador de la solicitud de cambio,
- fecha de solicitud,
- estado,
- impactos,
- flags como:
  - `created_work_request_flag`
  - `modified_work_request_flag`
- y, cuando aplica, el vínculo con una solicitud de trabajo (`request`).

En la base de datos, esta información se guarda principalmente en:

- `change_requests`
- y en sus tablas auxiliares:
  - `change_request_statuses`
  - `change_types`
  - `contract_impacts`
  - `work_request_impacts`
  - `stakeholder_impacts`
  - `service_impacts`
  - etc.

---

## 2. Idea general del cálculo de costes y presupuestos

Varios indicadores no se basan solo en contar registros, sino en calcular **costes**.

La lógica general es esta:

1. En planificación, las horas están en `planning_time_values`.
2. Cada línea de planificación está en `planning_lines`.
3. Cada línea tiene un `report_code`.
4. El coste por hora de ese `report_code` se obtiene de `report_codes`:

   - `coste_hora = unit_price / at_unit_hours`

5. El coste de una línea o de un conjunto de horas se obtiene multiplicando:

   - `horas * coste_hora`

Cuando se habla de:

- **plan estimado**, se usan líneas `source_type = "estimated"`
- **plan real**, se usan líneas `source_type = "real"`

Con esta misma lógica se calculan varios indicadores de cumplimiento de planificación o desviación de presupuesto.

---

## 3. Indicadores implementados

A continuación se recogen los indicadores que ya se han definido en este desarrollo.

---

## IN01-EFEC-IL — Cumplimiento Planificación Presupuestaria

**Qué mide**  
Qué porcentaje supone el gasto real acumulado del año respecto al presupuesto acumulado del año.

**De dónde sale**

- **Presupuesto acumulado**: de `monthly_budgets`
  - suma de `amount`
  - desde enero del año seleccionado hasta el mes elegido
- **Gasto acumulado**: de planificación real
  - `planning_lines` con `source_type = "real"`
  - `planning_time_values` para las horas
  - `report_codes` para obtener el coste por hora

**Lógica general**

1. Se suman los presupuestos mensuales desde enero hasta el mes elegido.
2. Se suman los costes reales de planificación del mismo periodo.
3. Se divide:

`gasto_real_acumulado / presupuesto_acumulado`

---

## IN02-EFEC-IL — Cumplimiento de la Planificación Estratégica

**Qué mide**  
Qué porcentaje supone el coste real del mes respecto al coste estimado del mes.

**De dónde sale**

- `planning_lines`
- `planning_time_values`
- `report_codes`

**Lógica general**

1. Para cada `report_code`, se obtiene el coste por hora.
2. Se calcula el coste del mes con líneas `estimated`.
3. Se calcula el coste del mes con líneas `real`.
4. Se divide:

`coste_real_mes / coste_estimado_mes`

**Regla especial**

- Se excluye el `report_code` `IDATGENGES01`.

---

## IN03-EFEC-IL — Cumplimiento de Planificación Estratégica

**Qué mide**  
Para cada perfil o `report_code` del bloque del cuadro de mando, cuánto se ha seguido la planificación estimada respecto a la real.

**De dónde sale**

- `planning_lines`
- `planning_time_values`

**Lógica general**

Para cada `report_code` mostrado en el Excel de salida:

1. Se suman las horas del mes con `source_type = "estimated"`.
2. Se suman las horas del mes con `source_type = "real"`.
3. Se divide:

`horas_reales / horas_estimadas`

**Importante**

Este indicador no es una sola cifra: se calcula **por perfil** y se vuelca en varias filas del Excel.

---

## IN04-EFEC-IP — % ST Evaluadas <= 48h

**Qué mide**  
El porcentaje de solicitudes registradas en el año que han sido evaluadas en menos de 48 horas.

**De dónde sale**

- `requests.request_date`
- `requests.less_48h`

**Lógica general**

1. Se cuentan las `requests` registradas desde el inicio del año hasta el mes elegido.
2. De esas, se cuentan las que tienen `less_48h = true`.
3. Se calcula el porcentaje.

---

## IN05-EFEC-IL — % ST Terminadas con desviación de plazo

**Qué mide**  
Qué porcentaje de solicitudes entregadas se han entregado con desviación de plazo respecto a la fecha planificada.

**De dónde sale**

- `requests.planned_start_date`
- `requests.planned_end_date`
- `requests.work_status_date`
- `requests.work_status_id`
- `work_statuses.name`

**Lógica general**

1. Se toman las solicitudes entregadas en lo que va de año.
2. Solo se tienen en cuenta las que tienen fechas planificadas válidas.
3. Se compara `work_status_date` con `planned_end_date`.
4. Si no coinciden, se considera que hay desviación.
5. Se calcula el porcentaje sobre el total de entregadas válidas.

---

## IN06-EFEC-IL — % ST Terminadas con desviación de presupuesto

**Qué mide**  
Qué porcentaje de solicitudes entregadas tienen una desviación de presupuesto superior al 15% o inferior al -15%.

**De dónde sale**

- `requests.amount`
- `requests.work_status_date`
- `requests.work_status_id`
- `work_statuses.name`
- `planning_items.request_id`
- `planning_lines` (`source_type = "real"`)
- `planning_time_values`
- `report_codes`

**Lógica general**

1. Se toman las solicitudes entregadas en lo que va de año.
2. Solo cuentan las que tienen `amount` válido y distinto de 0.
3. Se calcula el gasto real de cada request a partir de la planificación real vinculada a ella.
4. Se compara el gasto real con el presupuesto `amount`.
5. Si la desviación es mayor de +15% o menor de -15%, esa request cuenta como desviada.
6. Se calcula el porcentaje sobre el total de entregadas válidas.

---

## IN07-EFEC-IP — % ST Modificadas

**Qué mide**  
Qué porcentaje suponen las solicitudes de cambio que modifican una ST respecto al volumen de solicitudes de referencia definido para el indicador.

**De dónde sale**

- `change_requests.modified_work_request_flag`
- `change_requests.request_date`
- `requests.request_date`
- y la lógica de denominador acordada para este indicador

**Lógica general**

1. Se cuentan las `change_requests` del año hasta el mes elegido con `modified_work_request_flag = true`.
2. Se obtiene el denominador definido funcionalmente para este indicador.
3. Se calcula el porcentaje.

**Nota**

Este indicador ha tenido varios ajustes de definición durante el desarrollo, por lo que conviene revisarlo siempre junto con la regla de negocio vigente.

---

## IN08-EFEC-IP — % Modificaciones detonan nueva ST

**Qué mide**  
Qué porcentaje de solicitudes de cambio del año generan una nueva solicitud de trabajo.

**De dónde sale**

- `change_requests.request_date`
- `change_requests.created_work_request_flag`

**Lógica general**

1. Se toman las `change_requests` del año hasta el mes elegido.
2. Se cuentan las que tienen `created_work_request_flag = true`.
3. Se calcula el porcentaje sobre el total de `change_requests` del mismo periodo.

---

## IN10-EFEC-IL — Tasa media de desviación presupuesto

**Qué mide**  
La desviación media de presupuesto de las solicitudes entregadas.

**De dónde sale**

Las mismas fuentes que en **IN06**:

- `requests.amount`
- `requests.work_status_date`
- `work_statuses`
- `planning_items`
- `planning_lines`
- `planning_time_values`
- `report_codes`

**Lógica general**

1. Se toman las solicitudes entregadas en lo que va de año.
2. Solo cuentan las que tienen `amount` válido y distinto de 0.
3. Para cada una se calcula la desviación porcentual entre gasto real y presupuesto.
4. Se hace la media de todas esas desviaciones.

---

## IN11-EFEC-IA — % ST con desviación de presupuesto mensual

**Qué mide**  
Qué porcentaje de solicitudes abiertas tiene desviación de presupuesto mensual superior al 15% o inferior al -15%.

**De dónde sale**

- `requests`
- `approval_statuses`
- `request_statuses`
- `planning_items`
- `planning_lines`
- `planning_time_values`
- `report_codes`

**Lógica general**

1. Se localizan las requests abiertas según la definición funcional actual.
2. Para cada una, se calcula el coste estimado del mes.
3. Para esa misma request, se calcula el coste real del mes.
4. Se obtiene la desviación porcentual mensual.
5. Se cuenta cuántas están fuera del margen ±15%.
6. Se calcula el porcentaje sobre las abiertas para las que el cálculo es posible.

---

## IN12-EFEC-IL — Tasa media desviación plazo

**Qué mide**  
La desviación media de plazo de las solicitudes entregadas, en porcentaje.

**De dónde sale**

- `requests.planned_start_date`
- `requests.planned_end_date`
- `requests.work_status_date`
- `work_statuses.name`

**Lógica general**

1. Se toman las solicitudes entregadas en lo que va de año.
2. Se calcula el plazo planificado:
   - `planned_end_date - planned_start_date`
3. Se calcula la duración real:
   - `work_status_date - planned_start_date`
4. Se obtiene la desviación porcentual de cada request.
5. Se hace la media de esas desviaciones.

---

## IN13-CALS-IR — % ST con desviación en perfiles

**Qué mide**  
Qué porcentaje de solicitudes entregadas presenta desviación de perfiles.

**De dónde sale**

- `requests.different_profiles`
- `requests.work_status_date`
- `requests.work_status_id`
- `work_statuses.name`

**Lógica general**

1. Se toman las solicitudes entregadas acumuladas hasta el fin del mes elegido.
2. Se cuentan las que tienen `different_profiles = true`.
3. Se calcula el porcentaje sobre el total de entregadas.

---

## IN14-CALS-IR — % ST con perfiles específicos

**Qué mide**  
Qué porcentaje de solicitudes aprobadas por MdM tiene perfiles específicos.

**De dónde sale**

- `requests.specific_profiles`
- `requests.request_date`
- `requests.approval_status_id`
- `approval_statuses.name`

**Lógica general**

1. Se toman las solicitudes con `approval_status = "MdM Aprobada"` y `request_date` hasta fin de mes.
2. Se cuentan las que tienen `specific_profiles = true`.
3. Se calcula el porcentaje sobre el total de solicitudes aprobadas por MdM del periodo acumulado.

---

## IN17-CALS-IR — N.º Personas en ejecución

**Qué mide**  
El número de responsables distintos que han participado en la planificación real del mes.

**De dónde sale**

- `planning_lines.source_type`
- `planning_lines.responsible_id`
- `planning_time_values.year`
- `planning_time_values.month`

**Lógica general**

1. Se toman las líneas de planificación `real` del mes.
2. Se mira qué responsables aparecen en ellas.
3. Se cuentan los responsables distintos.

---

## IN18-CALS-IR — N.º Perfiles en ejecución

**Qué mide**  
El número de perfiles o `report_code` distintos que han participado en la planificación real del mes.

**De dónde sale**

- `planning_lines.source_type`
- `planning_lines.report_code`
- `planning_time_values.year`
- `planning_time_values.month`

**Lógica general**

1. Se toman las líneas de planificación `real` del mes.
2. Se cuentan los `report_code` distintos.

**Reglas especiales aplicadas**

- `IDATGENGES01` no se cuenta.
- `IDATGENAGN02` e `IDATGENAGD01` se consideran el mismo perfil a efectos de este indicador.

---

## IN19-CALS-IA — Número de ETC en ejecución

**Qué mide**  
Cuántos ETC equivalentes representan las horas reales dedicadas por el equipo en el mes, excluyendo al Director de Servicio.

**De dónde sale**

- `planning_lines.source_type`
- `planning_lines.report_code`
- `planning_time_values.hours`

**Lógica general**

1. Se suman las horas reales del mes.
2. Se excluyen las horas del `report_code` `IDATGENGES01`.
3. Se divide por el ETC mensual de referencia del contrato.

---

## IN20-EFIC-II — N.º ST Nuevas

**Qué mide**  
Cuántas solicitudes se han solicitado en el mes.

**De dónde sale**

- `requests.request_date`

**Lógica general**

1. Se cuentan las `requests` cuya `request_date` cae dentro del mes elegido.

---

## IN21-EFIC-IA — N.º ST Abiertas

**Qué mide**  
Número de solicitudes abiertas según la definición funcional actual.

**De dónde sale**

- `requests.approval_status_id`
- `requests.request_status_id`
- `approval_statuses.name`
- `request_statuses.name`

**Definición de abierta**

Una request se considera abierta cuando:

- `approval_status = "MdM Aprobada"`
- y `request_status` no es:
  - `Cancelada`
  - `Cerrada`
  - `Rechazada`
- si `request_status` es `NULL`, se considera abierta

---

## IN22-EFIC-IA — N.º ST Entregadas

**Qué mide**  
Número de solicitudes entregadas en el mes.

**De dónde sale**

- `requests.work_status_date`
- `requests.work_status_id`
- `work_statuses.name`

**Lógica general**

1. Se cuentan las `requests` cuyo `work_status = "Entregado"`.
2. Además, `work_status_date` debe caer dentro del mes elegido.

---

## IN23-EFIC-IA — N.º ST Cerradas

**Qué mide**  
Número de solicitudes que han pasado a estado cerrado en el mes.

**De dónde sale**

- `requests.request_status_date`
- `requests.request_status_id`
- `request_statuses.name`

**Lógica general**

1. Se cuentan las `requests` con `request_status = "Cerrada"`.
2. Además, `request_status_date` debe caer dentro del mes elegido.

---

## IN24-EFIC-IA — N.º Solicitudes de Cambios de ST

**Qué mide**  
Número total de solicitudes de cambio acumuladas en el año hasta el mes elegido.

**De dónde sale**

- `change_requests.request_date`

**Lógica general**

1. Se cuentan las `change_requests` con `request_date` entre:
   - 1 de enero del año elegido
   - fin del mes elegido

---

## IN25-EFIC-IP — Número total de solicitudes anuladas/canceladas/rechazadas

**Qué mide**  
Actualmente se está usando para contar solicitudes canceladas según la definición operativa implementada.

**De dónde sale**

- `requests.request_status_id`
- `request_statuses.name`

**Lógica general**

1. Se cuentan las `requests` cuyo `request_status = "Cancelada"`.

**Nota**

Si más adelante se decide incluir también `Anulada` o `Rechazada`, este indicador se puede ampliar fácilmente.

---

## IN26-EFIC-IR — % ST en curso

**Qué mide**  
Qué porcentaje de solicitudes está actualmente en curso respecto al total de solicitudes abiertas.

**De dónde sale**

- `requests.request_status_id`
- `request_statuses.name`
- y el denominador de `IN21`

**Lógica general**

1. Se cuentan las `requests` con `request_status = "En Curso"`.
2. Se divide por el número de solicitudes abiertas (`IN21`).

**Nota**

Con los datos actuales, este indicador puede quedar muy condicionado por la forma en que origen distingue o no distingue entre “abierta” y “en curso”.

---

## IN27-EFIC-II — % ST Entregadas

**Qué mide**  
Qué porcentaje suponen las solicitudes entregadas del mes respecto al total de solicitudes abiertas.

**De dónde sale**

- resultado de `IN22`
- resultado de `IN21`

**Lógica general**

1. Se obtiene el número de solicitudes entregadas del mes.
2. Se divide por el número de solicitudes abiertas.

---

## IN28-EFIC-IA — % Dedicación del Director de Servicio

**Qué mide**  
El porcentaje de dedicación real del Director de Servicio respecto a su referencia contractual.

**De dónde sale**

- `planning_lines.source_type = "real"`
- `planning_lines.report_code = "IDATGENGES01"`
- `planning_time_values.hours`
- `report_codes.at_unit_hours`

**Lógica general**

1. Se suman las horas reales del mes del `report_code` `IDATGENGES01`.
2. Se toma `at_unit_hours` de ese mismo `report_code`.
3. Se calcula el porcentaje.

---

## 4. Observaciones finales

- La herramienta **regenera la base de datos completa en cada ejecución** a partir de los Excel actuales.
- Por tanto, los indicadores no se calculan sobre históricos “guardados” de ejecuciones previas, sino sobre el estado reconstruido a partir de los ficheros cargados en ese momento.
- En varios indicadores se han aplicado reglas para **evitar cálculos imposibles**:
  - excluir registros inválidos,
  - no dividir entre cero,
  - devolver `"-"` o `"error"` cuando el cálculo no es interpretable.
- Algunos indicadores han evolucionado durante el desarrollo porque la definición funcional ha ido afinándose. Por eso conviene usar este documento como referencia operativa de lo que actualmente hace la herramienta.

---

## 5. Resumen rápido de tablas más importantes

### Solicitudes
- `requests`
- `request_statuses`
- `work_statuses`
- `approval_statuses`

### Planificación
- `planning_items`
- `planning_lines`
- `planning_time_values`
- `report_codes`
- `responsibles`
- `monthly_budgets`

### Solicitudes de cambio
- `change_requests`
- `change_request_statuses`
- tablas auxiliares de impactos y tipos

---

Si se desea, este documento se puede ampliar más adelante con ejemplos concretos de cada indicador o con equivalencias entre nombres de columnas del Excel y campos de la base de datos.
