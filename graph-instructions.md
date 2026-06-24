
## Gráfico: ST Creadas lo que va de año

  Fuente: CM
  Tipo de datos:
  - IN20-EFIC-IA

  Periodo: año hasta mes seleccionado

  Formato:
  - Título: ST Creadas en {AÑO}
  - Fila 1: Meses
  - Fila 2: IN20-EFIC-IA pero acumulando lo de los meses pasados YTD.



## Gráfico: ST entregadas y porcentaje de entregadas

  Fuente: CM
  Tipo de datos:
  - IN22-EFIC-IA
  - IN27-EFIC-II

  Periodo: seleccionado por usuario

  Formato:
  - Título: ST entregadas y % ST entregadas
  - Fila 1: Meses
  - Fila 2: IN22-EFIC-IA
  - Fila 3: IN27-EFIC-II

## Gráfico: ST en curso

  Fuente: CM
  Tipo de datos:
  - IN21-EFIC-IA

  Periodo: año hasta el mes elegido

  Formato:
  - Título: ST en curso
  - Fila 1: Meses
  - Fila 2: IN21-EFIC-IA

## Gráfico: ST cerradas

  Fuente: CM
  Tipo de datos:
  - IN23-EFIC-IA

  Periodo: año hasta el mes elegido

  Formato:
  - Título: ST cerradas
  - Fila 1: Meses
  - Fila 2: IN23-EFIC-IA pero acumulando lo de los meses pasados YTD.

## Gráfico: Tipo de actividad del servicio

  Fuente: SQLite
  Tabla/base: requests + service_activity_types + approval_statuses

  Filtro:
  - Solicitudes creadas dentro del año seleccionado hasta mes seleccionado

  Agrupación:
  - Por tipo de actividad

  Formato:
    - Título: Tipo de actividad del servicio
    - Fila 1: Tipo de actividad
    - Fila 2: Porcentaje de ST sobre el total de ST creadas en el año hasta el mes seleccionado. La fila debe sumar 100%, porque se usará para un pie chart.

  ## Gráfico: Grupo de interés

  Fuente: SQLite
  Tabla/base: requests + request_interest_group_activity_types + interest_group_activity_types

  Filtro:
  - Solicitudes creadas dentro del año seleccionado hasta mes seleccionado

  Agrupación:
  - Grupo de interés. En el caso de tener varios, se agrupa con aquellos que también tenga esos varios. Es para un pie chart.

  Formato:
    - Título: Grupo de interés
    - Fila 1: Grupo de interés
    - Fila 2: Porcentaje de ST que tiene esa combinación de grupos de interés y solo esa

  ## Gráfico: Ámbito funcional

  Fuente: SQLite
  Tabla/base: requests + functional_areas

  Filtro:
  - Solicitudes creadas dentro del año seleccionado hasta mes seleccionado

  Agrupación:
  - Por área funcional

  Formato:
    - Título: Ámbito funcional
    - Fila 1: Área funcional
    - Fila 2: Número de ST

  ## Gráfico: Sistemas

  Fuente: SQLite
  Tabla/base: requests + request_systems + systems

  Filtro:
  - Solicitudes creadas dentro del año seleccionado hasta mes seleccionado

  Agrupación:
  - System. En el caso de tener varios, se agrupa con aquellos que también tenga esos varios.

  Formato:
    - Título: Sistemas
    - Fila 1: Sistema
    - Fila 2: Número de ST

## Gráfico: Solicitudes de cambios de ST

  Fuente: CM
  Tipo de datos:
  - IN24-EFIC-IA

  Periodo: año hasta el mes elegido

  Formato:
  - Título: Solicitudes de cambios de ST
  - Fila 1: Meses
  - Fila 2: IN24-EFIC-IA

## Gráfico: %Modificaciones detonan nueva ST

  Fuente: CM
  Tipo de datos:
  - IN08-EFEC-IP

  Periodo: año hasta el mes elegido

  Formato:
  - Título: %Modificaciones detonan nueva ST
  - Fila 1: Meses
  - Fila 2: IN08-EFEC-IP, formato porcentaje

## Gráfico: %ST Modificadas respecto a aprobadas

  Fuente: CM
  Tipo de datos:
  - IN07-EFEC-IP

  Periodo: año hasta el mes elegido

  Formato:
  - Título: %ST Modificadas respecto a aprobadas
  - Fila 1: Meses
  - Fila 2: IN07-EFEC-IP, formato porcentaje

## Gráfico: Total de ST por area

Fuente: SQLite
Tabla/base: requests + functional_areas

Filtro:
- Todas las solicitudes existentes

Agrupación:
- Por área funcional general. Esto es, las áreas funcionales que hay son:
CISIC, CISIC - IICC Y E4.0, CISIC - SAAF, CISIC - STMAS CTR CTRL, OGP, OGP - DIGITALIZACIÓN, OGP - ECONÓMICA, OGP - PMO, SISF - ENERGÍA, SISF - SEÑALIZACIÓN, SISIE, SISIE - INST Y CTRL, SISIE - TELECOMUNICACIONES, SISIE - TICKETING, SISIE - TRANSPORTE VERTICAL, ÁREA. Si ves, tienen como comienzos comunes entre varias de ellas. Aquí hay que agrupar por esos comienzos: SISIE, Área, CISIC, OGP, SISF.

Formato:
- Título: ST anuladas
- Fila 1: Área funcional
- Fila 2: Porcentaje sobre el total

## Gráfico: ST en curso

Fuente: SQLite
Tabla/base: requests + functional_areas

Filtro:
- Todas las solicitudes existentes que estén en curso. (Ver como se calcula el IN21-EFIC-IA, misma forma de obtenerlas) 

Agrupación:
- Por área funcional general. Esto es, las áreas funcionales que hay son:
CISIC, CISIC - IICC Y E4.0, CISIC - SAAF, CISIC - STMAS CTR CTRL, OGP, OGP - DIGITALIZACIÓN, OGP - ECONÓMICA, OGP - PMO, SISF - ENERGÍA, SISF - SEÑALIZACIÓN, SISIE, SISIE - INST Y CTRL, SISIE - TELECOMUNICACIONES, SISIE - TICKETING, SISIE - TRANSPORTE VERTICAL, ÁREA. Si ves, tienen como comienzos comunes entre varias de ellas. Aquí hay que agrupar por esos comienzos: SISIE, Área, CISIC, OGP, SISF.


Formato:
- Título: ST anuladas
- Fila 1: Área funcional
- Fila 2: Porcentaje sobre el total

## Gráfico: ST cerradas

Fuente: SQLite
Tabla/base: requests + functional_areas

Filtro:
- Todas las solicitudes existentes que estén en curso. (Ver como se calcula el IN23-EFIC-IA, misma forma de obtenerlas pero en lugar de para un mes concreto es el total que están cerradas) 

Agrupación:
- Por área funcional general. Esto es, las áreas funcionales que hay son:
CISIC, CISIC - IICC Y E4.0, CISIC - SAAF, CISIC - STMAS CTR CTRL, OGP, OGP - DIGITALIZACIÓN, OGP - ECONÓMICA, OGP - PMO, SISF - ENERGÍA, SISF - SEÑALIZACIÓN, SISIE, SISIE - INST Y CTRL, SISIE - TELECOMUNICACIONES, SISIE - TICKETING, SISIE - TRANSPORTE VERTICAL, ÁREA. Si ves, tienen como comienzos comunes entre varias de ellas. Aquí hay que agrupar por esos comienzos: SISIE, Área, CISIC, OGP, SISF.


Formato:
- Título: ST anuladas
- Fila 1: Área funcional
- Fila 2: Porcentaje sobre el total

## Gráfico: ST anuladas

Fuente: SQLite
Tabla/base: requests + functional_areas

Filtro:
- Todas las solicitudes existentes que estén en curso. (Ver como se calcula el IN25-EFIC-IA, misma forma de obtenerlas) 

Agrupación:
- Por área funcional general. Esto es, las áreas funcionales que hay son:
CISIC, CISIC - IICC Y E4.0, CISIC - SAAF, CISIC - STMAS CTR CTRL, OGP, OGP - DIGITALIZACIÓN, OGP - ECONÓMICA, OGP - PMO, SISF - ENERGÍA, SISF - SEÑALIZACIÓN, SISIE, SISIE - INST Y CTRL, SISIE - TELECOMUNICACIONES, SISIE - TICKETING, SISIE - TRANSPORTE VERTICAL, ÁREA. Si ves, tienen como comienzos comunes entre varias de ellas. Aquí hay que agrupar por esos comienzos: SISIE, Área, CISIC, OGP, SISF.


Formato:
- Título: ST anuladas
- Fila 1: Área funcional
- Fila 2: Porcentaje sobre el total

## Tabla de soporte: Totales de ST para porcentajes

Fuente: SQLite
Tabla/base: requests + approval_statuses + request_statuses

Objetivo:
- Dar soporte a las gráficas que expresan porcentajes sobre el total de ST, ST en curso, ST cerradas y ST anuladas.

Formato:
- Título: Totales de ST para porcentajes
- Fila 1: Total de ST, ST en curso, ST cerradas, ST anuladas
- Fila 2: Número total de cada categoría a día de fin del periodo elegido

Notas:
- Total de ST: total de solicitudes existentes en base de datos.
- ST en curso: misma regla que IN21-EFIC-IA.
- ST cerradas: solicitudes con estado `Cerrada`.
- ST anuladas: solicitudes con estado `Cancelada`.


## Gráfico: Cumplimiento de la Planificación Estratégica

  Fuente: CM
  Tipo de datos:
  - IN02-EFEC-IL

  Periodo: año hasta el mes elegido

  Formato:
  - Título: Cumplimiento de la Planificación estratégica
  - Fila 1: Meses
  - Fila 2: IN02-EFEC-IL, formato porcentaje

 ## Gráfico: Cumplimiento de la Planificación Funcional

  Fuente: CM
  Tipo de datos:
  - IN03-EFEC-IL

  Periodo: año hasta el mes elegido

  Formato:
  - Título: Cumplimiento de la Planificación funcional
  - Fila 1: Meses
  - Fila 2: IN03-EFEC-IL, IDATGENAGD01 fila 12 del excel formato porcentaje 
  - Fila 3: IN03-EFEC-IL, IDATGENAGN02 fila 13 del excel formato porcentaje 
  - Fila 4: IN03-EFEC-IL, IDATGENPRE01 fila 14 del excel formato porcentaje 
  - Fila 5: IN03-EFEC-IL, IDATGENSSP01 fila 15 del excel formato porcentaje 
  - Fila 6: IN03-EFEC-IL, IDATGENDEL02 fila 17 del excel formato porcentaje 

  Nota: este es un indicador que tiene sus datos en varias filas. De ahí el como he puesto que para cada fila que hay que generar el dato porcentual se halla en una fila del excel diferente. El código que sale antes de cuando pongo la fila es el "título de la fila".

## Gráfico: %ST Entregadas con desviación de plazo sobre las ST entregadas

  Fuente: CM
  Tipo de datos:
  - IN05-EFEC-IL

  Periodo: año hasta el mes elegido

  Formato:
  - Título: %ST Entregadas con desviación de plazo
  - Fila 1: Meses
  - Fila 2: IN05-EFEC-IL, formato porcentaje

  ## Gráfico: %ST Entregadas con desviación de presupuesto sobre las ST entregadas

  Fuente: CM
  Tipo de datos:
  - IN06-EFEC-IL

  Periodo: año hasta el mes elegido

  Formato:
  - Título: %ST Entregadas con desviación de presupuesto
  - Fila 1: Meses
  - Fila 2: IN06-EFEC-IL, formato porcentaje

  ## Gráfico: Tasa media de desviación de plazo en ST entregadas

  Fuente: CM
  Tipo de datos:
  - IN12-EFEC-IL

  Periodo: año hasta el mes elegido

  Formato:
  - Título: Tasa de media de desviación de plazo
  - Fila 1: Meses
  - Fila 2: IN12-EFEC-IL, formato porcentaje

  ## Gráfico: Tasa media de desviación de presupuesto en ST entregadas

  Fuente: CM
  Tipo de datos:
  - IN10-EFEC-IL

  Periodo: año hasta el mes elegido

  Formato:
  - Título: Tasa de media de desviación de plazo
  - Fila 1: Meses
  - Fila 2: IN10-EFEC-IL, formato porcentaje

  ## Gráfico: Planificación Presupuestaria

  Fuente: SQLite
  Tabla/base: requests + planning_lines + planning_time_values + report_codes + monthly_budget

  Filtro:
  - todas las solicitudes existentes

  Nota: hay que obtener el gasto de cada mes gracias a las planning lines, los planning time values y los report codes. Se obtienen las horas realizadas en el mes para cada report code y multiplicando dichas horas por unit_price/at_unit_hours queda el gasto mensual para el report code, se suman todos y queda el gasto real total del mes.

  Formato:
    - Título: Planificación Presupuestaria
    - Fila 1: Meses
    - Fila 2: Planificación Estratégica. Cálculo de gasto mensual para planning_lines con source = "estimated".
    - Fila 3: Planificación Económica. Monthly budget del mes
    - Fila 4: Ejecución Material. Cálculo de gasto mensual para planning_lines con source = "real".

  ## Gráfico: Cumplimiento Planificación Presupuestaria

  Fuente: CM
  Tipo de datos:
  - IN01-EFEC-IL

  Periodo: año hasta el mes elegido

  Formato:
  - Título: Cumplimiento Planificación Presupuestaria
  - Fila 1: Meses
  - Fila 2: IN01-EFEC-IL, formato porcentaje

  ## Gráfico: Facturación en el año

  Fuente: SQLite
  Tabla/base: requests + planning_lines + planning_time_values + report_codes + monthly_budget

  Filtro:
  - todas las solicitudes existentes
  - planning_lines con source = "real"

  Nota: hay que obtener el gasto de cada mes gracias a las planning lines, los planning time values y los report codes. Se obtienen las horas realizadas en el mes para cada report code y multiplicando dichas horas por unit_price/at_unit_hours queda el gasto mensual para el report code, se suman todos y queda el gasto real total del mes.

  Formato:
    - Título: Facturación {{año}}
    - Fila 1: Meses
    - Fila 2: Facturación Acumulada. El cálculo de la nota acumulado desde principio de año al mes en cuestión. Mejor calcular primero fila 3 y utilizar para fila 2. Para los meses posteriores al mes elegido, mantener la cifra acumulada del mes elegido hasta finalizar el año. Formato moneda
    - Fila 3: Facturación Mensual. El cálculo indicado en la nota para el mes en concreto que estemos trabajando. Formato moneda
    - Fila 4: Facturación objetivo {{año}}. Obtener la suma de todos los monthy budgets del año elegido, restar un 15% de beneficio industrial/GG, BI y poner esa misma cifra para todos los meses (la idea es que luego para la gráfica queda como linea horizontal). Formato moneda

  ## Gráfico: Facturación en el año con GG, BI

  Fuente: SQLite
  Tabla/base: requests + planning_lines + planning_time_values + report_codes + monthly_budget

  Filtro:
  - todas las solicitudes existentes
  - planning_lines con source = "real"

  Nota: es igual que el gráfico "Facturación en el año", pero aplicando un 15% adicional de beneficio industrial/GG, BI a la facturación mensual. En esta tabla el objetivo conserva la suma anual completa de monthly budgets, porque representa el objetivo con beneficio industrial/GG, BI.

  Formato:
    - Título: Facturación {{año}} con GG, BI
    - Fila 1: Meses
    - Fila 2: Facturación Acumulada. Facturación mensual con el 15% adicional acumulada desde principio de año. Para los meses posteriores al mes elegido, mantener la cifra acumulada del mes elegido hasta finalizar el año. Formato moneda
    - Fila 3: Facturación Mensual. Facturación mensual multiplicada por 1,15. Formato moneda
    - Fila 4: Facturación objetivo {{año}}. Suma anual completa de monthly budgets repetida para todos los meses. Formato moneda

  ## Gráfico: Número de ETCs en ejecución

  Fuente: CM
  Tipo de datos:
  - IN19-CALS-IA

  Periodo: año hasta el mes elegido

  Formato:
  - Título: Número de ETCs en ejecución
  - Fila 1: Meses
  - Fila 2: IN19-CALS-IA

  ## Gráfico: Dedicación del Director de Servicio 

  Fuente: CM
  Tipo de datos:
  - IN28-EFIC-IA

  Periodo: año hasta el mes elegido

  Formato:
  - Título: Dedicación del Director de Servicio
  - Fila 1: Meses
  - Fila 2: IN28-EFIC-IA, formato porcentaje

  ## Gráfico: Tiempo de respuesta en la valoración de solicitudes <= 48h

  Fuente: CM
  Tipo de datos:
  - IN04-EFEC-IP

  Periodo: año hasta el mes elegido

  Formato:
  - Título: Tiempo de respuesta en la valoración de solicitudes <= 48h
  - Fila 1: Meses
  - Fila 2: IN04-EFEC-IP, formato porcentaje
