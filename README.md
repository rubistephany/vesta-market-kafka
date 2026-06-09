# 🛒 VestaMarket S.A. - Arquitectura de Event Streaming con Apache Kafka

Este proyecto implementa una arquitectura distribuida de procesamiento de datos en tiempo real (Event Streaming) para la simulación de transacciones de Terminales de Punto de Venta (TPVs) y el análisis analítico/operativo de la cadena de supermercados **VestaMarket S.A.**

La infraestructura y los microservicios han sido desacoplados utilizando un clúster de **Apache Kafka** bajo la arquitectura moderna **KRaft** (sin depender de ZooKeeper), garantizando tolerancia a fallos, alta disponibilidad y escalabilidad horizontal.

---

## 🛠️ Arquitectura del Sistema e Hitos Desarrollados

### 📂 Fase 1: Infraestructura y Orquestación del Clúster (`docker-compose.yml`)
* Despliegue de un nodo de **Apache Kafka** en modo **KRaft** mediante contenedores Docker encapsulados en una red virtual dedicada (`vesta-market-kafka_default`).
* Configuración de listeners internos (`29092`) para la comunicación entre microservicios y externos (`9092`) para depuración local.
* Creación determinista del tópico central **`ventas-tpv`**, configurado con **5 particiones** para habilitar el balanceo de carga y el procesamiento paralelo masivo.

### 🚀 Fase 2: Productor Financiero de Alta Fiabilidad (`productor.py`)
* Simulación automatizada de la emisión de tickets inmutables en formato **JSON** procedentes de distintas sucursales (`TIENDA_MADRID_01`, `TIENDA_BARCELONA_02`, `TIENDA_SEVILLA_03`).
* **Políticas de Fiabilidad Financiera:** Implementación de confirmación estricta mediante `acks=all` (espera de réplicas en el ISR) y mecanismos de reintento automatizados (`retries=5`) con backoff de resiliencia.
* **Estrategia de Particionado:** Uso del ID de la tienda como **Clave (Key)** del mensaje. Esto asegura que Kafka direccione los eventos de una misma sucursal a una partición fija de manera consistente, garantizando el orden cronológico estricto por tienda.

### 📦 Fase 3: Consumidor Operativo - Control de Inventario (`consumidor.py`)
* Microservicio asignado al grupo de consumo operativo (`grupo-control-stock`).
* Implementación de la política `'auto.offset.reset': 'earliest'` para asegurar la lectura completa del histórico de transacciones en caso de caída del sistema.
* Parseo del flujo crudo de bytes y procesamiento de la lógica de negocio para simular la reducción de stock en los almacenes en tiempo real conforme se procesan las colas de mensajes de cada partición.

### 📊 Fase 4: Consumidor Analítico - Panel Financiero (`analiticas.py`)
* Demostración del **Desacoplamiento Absoluto de Kafka:** Despliegue de un segundo consumidor paralelo asignado a un grupo de consumo independiente (`grupo-analiticas-financieras`).
* Este componente escucha el mismo tópico (`ventas-tpv`) sin interferir con el consumidor de stock ni duplicar almacenamiento.
* Procesa los importes en streaming para generar métricas de negocio en tiempo real: cálculo de la recaudación total acumulada de la compañía y desglose de facturación líquida por sucursal.

---

## 📸 Estructura del Repositorio y Evidencias

El repositorio cuenta con una carpeta dedicada a las capturas de pantalla que validan la consistencia de los datos en tránsito y en reposo:
* `productor.py` -> Código fuente del emisor de eventos.
* `consumidor.py` -> Código fuente del gestor de stock.
* `analiticas.py` -> Código fuente del procesador financiero.
* `evidencias/` -> Almacena capturas de la terminal demostrando el correcto comportamiento de las particiones de Kafka y la consistencia de los contadores en la nube.
