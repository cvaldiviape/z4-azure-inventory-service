# Inventory service

Servicio responsable de reservar y liberar stock durante la Saga de compra.

## Configuración local

| Propiedad | Valor predeterminado |
|---|---|
| Puerto HTTP | `8083` |
| PostgreSQL | `localhost:5435/inventory_db` |
| Kafka | `localhost:9092` |
| Consumer group ID | `inventory-event-consumer-group-id` |

Flyway crea y modifica el esquema mediante `db/migration`. Hibernate utiliza `ddl-auto: validate` únicamente para comprobar que las entidades coincidan con las tablas; no crea ni altera la estructura.

## Kafka

`InventoryEventConsumer` permanece a la escucha de:

```text
orders-events-topic
inventory-events-topic
```

Las estrategias ejecutadas son:

```text
ORDER_CREATED → ReserveStockEventStrategy
RELEASE_STOCK → ReleaseStockEventStrategy
```

El servicio publica sus resultados en:

```text
inventory-events-topic
```

## Variables disponibles

```text
INVENTORY_DB_URL
INVENTORY_DB_USERNAME
INVENTORY_DB_PASSWORD
KAFKA_BOOTSTRAP_SERVERS
SPRING_PROFILES_ACTIVE
```

## Ejecutar y depurar

Desde la raíz del proyecto:

```bash
docker compose -f infra/docker-compose.yml up -d kafka inventory-postgres
```

Después ejecuta `InventoryServiceApplication` con **Debug** en IntelliJ.

Para compilar sin ejecutar pruebas:

```bash
./mvnw clean package -DskipTests
```
