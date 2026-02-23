# Cron

## Purpose

Cron provides cron job scheduling library for scheduling recurring tasks and background jobs.

## Type

Shared library

## Consumed By

- Deployment-service
- Data-manager-service
- Symbol-manager-service
- Any service with scheduled tasks

## Exposed Interface

**Key Functions**:

- `New()`: Create cron instance
- `AddFunc(spec string, cmd func())`: Add cron job
- `Start()`: Start scheduler
- `Stop()`: Stop scheduler
- `RemoveEntry(id)`: Remove scheduled job

**Cron Spec Format**:
```
Field        Allowed Values
-----        --------------
Minute       0-59
Hour         0-23
Day of Month 1-31
Month        1-12
Day of Week  0-6 (0=Sunday)
```

## Usage Example

```go
import "github.com/arconomy/cron"

c := cron.New()
c.AddFunc("0 * * * *", func() {
    // Run every hour
})
c.Start()
defer c.Stop()
```

---

**Last Updated**: 2026-02-22



After ANY change to this service that affects:
- API surface (new/modified endpoints or gRPC methods)
- NATS subjects (new publish/subscribe calls)
- Data models (schema/migration changes)
- Config vars (new env vars)
- Inter-service dependencies (new gRPC clients or NATS consumers)

You MUST update this service's AGENTS.md to reflect the change before committing.
Update the root ./AGENTS.md if the change affects the architecture graph,
NATS subject registry, or gRPC relationship table.

---

**Last Updated**: 2026-02-23
