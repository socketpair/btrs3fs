# btrs3fs — план реализации

Пошаговый план до готового решения. Требования и решения — в `SPEC.md`; здесь — порядок
работ и статус. После каждого этапа — коммит и отметка здесь.

## Архитектура кода

```
src/
  main.rs      CLI (clap): create / run / detach / destroy
  config.rs    disk.json (DiskConfig + проверки), creds.json (Creds)
  store.rs     трейт ObjectStore (put / patch / get_range / delete / list / get) + MemStore
  s3.rs        S3Store: reqwest + aws-sigv4 + quick-xml, повторы с экспонентой
  zones.rs     движок зон: состояния, буферы, указатели, FLUSH, лимиты open/active,
               грязные данные — без ublk, покрыт юнит-тестами на MemStore
  device.rs    ublk: параметры устройства, очереди, разбор команд, REPORT_ZONES,
               мост в tokio через eventfd, остановка
```

Ключевые решения по коду:
- `ObjectStore` — асинхронный трейт; `MemStore` (HashMap в памяти) нужен для этапа 1,
  юнит-тестов и режима `run --memory` (без S3).
- Сектора в `ublksrv_io_desc` всегда по 512 байт, даже при логическом блоке 4096.
- Мост: на каждый тег очереди — свой eventfd. Задача IO отправляет работу в tokio
  (`Handle::spawn`), сама ждёт `opcode::Read` на eventfd через `UblkQueue::ublk_submit_sqe`;
  tokio кладёт результат в общий слот и пишет в eventfd. Причина: поток очереди спит в
  io_uring (таймаут libublk — секунды), обычный кросс-поточный waker его не разбудит.
- Остановка: SIGTERM/SIGINT → `kill_dev()` → потоки очередей завершаются (новых записей
  больше нет) → финальный сброс всех зон в S3 с бюджетом повторов → `del_dev()`.

## Этап 1 — каркас и движок зон в памяти

- [x] 1.1 Проект Cargo, зависимости, CLI-скелет.
- [x] 1.2 `config.rs`: `DiskConfig` (значения по умолчанию, проверки §2), `Creds`.
- [x] 1.3 `store.rs`: трейт `ObjectStore` + `MemStore`.
- [x] 1.4 `zones.rs`: состояния зон, WRITE / ZONE_APPEND / READ / FLUSH / FUA,
      OPEN / CLOSE / FINISH / RESET / RESET_ALL, REPORT, лимиты open/active,
      учёт грязных данных; восстановление из `list()`. Юнит-тесты.
- [x] 1.5 `device.rs`: ublk-устройство (`USER_COPY | ZONED`, 4K-сектор, volatile cache + FUA),
      очереди, мост eventfd, `run --memory`.
- [x] 1.6 Проверка на реальном устройстве: `blkzone report`, `fio` (zonemode=zbd, verify),
      `mkfs.btrfs -O zoned`, mount, запись/чтение файлов. Опытная проверка zone write plugging.
      Сделано: устройство host-managed, 320 зон, блок 4096, write back + FUA; `blkzone`
      report/reset; запись не по указателю → EIO; `fio` zbd 4×1 ГиБ с verify — без ошибок
      (iodepth 8 обычных записей на зону — ни одной не по указателю ⇒ zone write plugging
      работает); `mkfs.btrfs -O zoned` и mount пользователем — чисто в журнале ядра.
      Запись и чтение файлов — см. 4.1.

## Этап 2 — S3-клиент

- [x] 2.1 `s3.rs`: подпись SigV4 (`UNSIGNED-PAYLOAD`), PUT / PATCH / GET Range / DELETE /
      ListObjectsV2 (пагинация) / GET целиком; транспорт (keep-alive, `TCP_NODELAY`,
      TCP keepalive, `--http`); повторы (5xx/429/сеть, бюджет из `disk.json`).
- [x] 2.2 Интеграционные тесты против бакета (`#[ignore]`, запускаются явно, свой префикс).
- [x] 2.3 `create` / `destroy` через S3.

## Этап 3 — устройство на S3

- [x] 3.1 `run` с S3: загрузка `disk.json`, восстановление зон из List, сброс PUT/PATCH,
      чтение Range-GET.
- [x] 3.2 Лимит грязных данных: фоновая выгрузка при `dirty_limit_bytes`, ожидание при 2×.
- [x] 3.3 Остановка: финальный сброс, выход с кодом ≠ 0 и логом потерь при исчерпании
      бюджета; `detach` (проверка `O_EXCL`, SIGTERM демону, ожидание).

## Этап 4 — тесты целиком

- [x] 4.1 Персистентность: mkfs → файлы + контрольные суммы → umount → detach → run →
      mount → сверка.
      Диск 10 ГиБ в S3: 227 файлов (42 МБ: крупные, 200 мелких, дерево каталогов, symlink) →
      sync → umount → detach → run (8 закрытых зон восстановлено) → mount → все sha256
      совпали, ошибок btrfs в журнале ядра нет.
      Повторено 2026-10-24 с зоной 16 МиБ (640 зон): 227 файлов, 40 МБ → тот же цикл →
      9 зон восстановлено (8 closed, 1 full) → все sha256 совпали.
- [x] 4.2 `fio` с verify на сыром зонированном устройстве поверх S3.
      2×64 МиБ, fsync каждые 32 записи → detach → run (4 полные зоны восстановлены) →
      `--verify_only` читает всё из S3: `err=0`.
- [x] 4.3 Отказ S3 (неверный endpoint) → `EIO` на FLUSH, корректный выход при остановке.
      Сделано через локальный CONNECT-прокси (`--proxy`), бюджет повторов 10 с: при «обрыве»
      fsync → `EIO` (~16 с), данные остаются в RAM и читаются; после восстановления FLUSH
      досылает их; `detach` при обрыве — через бюджет лог «data lost: zone N, X bytes»,
      устройство удаляется, код выхода 2. (Пересмотрено 2026-10-24: при остановке буферы
      не досылаются вовсе, см. SPEC §10.)

## Этап 6 — монтирование и логи (запрос пользователя 2026-10-24)

- [x] 6.1 Логи: journald → `/dev/log` → (ничего); `--log-to-stderr` (цвет на терминале);
      итоговая ошибка команды — всегда в stderr.
- [x] 6.2 `serve()` с колбэком готовности и программной остановкой; `create` делает mkfs
      на временном устройстве, откат при ошибке.
- [x] 6.3 `mount`: fork демона до запуска потоков, готовность по pipe, `mount -t btrfs -o
      noatime,lazytime,commit=300[,…]`, остановка демона при ошибке монтирования.
- [x] 6.4 `umount`: точка → устройство по mountinfo → umount → detach.
- [x] 6.5 Проверка: create → mount → файлы → umount → mount → сверка; журнал в journald.
- [x] 6.6 README, systemd unit, CLAUDE.md.
      Проверено: `umount` снял диск, смонтированный через fstab; `create` (4 ГиБ) с mkfs — 7 с;
      `mount` — 1,6 с, опции `noatime,lazytime,commit=300`, демон в фоне, повторный mount
      на занятую точку — ошибка в stderr; 37 файлов → `umount` (отброшенного нет) → `mount` →
      sha256 совпали; запись без fsync + detach → в журнале «not storing 3145728 bytes»;
      логи — в journald (`journalctl -t btrs3fs`).

## Этап 7 — аварии

- [x] 7.1 `kill -9` демона на смонтированной ФС (`detach --force`): ядро убирает
      `/dev/ublkbN`, btrfs прерывает транзакцию, «emergency shutdown», ФС размонтирована;
      `detach` удаляет устройство с мёртвым сервером; повторный `mount` из S3 — без ошибок
      в журнале ядра; 11/11 файлов после `sync`/`fsync` целы, 10 файлов без fsync
      (после последнего commit) пропали — ожидаемо при `commit=300`; 37 старых файлов целы.

## Этап 8 — статистика (запрос пользователя 2026-10-24)

- [x] 8.1 Счётчики и гистограммы, сокет `/run/btrs3fs/<N>.sock`, `btrs3fs stats [--watch] [--json]`.
      Проверено: `fio` 4×16 МиБ с fsync каждые 8 записей на устройстве поверх S3 — 158
      выгрузок (4 PUT + 154 PATCH) на 63,5 МиБ, средняя ~410 КиБ, до 4 одновременно,
      2–4 зоны на FLUSH.
- [x] 8.2 По просьбе пользователя команда `stats` и Unix-сокет заменены веб-страницей в демоне
      (`http://127.0.0.1:9470+N/`, обновление каждую секунду, `/stats.json`).

## Этап 5 — поставка

- [x] 5.1 systemd unit для `run` (`packaging/btrs3fs@.service`, `Type=notify` через `sd_notify`).
- [x] 5.2 README: сборка, настройка, команды, ограничения.
