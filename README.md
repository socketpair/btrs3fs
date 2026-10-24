# btrs3fs

Зонированное блочное устройство (host-managed zoned, ublk), данные которого хранятся в S3:
**одна зона — один объект**, имя объекта — смещение зоны в hex. Поверх устройства
монтируется btrfs в zoned-режиме.

- Запись копится в RAM и уходит в S3 дозаписью (Yandex `PATCH`) только по FLUSH/FUA
  или при превышении лимита буфера.
- Чтение — `GET` с `Range`, свежие данные — из RAM, за указателем записи — нули.
- Состояние зон восстанавливается при старте по размерам объектов (`ListObjectsV2`).

Подробности и принятые решения — `SPEC.md`, план и статус — `PLAN.md`.

## Требования

- Linux ≥ 6.10 с `CONFIG_BLK_DEV_UBLK` и `CONFIG_BLK_DEV_ZONED` (проверено на 7.2).
- Root для демона: зонированный ublk требует `UBLK_F_USER_COPY`, недоступный
  непривилегированным процессам.
- S3 с поддержкой Yandex `PATCH` (Yandex Object Storage), бакет без версионирования.
- Rust (для сборки), btrfs-progs.

## Сборка и настройка хоста (Fedora)

```bash
sudo ./setup-permissions.sh build-deps   # компилятор и заголовки
sudo ./setup-permissions.sh install      # пакеты, модуль, права для разработки
cargo build                              # target/debug/btrs3fs
```

`setup-permissions.sh install` разрешает пользователю `sudo` без пароля только для
`target/debug/btrs3fs` и `chown <user> /mnt/btrs3fs`, даёт группе `btrs3fs` доступ к
`/dev/ublkbN` и добавляет в fstab `/dev/ublkb0 /mnt/btrs3fs btrfs noauto,user`.
Откат — `sudo ./setup-permissions.sh uninstall`.

## Использование

Файл реквизитов (`chmod 600`):

```json
{
  "endpoint": "https://storage.yandexcloud.net",
  "region": "ru-central1",
  "bucket": "my-bucket",
  "access_key": "...",
  "secret_key": "..."
}
```

```bash
B=target/debug/btrs3fs

# Создать диск: <prefix>/disk.json + zoned btrfs (зона по умолчанию 16M; нужен root)
sudo $B create --creds creds.json --prefix disks/test --size 10G

# Смонтировать: демон уходит в фон, btrfs монтируется с noatime,lazytime,commit=300
sudo $B mount --creds creds.json --prefix disks/test /mnt/data [-o compress=zstd]

# Размонтировать и остановить устройство
sudo $B umount /mnt/data

# Удалить все данные диска
$B destroy --creds creds.json --prefix disks/test --yes
```

Логи — в journald (`journalctl -t btrs3fs`), без journald — в `/dev/log`; `--log-to-stderr`
дублирует их в stderr (цветные на терминале), `--log-level debug` — подробнее.

`commit=300`: без `fsync` btrfs сбрасывает данные раз в 5 минут. Данные, не подтверждённые
FLUSH/fsync, при остановке демона не дописываются (как кеш записи диска при отключении
питания) — в журнал пишется их объём. После `umount` таких данных нет.

Низкоуровневые команды: `run` (устройство без монтирования, на переднем плане — для systemd),
`detach` (остановить несмонтированное устройство), `run --memory --size 1G` (без S3, для
тестов). Параметры диска — `create --help`; сеть — `--http auto|http1|http2`, `--proxy URL`.

## systemd

```bash
sudo install -m 755 target/release/btrs3fs /usr/local/bin/
sudo install -m 644 packaging/btrs3fs@.service /etc/systemd/system/
sudo install -D -m 600 packaging/example.conf /etc/btrs3fs/main.conf   # отредактировать
sudo systemctl enable --now btrs3fs@main
```

В fstab: `/dev/ublkb0 /mnt/data btrfs _netdev,x-systemd.requires=btrs3fs@main.service 0 0` —
тогда при остановке файловая система размонтируется раньше устройства.

## Тесты

```bash
cargo test                                            # юнит-тесты
BTRS3FS_CREDS=creds.json cargo test -- --ignored      # против настоящего бакета (префикс test/)
```

## Ограничения

- Один хост на диск: защиты от одновременного подключения нет.
- Нет кеша чтения: каждое чтение сброшенных данных — запрос к S3.
- Состояние FINISH не сохраняется: после перезапуска такая зона — CLOSED (для btrfs безопасно).
- Данные, не подтверждённые FLUSH, при остановке или падении демона не сохраняются (как у диска с кешем записи).
