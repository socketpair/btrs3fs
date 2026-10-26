# btrs3fs

Диск в S3-бакете с файловой системой btrfs: зонированное блочное устройство Linux (ublk),
**одна зона — один объект** в S3 (Yandex Object Storage, Selectel и другие), btrfs в zoned-режиме
поверх.

```bash
sudo btrs3fs create --creds creds.json --prefix disks/data --size 10G
sudo btrs3fs mount  --creds creds.json --prefix disks/data /mnt/data
sudo btrs3fs umount /mnt/data
```

## Документация

| Документ | Для кого |
|---|---|
| [`manual.md`](manual.md) | Пользователям: установка, команды, параметры, сохранность данных, systemd, типичные ситуации. |
| [`SPEC.md`](SPEC.md) | Разработчикам: решения, устройство, проверенные факты, замеры, отложенное. |
| [`PLAN.md`](PLAN.md) | План реализации и статус. |

## Разработка

Хост для разработки (Fedora) настраивает `setup-permissions.sh` (подробности — `SPEC.md` §10):

```bash
sudo ./setup-permissions.sh build-deps   # компилятор, заголовки, clippy, rustfmt
sudo ./setup-permissions.sh install      # пакеты, модуль, права для разработки
sudo ./setup-permissions.sh diag-tools   # fio, blktrace, awscli2 и т. п.
cargo build                              # target/debug/btrs3fs
```

`install` разрешает `sudo` без пароля только для `target/debug/btrs3fs` и
`chown <user> /mnt/btrs3fs`, даёт группе `btrs3fs` доступ к `/dev/ublkbN` и добавляет в fstab
`/dev/ublkb0 /mnt/btrs3fs btrfs noauto,user`. Откат — `sudo ./setup-permissions.sh uninstall`.

Тесты:

```bash
cargo test                                            # юнит-тесты
BTRS3FS_CREDS=creds.json cargo test -- --ignored      # против настоящего бакета (префикс test/)
cargo clippy --all-targets && cargo fmt --check
```
