//! What btrfs keeps in one zone, for the zone map of the statistics page
//! (SPEC.md §9.3): the block group the zone belongs to, the files of a data
//! zone, the trees of a metadata zone. Read-only ioctls on the mounted
//! filesystem; the daemon runs as root, which they need.

use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

/// Zone of the superblock log copies, by byte offset (`fs/btrfs/zoned.c`:
/// the primary at 0, mirrors at 512 GiB and 4 TiB); each takes two zones.
const SB_LOG_OFFSETS: [u64; 3] = [0, 512 << 30, 4 << 40];
const SB_LOG_ZONES: u64 = 2;

// Trees (`BTRFS_*_TREE_OBJECTID`).
const ROOT_TREE: u64 = 1;
const EXTENT_TREE: u64 = 2;
const CHUNK_TREE: u64 = 3;
const FS_TREE: u64 = 5;
const BLOCK_GROUP_TREE: u64 = 11;
const FIRST_CHUNK_TREE_OBJECTID: u64 = 256;
const FIRST_FREE_OBJECTID: u64 = 256;

// Item types (`BTRFS_*_KEY`).
const ROOT_BACKREF_KEY: u8 = 144;
const EXTENT_ITEM_KEY: u8 = 168;
const METADATA_ITEM_KEY: u8 = 169;
const EXTENT_OWNER_REF_KEY: u8 = 172;
const TREE_BLOCK_REF_KEY: u8 = 176;
const EXTENT_DATA_REF_KEY: u8 = 178;
const SHARED_BLOCK_REF_KEY: u8 = 182;
const SHARED_DATA_REF_KEY: u8 = 184;
const BLOCK_GROUP_ITEM_KEY: u8 = 192;
const CHUNK_ITEM_KEY: u8 = 228;

/// `BTRFS_EXTENT_FLAG_TREE_BLOCK`.
const EXTENT_FLAG_TREE_BLOCK: u64 = 2;
/// `BTRFS_LOGICAL_INO_ARGS_IGNORE_OFFSET`: every file referencing the extent.
const LOGICAL_INO_IGNORE_OFFSET: u64 = 1;

/// `_IOWR(BTRFS_IOCTL_MAGIC, nr, size)`.
const fn iowr(nr: u64, size: usize) -> libc::c_ulong {
    ((3 << 30) | ((size as u64) << 16) | (0x94 << 8) | nr) as libc::c_ulong
}

/// `struct btrfs_ioctl_search_key`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SearchKey {
    tree_id: u64,
    min_objectid: u64,
    max_objectid: u64,
    min_offset: u64,
    max_offset: u64,
    min_transid: u64,
    max_transid: u64,
    min_type: u32,
    max_type: u32,
    nr_items: u32,
    unused: u32,
    unused1: u64,
    unused2: u64,
    unused3: u64,
    unused4: u64,
}

/// Bytes of items one TREE_SEARCH_V2 returns at most; more take more calls.
const SEARCH_BUF: usize = 64 << 10;

/// `struct btrfs_ioctl_search_args_v2` with its buffer.
#[repr(C)]
struct SearchArgsV2 {
    key: SearchKey,
    buf_size: u64,
    buf: [u8; SEARCH_BUF],
}

/// `struct btrfs_ioctl_logical_ino_args`.
#[repr(C)]
struct LogicalInoArgs {
    logical: u64,
    size: u64,
    reserved: [u64; 3],
    flags: u64,
    inodes: u64,
}

/// `struct btrfs_ioctl_ino_path_args`.
#[repr(C)]
struct InoPathArgs {
    inum: u64,
    size: u64,
    reserved: [u64; 4],
    fspath: u64,
}

/// `struct btrfs_ioctl_ino_lookup_args`.
#[repr(C)]
struct InoLookupArgs {
    treeid: u64,
    objectid: u64,
    name: [u8; 4080],
}

const _: () = assert!(std::mem::size_of::<SearchKey>() == 104);
const _: () = assert!(std::mem::size_of::<LogicalInoArgs>() == 56);
const _: () = assert!(std::mem::size_of::<InoPathArgs>() == 56);
const _: () = assert!(std::mem::size_of::<InoLookupArgs>() == 4096);

/// The ioctl size of TREE_SEARCH_V2 counts the fixed part only.
const BTRFS_IOC_TREE_SEARCH_V2: libc::c_ulong = iowr(17, 112);
const BTRFS_IOC_INO_LOOKUP: libc::c_ulong = iowr(18, 4096);
const BTRFS_IOC_INO_PATHS: libc::c_ulong = iowr(35, 56);
const BTRFS_IOC_LOGICAL_INO_V2: libc::c_ulong = iowr(59, 56);

/// What btrfs keeps in a zone.
#[derive(Serialize, Default)]
pub struct ZoneFs {
    /// The filesystem is mounted (nothing else is known otherwise).
    pub mounted: bool,
    /// The zone holds a copy of the superblock log.
    pub superblock: bool,
    /// The block group on the zone; `None`: btrfs has not allocated it.
    pub block_group: Option<BlockGroup>,
    /// Files with data in the zone, by path.
    pub files: Vec<FileEntry>,
    /// Data extents or inodes that could not be named: deleted meanwhile,
    /// or in a subvolume not reachable from the mount.
    pub unnamed: u64,
    /// Tree blocks in the zone, by tree and level.
    pub trees: Vec<TreeBlocks>,
    /// Why the btrfs part is incomplete.
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct BlockGroup {
    /// Logical address of the block group.
    pub logical: u64,
    pub length: u64,
    /// `data`, `metadata`, `system`, or several joined by `+`.
    pub kind: String,
    /// `single`, `DUP`, `RAID1`, …
    pub profile: String,
    /// Bytes btrfs counts as used in the block group.
    pub used: Option<u64>,
    /// Logical address the zone starts at (a block group may span zones).
    pub zone_logical: u64,
}

#[derive(Serialize)]
pub struct FileEntry {
    /// Path from the mount point.
    pub path: String,
    /// File size; `None` if it vanished since.
    pub size: Option<u64>,
}

#[derive(Serialize)]
pub struct TreeBlocks {
    pub tree: String,
    /// 0: leaves; higher: nodes.
    pub level: u8,
    pub blocks: u64,
}

/// What btrfs on `mountpoint` keeps in the zone at device bytes
/// `[zone_start, zone_start + zone_size)`.
pub fn zone_contents(mountpoint: &Path, zone_start: u64, zone_size: u64) -> ZoneFs {
    let mut out = ZoneFs {
        mounted: true,
        superblock: SB_LOG_OFFSETS.iter().any(|&o| {
            let first = o / zone_size;
            (first..first + SB_LOG_ZONES).contains(&(zone_start / zone_size))
        }),
        ..Default::default()
    };
    if let Err(e) = fill(mountpoint, zone_start, zone_size, &mut out) {
        out.error = Some(e);
    }
    out
}

fn fill(
    mountpoint: &Path,
    zone_start: u64,
    zone_size: u64,
    out: &mut ZoneFs,
) -> Result<(), String> {
    let dir =
        File::open(mountpoint).map_err(|e| format!("cannot open {}: {e}", mountpoint.display()))?;
    let fd = dir.as_raw_fd();
    let Some(chunk) = chunk_of(fd, zone_start)? else {
        return Ok(());
    };
    let zone_logical = chunk.logical + (zone_start - chunk.stripe_offset);
    let used = block_group_used(fd, chunk.logical, chunk.length)?;
    out.block_group = Some(BlockGroup {
        logical: chunk.logical,
        length: chunk.length,
        kind: kind_name(chunk.flags),
        profile: profile_name(chunk.flags),
        used,
        zone_logical,
    });
    // Items of the extent tree that start in the zone (extents never cross
    // a block group, so none starts before the block group).
    let first = chunk.logical.max(zone_logical.saturating_sub(chunk.length));
    let end = zone_logical + zone_size.min(chunk.length);
    let mut data_extents = Vec::new();
    let mut trees: BTreeMap<(u64, u8), u64> = BTreeMap::new();
    tree_search(
        fd,
        EXTENT_TREE,
        (first, 0, 0),
        (end - 1, u8::MAX, u64::MAX),
        |key, data| {
            let (bytenr, kind, offset) = key;
            match kind {
                EXTENT_ITEM_KEY | METADATA_ITEM_KEY => {
                    let length = if kind == METADATA_ITEM_KEY { 1 } else { offset };
                    if bytenr + length <= zone_logical {
                        return;
                    }
                    match parse_extent(kind, offset, data) {
                        Some(Extent::Tree { level, owner }) => {
                            *trees.entry((owner, level)).or_default() += 1
                        }
                        Some(Extent::Data) => data_extents.push(bytenr),
                        None => {}
                    }
                }
                _ => {}
            }
        },
    )?;
    out.trees = trees
        .into_iter()
        .map(|((owner, level), blocks)| TreeBlocks {
            tree: tree_name(owner),
            level,
            blocks,
        })
        .collect();
    if !data_extents.is_empty() {
        name_files(fd, mountpoint, &data_extents, out)?;
    }
    Ok(())
}

/// A chunk: where a block group lies on the device.
#[derive(Debug, PartialEq)]
struct Chunk {
    logical: u64,
    length: u64,
    flags: u64,
    /// Device offset of the stripe holding the zone.
    stripe_offset: u64,
}

/// The chunk with a stripe covering device offset `physical` (one device).
fn chunk_of(fd: i32, physical: u64) -> Result<Option<Chunk>, String> {
    let mut found = None;
    tree_search(
        fd,
        CHUNK_TREE,
        (FIRST_CHUNK_TREE_OBJECTID, CHUNK_ITEM_KEY, 0),
        (FIRST_CHUNK_TREE_OBJECTID, CHUNK_ITEM_KEY, u64::MAX),
        |(_, kind, logical), data| {
            if kind == CHUNK_ITEM_KEY
                && found.is_none()
                && let Some(c) = parse_chunk(logical, data, physical)
            {
                found = Some(c);
            }
        },
    )?;
    Ok(found)
}

/// `struct btrfs_chunk` with its stripes; `Some` if a stripe covers `physical`.
fn parse_chunk(logical: u64, data: &[u8], physical: u64) -> Option<Chunk> {
    let length = le64(data, 0)?;
    let flags = le64(data, 24)?;
    let stripes = u16::from_le_bytes(data.get(44..46)?.try_into().ok()?) as usize;
    (0..stripes).find_map(|i| {
        let offset = le64(data, 48 + i * 32 + 8)?;
        (offset <= physical && physical < offset + length).then_some(Chunk {
            logical,
            length,
            flags,
            stripe_offset: offset,
        })
    })
}

/// Used bytes of the block group at `logical`: in the extent tree, or in
/// the block group tree on filesystems made with it.
fn block_group_used(fd: i32, logical: u64, length: u64) -> Result<Option<u64>, String> {
    for tree in [EXTENT_TREE, BLOCK_GROUP_TREE] {
        let mut used = None;
        let key = (logical, BLOCK_GROUP_ITEM_KEY, length);
        match tree_search(fd, tree, key, key, |_, data| used = le64(data, 0)) {
            Ok(()) if used.is_some() => return Ok(used),
            // No block group tree on this filesystem.
            Ok(()) | Err(_) if tree == BLOCK_GROUP_TREE => return Ok(None),
            Ok(()) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}

#[derive(Debug, PartialEq)]
enum Extent {
    Data,
    /// A tree block: its level and the tree owning it (0: shared, owned
    /// through a parent block — snapshots).
    Tree {
        level: u8,
        owner: u64,
    },
}

/// An extent item (`struct btrfs_extent_item` and inline references).
fn parse_extent(kind: u8, key_offset: u64, data: &[u8]) -> Option<Extent> {
    let flags = le64(data, 16)?;
    if flags & EXTENT_FLAG_TREE_BLOCK == 0 {
        return Some(Extent::Data);
    }
    // Skinny metadata keeps the level in the key; the old format keeps a
    // `struct btrfs_tree_block_info` (a disk key and the level) first.
    let (level, mut pos) = if kind == METADATA_ITEM_KEY {
        (key_offset as u8, 24)
    } else {
        (*data.get(24 + 17)?, 24 + 18)
    };
    let mut owner = None;
    while let Some(&ref_kind) = data.get(pos) {
        let value = le64(data, pos + 1);
        pos += match ref_kind {
            TREE_BLOCK_REF_KEY | EXTENT_OWNER_REF_KEY => {
                owner = owner.or(value);
                9
            }
            SHARED_BLOCK_REF_KEY => {
                owner = owner.or(Some(0));
                9
            }
            EXTENT_DATA_REF_KEY => 29,
            SHARED_DATA_REF_KEY => 13,
            _ => break,
        };
    }
    // References not inline (many owners) are separate items: "shared".
    Some(Extent::Tree {
        level,
        owner: owner.unwrap_or(0),
    })
}

/// Names the files referencing `extents` into `out.files`.
fn name_files(fd: i32, mountpoint: &Path, extents: &[u64], out: &mut ZoneFs) -> Result<(), String> {
    let mut inodes: BTreeSet<(u64, u64)> = BTreeSet::new();
    for &bytenr in extents {
        let refs = logical_ino(fd, bytenr)?;
        if refs.is_empty() {
            out.unnamed += 1;
        }
        inodes.extend(refs);
    }
    let mut subvolumes: BTreeMap<u64, Option<(PathBuf, File)>> = BTreeMap::new();
    let mut paths = BTreeSet::new();
    for (root, inum) in inodes {
        let subvol = subvolumes
            .entry(root)
            .or_insert_with(|| open_subvolume(fd, mountpoint, root));
        let Some((dir, file)) = subvol else {
            out.unnamed += 1;
            continue;
        };
        let found = ino_paths(file.as_raw_fd(), inum).unwrap_or_default();
        if found.is_empty() {
            out.unnamed += 1;
        }
        for p in found {
            paths.insert(dir.join(p));
        }
    }
    out.files = paths
        .into_iter()
        .map(|p| FileEntry {
            size: std::fs::symlink_metadata(&p).ok().map(|m| m.len()),
            path: p
                .strip_prefix(mountpoint)
                .unwrap_or(&p)
                .display()
                .to_string(),
        })
        .collect();
    Ok(())
}

/// (root, inode) of every file referencing the data extent at `bytenr`.
fn logical_ino(fd: i32, bytenr: u64) -> Result<Vec<(u64, u64)>, String> {
    let mut size = 64 << 10;
    loop {
        let mut buf = vec![0u64; size / 8];
        let mut args = LogicalInoArgs {
            logical: bytenr,
            size: size as u64,
            reserved: [0; 3],
            flags: LOGICAL_INO_IGNORE_OFFSET,
            inodes: buf.as_mut_ptr() as u64,
        };
        // SAFETY: the kernel writes at most `size` bytes into `buf`, which
        // lives across the call.
        let r = unsafe { libc::ioctl(fd, BTRFS_IOC_LOGICAL_INO_V2, &mut args) };
        if r < 0 {
            let e = std::io::Error::last_os_error();
            // An extent freed since the search has no references.
            if e.raw_os_error() == Some(libc::ENOENT) {
                return Ok(Vec::new());
            }
            return Err(format!("LOGICAL_INO of {bytenr}: {e}"));
        }
        let bytes: Vec<u8> = buf.iter().flat_map(|w| w.to_ne_bytes()).collect();
        let (missing, values) = container(&bytes);
        // The kernel allows 16 MiB at most.
        if missing > 0 && size < (16 << 20) {
            size = (size + missing as usize).next_multiple_of(8).min(16 << 20);
            continue;
        }
        return Ok(values
            .as_chunks::<3>()
            .0
            .iter()
            .map(|v| (v[2], v[0]))
            .collect());
    }
}

/// Paths of inode `inum` inside the subvolume of `fd`, relative to it.
fn ino_paths(fd: i32, inum: u64) -> Result<Vec<String>, String> {
    let size = 64 << 10;
    let mut buf = vec![0u64; size / 8];
    let mut args = InoPathArgs {
        inum,
        size: size as u64,
        reserved: [0; 4],
        fspath: buf.as_mut_ptr() as u64,
    };
    // SAFETY: the kernel writes at most `size` bytes into `buf`.
    let r = unsafe { libc::ioctl(fd, BTRFS_IOC_INO_PATHS, &mut args) };
    if r < 0 {
        return Err(format!(
            "INO_PATHS of {inum}: {}",
            std::io::Error::last_os_error()
        ));
    }
    let bytes: Vec<u8> = buf.iter().flat_map(|w| w.to_ne_bytes()).collect();
    Ok(paths_of(&bytes))
}

/// `struct btrfs_data_container`: bytes missing and the values.
fn container(bytes: &[u8]) -> (u32, Vec<u64>) {
    let u32_at = |o: usize| u32::from_ne_bytes(bytes[o..o + 4].try_into().unwrap());
    let (missing, count) = (u32_at(4), u32_at(8) as usize);
    let values = bytes[16..]
        .as_chunks::<8>()
        .0
        .iter()
        .take(count)
        .map(|c| u64::from_ne_bytes(*c))
        .collect();
    (missing, values)
}

/// Strings of an INO_PATHS container: each value is an offset from the start
/// of the values to a NUL-terminated path.
fn paths_of(bytes: &[u8]) -> Vec<String> {
    let (_, offsets) = container(bytes);
    let values = &bytes[16..];
    offsets
        .into_iter()
        .filter_map(|o| {
            let s = values.get(o as usize..)?;
            let end = s.iter().position(|&b| b == 0)?;
            Some(String::from_utf8_lossy(&s[..end]).into_owned())
        })
        .collect()
}

/// The directory of subvolume `root` under the mount (the top-level
/// subvolume is mounted) and an open handle on it; `None` if unreachable.
fn open_subvolume(fd: i32, mountpoint: &Path, root: u64) -> Option<(PathBuf, File)> {
    let rel = subvolume_path(fd, root, 0)?;
    let dir = mountpoint.join(rel);
    let file = File::open(&dir).ok()?;
    Some((dir, file))
}

/// Path of subvolume `root` from the top-level one, by its back references.
fn subvolume_path(fd: i32, root: u64, depth: usize) -> Option<PathBuf> {
    if root == FS_TREE {
        return Some(PathBuf::new());
    }
    // Nesting deeper than this is a loop, not a filesystem.
    if depth > 64 {
        return None;
    }
    let mut found = None;
    tree_search(
        fd,
        ROOT_TREE,
        (root, ROOT_BACKREF_KEY, 0),
        (root, ROOT_BACKREF_KEY, u64::MAX),
        |(_, kind, parent), data| {
            if kind == ROOT_BACKREF_KEY && found.is_none() {
                // `struct btrfs_root_ref`: dirid, sequence, name_len, name.
                let dirid = le64(data, 0);
                let len = data
                    .get(16..18)
                    .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize);
                if let (Some(dirid), Some(len)) = (dirid, len)
                    && let Some(name) = data.get(18..18 + len)
                {
                    found = Some((parent, dirid, String::from_utf8_lossy(name).into_owned()));
                }
            }
        },
    )
    .ok()?;
    let (parent, dirid, name) = found?;
    let mut path = subvolume_path(fd, parent, depth + 1)?;
    if dirid != FIRST_FREE_OBJECTID {
        path.push(ino_lookup(fd, parent, dirid)?);
    }
    path.push(name);
    Some(path)
}

/// Path of directory `dirid` inside subvolume `tree`, from its root.
fn ino_lookup(fd: i32, tree: u64, dirid: u64) -> Option<String> {
    let mut args = InoLookupArgs {
        treeid: tree,
        objectid: dirid,
        name: [0; 4080],
    };
    // SAFETY: the ioctl fills one btrfs_ioctl_ino_lookup_args we own.
    let r = unsafe { libc::ioctl(fd, BTRFS_IOC_INO_LOOKUP, &mut args) };
    if r < 0 {
        return None;
    }
    let end = args.name.iter().position(|&b| b == 0)?;
    Some(
        String::from_utf8_lossy(&args.name[..end])
            .trim_end_matches('/')
            .to_string(),
    )
}

/// Calls `item` for every item of `tree` with a key in `[min, max]` (keys
/// compare as (objectid, type, offset)).
fn tree_search(
    fd: i32,
    tree: u64,
    min: (u64, u8, u64),
    max: (u64, u8, u64),
    mut item: impl FnMut((u64, u8, u64), &[u8]),
) -> Result<(), String> {
    // 64 KiB: on the heap.
    // SAFETY: a plain C struct of integers and bytes; all-zero is valid.
    let mut args: Box<SearchArgsV2> = unsafe { Box::new_zeroed().assume_init() };
    let mut from = min;
    loop {
        args.key = SearchKey {
            tree_id: tree,
            min_objectid: from.0,
            max_objectid: max.0,
            min_type: from.1 as u32,
            max_type: max.1 as u32,
            min_offset: from.2,
            max_offset: max.2,
            max_transid: u64::MAX,
            nr_items: u32::MAX,
            ..Default::default()
        };
        args.buf_size = SEARCH_BUF as u64;
        // SAFETY: the ioctl reads the key and writes at most `buf_size`
        // bytes of items into the buffer of the struct we own.
        let r = unsafe { libc::ioctl(fd, BTRFS_IOC_TREE_SEARCH_V2, &mut *args) };
        if r < 0 {
            return Err(format!(
                "TREE_SEARCH of tree {tree}: {}",
                std::io::Error::last_os_error()
            ));
        }
        let Some(last) = for_items(&args.buf, args.key.nr_items, &mut item) else {
            return Ok(());
        };
        if last >= max {
            return Ok(());
        }
        from = next_key(last);
    }
}

/// Walks the items a search returned; the last key, `None` if none.
fn for_items(
    buf: &[u8],
    count: u32,
    item: &mut impl FnMut((u64, u8, u64), &[u8]),
) -> Option<(u64, u8, u64)> {
    let mut pos = 0;
    let mut last = None;
    for _ in 0..count {
        // `struct btrfs_ioctl_search_header`: transid, objectid, offset, type, len.
        let objectid = le64(buf, pos + 8)?;
        let offset = le64(buf, pos + 16)?;
        let kind = u32::from_le_bytes(buf.get(pos + 24..pos + 28)?.try_into().ok()?) as u8;
        let len = u32::from_le_bytes(buf.get(pos + 28..pos + 32)?.try_into().ok()?) as usize;
        let data = buf.get(pos + 32..pos + 32 + len)?;
        let key = (objectid, kind, offset);
        item(key, data);
        last = Some(key);
        pos += 32 + len;
    }
    last
}

/// The key right after `k`.
fn next_key(k: (u64, u8, u64)) -> (u64, u8, u64) {
    match k {
        (o, t, off) if off < u64::MAX => (o, t, off + 1),
        (o, t, _) if t < u8::MAX => (o, t + 1, 0),
        (o, _, _) => (o.saturating_add(1), 0, 0),
    }
}

fn le64(data: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(data.get(at..at + 8)?.try_into().ok()?))
}

/// Block group types (`BTRFS_BLOCK_GROUP_DATA` …).
fn kind_name(flags: u64) -> String {
    let kinds: Vec<&str> = [(1, "data"), (4, "metadata"), (2, "system")]
        .into_iter()
        .filter(|&(bit, _)| flags & bit != 0)
        .map(|(_, name)| name)
        .collect();
    kinds.join("+")
}

fn profile_name(flags: u64) -> String {
    [
        (8, "RAID0"),
        (16, "RAID1"),
        (32, "DUP"),
        (64, "RAID10"),
        (128, "RAID5"),
        (256, "RAID6"),
        (512, "RAID1C3"),
        (1024, "RAID1C4"),
    ]
    .into_iter()
    .find(|&(bit, _)| flags & bit != 0)
    .map_or("single", |(_, name)| name)
    .to_string()
}

/// What a tree is for, by its id.
fn tree_name(id: u64) -> String {
    match id {
        0 => "shared by snapshots (owner through a parent block)".into(),
        1 => "root tree (where the other trees are)".into(),
        2 => "extent tree (what uses each extent)".into(),
        3 => "chunk tree (where block groups lie on the disk)".into(),
        4 => "device tree".into(),
        5 => "top-level subvolume (directories and files)".into(),
        7 => "checksum tree".into(),
        9 => "UUID tree".into(),
        10 => "free space tree".into(),
        11 => "block group tree".into(),
        12 => "RAID stripe tree".into(),
        id if id == (-9i64) as u64 => "data relocation tree (balance)".into(),
        id if id == (-6i64) as u64 => "tree log (fsync)".into(),
        id if id >= 256 && id < (-255i64) as u64 => {
            format!("subvolume {id} (directories and files)")
        }
        id => format!("tree {id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn le(v: u64) -> [u8; 8] {
        v.to_le_bytes()
    }

    #[test]
    fn chunk_stripes() {
        // A DUP chunk of 16 MiB at logical 1 GiB, stripes at 32 and 48 MiB.
        let mut data = Vec::new();
        data.extend(le(16 << 20)); // length
        data.extend(le(2)); // owner
        data.extend(le(64 << 10)); // stripe_len
        data.extend(le(4 | 32)); // metadata, DUP
        data.extend([0; 12]); // io_align, io_width, sector_size
        data.extend(2u16.to_le_bytes()); // num_stripes
        data.extend(0u16.to_le_bytes()); // sub_stripes
        for offset in [32u64 << 20, 48 << 20] {
            data.extend(le(1)); // devid
            data.extend(le(offset));
            data.extend([0; 16]); // dev uuid
        }
        let c = parse_chunk(1 << 30, &data, 48 << 20).unwrap();
        assert_eq!(
            (c.logical, c.stripe_offset, c.length),
            (1 << 30, 48 << 20, 16 << 20)
        );
        assert_eq!(
            (kind_name(c.flags).as_str(), profile_name(c.flags).as_str()),
            ("metadata", "DUP")
        );
        assert_eq!(parse_chunk(1 << 30, &data, 64 << 20), None);
    }

    #[test]
    fn extent_items() {
        let item = |flags: u64, refs: &[(u8, u64)]| {
            let mut d = Vec::new();
            d.extend(le(1)); // refs
            d.extend(le(7)); // generation
            d.extend(le(flags));
            for &(kind, value) in refs {
                d.push(kind);
                d.extend(le(value));
            }
            d
        };
        assert_eq!(
            parse_extent(EXTENT_ITEM_KEY, 4096, &item(1, &[])),
            Some(Extent::Data)
        );
        // Skinny metadata: the level is the key offset.
        assert_eq!(
            parse_extent(METADATA_ITEM_KEY, 1, &item(2, &[(TREE_BLOCK_REF_KEY, 7)])),
            Some(Extent::Tree { level: 1, owner: 7 })
        );
        assert_eq!(
            parse_extent(
                METADATA_ITEM_KEY,
                0,
                &item(2, &[(SHARED_BLOCK_REF_KEY, 1 << 30)])
            ),
            Some(Extent::Tree { level: 0, owner: 0 })
        );
    }

    #[test]
    fn search_results_and_keys() {
        let mut buf = Vec::new();
        for (objectid, kind, offset, data) in
            [(5u64, 168u32, 4096u64, &b"ab"[..]), (9, 169, 0, b"")]
        {
            buf.extend(le(1)); // transid
            buf.extend(le(objectid));
            buf.extend(le(offset));
            buf.extend(kind.to_le_bytes());
            buf.extend((data.len() as u32).to_le_bytes());
            buf.extend(data);
        }
        let mut seen = Vec::new();
        let last = for_items(&buf, 2, &mut |k, d| seen.push((k, d.to_vec())));
        assert_eq!(
            seen,
            [((5, 168, 4096), b"ab".to_vec()), ((9, 169, 0), vec![])]
        );
        assert_eq!(last, Some((9, 169, 0)));
        assert_eq!(next_key((1, 2, 3)), (1, 2, 4));
        assert_eq!(next_key((1, 2, u64::MAX)), (1, 3, 0));
        assert_eq!(next_key((1, u8::MAX, u64::MAX)), (2, 0, 0));
    }

    #[test]
    fn path_containers() {
        // Two paths: values are offsets from the start of the values.
        let mut b = vec![0u8; 16];
        b[8..12].copy_from_slice(&2u32.to_ne_bytes());
        b.extend(16u64.to_ne_bytes());
        b.extend(18u64.to_ne_bytes());
        b.extend(b"a\0dir/b\0");
        assert_eq!(paths_of(&b), ["a", "dir/b"]);
    }

    #[test]
    fn superblock_zones() {
        let z = zone_contents(Path::new("/nonexistent"), 16 << 20, 16 << 20);
        assert!(z.superblock && z.error.is_some());
        let z = zone_contents(Path::new("/nonexistent"), 32 << 20, 16 << 20);
        assert!(!z.superblock);
        assert!(zone_contents(Path::new("/x"), 512 << 30, 16 << 20).superblock);
    }
}
