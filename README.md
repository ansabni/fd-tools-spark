# fd-tools-spark

Spark implementation of the **FD mix pipeline** (`mix_maprule`).

Reads `stdtime.*` footprint descriptor files from Linode Object Storage (S3),
applies mix_maprule blending logic via C++ JNI, and uploads the resulting
`stdtime.<suffix>` output file back to S3 — encrypted with SSE-C.

This replaces the standalone `mix_maprule` binary for cloud/distributed
execution while keeping all computation in the existing C++ codebase.

These `stdtime.*` input files are produced upstream by **fd-compute-spark**.

---

## How It Works

```
Linode Object Storage
  <MIX_MAPRULE_IDIR>/stdtime.mm1.keyN   (SSE-C encrypted)
  <MIX_MAPRULE_IDIR>/stdtime.w143.keyN
        │
        │  Download + SSE-C decrypt (driver, then broadcast to executor)
        ▼
  /tmp/fd_mix_<id>/input/stdtime.mm1
  /tmp/fd_mix_<id>/input/stdtime.w143
        │
        │  MIX_MAPRULE_CONFIG downloaded from S3, broadcast as string
        ▼
  C++ FD_Calculus::process() via JNI (FD_JNI.cpp)
  Config: "mm1 add 1\nw143 add 1"
        │
        ▼
  /tmp/fd_mix_<id>/output/stdtime.<suffix>
        │
        │  SSE-C encrypt + upload
        ▼
Linode Object Storage
  <MIX_MAPRULE_ODIR>/stdtime.<suffix>.keyN
```

---

## Repository Layout

```
fd-tools-spark/
├── compile_native.sh          ← Step 1: compile C++ on your target platform
├── run_pipeline_job.sh        ← Step 3: run the Spark job
├── build.sbt                  ← Scala 2.13 / Spark 4.1 build file
├── conf/
│   └── encryption_keys.json   ← SSE-C AES keys (one entry per key index)
├── cpp_src/                   ← All C++ source files
│   ├── FD_JNI.cpp             ← JNI bridge (Spark entry point)
│   ├── FD_Calculus.cpp/h      ← Core mix computation
│   ├── FD.cpp/h               ← FD data structure
│   ├── FD_QuickAdd.cpp/h      ← Fast accumulation helpers
│   ├── FD_Quota.h             ← Quota definitions
│   ├── FD_stdtime.cpp/h       ← stdtime file I/O
│   ├── FD_stdspace.cpp/h      ← stdspace file I/O
│   ├── FD_stdcount.cpp/h      ← stdcount file I/O
│   ├── now.cpp/h              ← Timing utility
│   ├── mix_maprule.cpp        ← Standalone binary (reference / local testing)
│   ├── t2s.cpp                ← stdtime → stdspace conversion binary
│   ├── t2c.cpp                ← stdtime → stdcount conversion binary
│   ├── concat_stdtime.cpp     ← concat binary
│   └── unshard.cpp            ← unshard binary
├── lib/                       ← Native library (output of compile_native.sh)
│   ├── libFD.dylib            ← macOS — used for local testing
│   └── libFD.so               ← Linux — used on Spark cluster
└── src/main/scala/com/fd/
    ├── FDPipelineJob.scala    ← Spark job
    └── FDNative.scala         ← JNI interface declaration
```

---

## Deliverables

| File | Portable? | Notes |
|---|---|---|
| `target/scala-2.13/fd-spark_2.13-0.1.jar` | ✅ Any JVM | Pure Scala bytecode |
| `lib/libFD.so` | ❌ Platform-specific | Must be **recompiled** on target Linux |
| `lib/libFD.dylib` | ❌ macOS arm64 only | For local Mac testing only |
| `run_pipeline_job.sh` | ✅ | Shell script, no changes needed |
| `conf/encryption_keys.json` | ✅ | Keep secret — contains AES keys |

**The `.so` / `.dylib` MUST be compiled on the machine/OS where Spark will run.**
A macOS `.dylib` will not load on a Linux cluster.

---

## Setup

### Step 1 — Compile the Native Library

Run on the **target platform** (the machine where Spark executors will run):

```bash
cd fd-tools-spark
./compile_native.sh
```

This produces:
- `lib/libFD.dylib` (macOS) or `lib/libFD.so` (Linux) — loaded by Spark via JNI
- Standalone binaries in `cpp_src/`: `mix_maprule`, `t2s`, `t2c`, `concat`, `unshard`

**Linux prerequisites:**
```bash
sudo apt-get install -y g++ openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

**macOS prerequisites:**
```bash
brew install openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
```

---

### Step 2 — Build the Scala Jar

```bash
cd fd-tools-spark
sbt package
# → target/scala-2.13/fd-spark_2.13-0.1.jar
```

The jar is pre-built and included if distributed. Only rebuild if you modify Scala code.

---

### Step 3 — Run

Set the three required `MIX_MAPRULE_*` env vars and run:

```bash
cd fd-tools-spark

MIX_MAPRULE_IDIR=Anirudh/fds \
MIX_MAPRULE_ODIR=Anirudh/mix_output \
MIX_MAPRULE_CONFIG=Anirudh/config.key2 \
./run_pipeline_job.sh
```

With all optional parameters:

```bash
MIX_MAPRULE_IDIR=Anirudh/fds \
MIX_MAPRULE_ODIR=Anirudh/mix_output \
MIX_MAPRULE_CONFIG=Anirudh/config.key2 \
MIX_MAPRULE_SUFFIX=mymix \
MIX_MAPRULE_THINNING_PCT_JUMP=5.0 \
S3_OUTPUT_KEY_INDEX=2 \
FD_LOCAL_DEBUG_DIR=/tmp/fd_debug_out \
./run_pipeline_job.sh
```

---

## Configuration Reference

### S3 Credentials

| Variable | Description |
|---|---|
| `S3_ACCESS_KEY` | Linode Object Storage access key |
| `S3_SECRET_KEY` | Linode Object Storage secret key |

### MIX_MAPRULE_* — S3 paths (mirror the standalone binary's env vars)

| Variable | Required | Description |
|---|---|---|
| `MIX_MAPRULE_IDIR` | ✅ | S3 prefix containing `stdtime.*` input files. All `stdtime.*` files under this prefix are downloaded. E.g. `Anirudh/fds` |
| `MIX_MAPRULE_ODIR` | ✅ | S3 prefix for output upload. E.g. `Anirudh/mix_output` |
| `MIX_MAPRULE_CONFIG` | ✅ | S3 path to the config file. E.g. `Anirudh/config.key2` |
| `MIX_MAPRULE_SUFFIX` | ✗ | Output filename suffix → `stdtime.<suffix>`. Defaults to config filename stem (e.g. `config` from `config.key2`) |
| `MIX_MAPRULE_THINNING_PCT_JUMP` | ✗ | Thinning percentage jump (same as `mix_maprule -j`). Default `0.0` = no thinning |

> Values can be bare S3 keys (`Anirudh/fds`) or full URLs (`s3://bucket/Anirudh/fds`) — both are accepted.

### Other S3 Settings

| Variable | Default | Description |
|---|---|---|
| `S3_BUCKET` | `fds-e3testing-bucket-husingh` | Bucket name |
| `S3_ENDPOINT` | `us-ord-10.linodeobjects.com` | S3-compatible endpoint |
| `S3_OUTPUT_KEY_INDEX` | smallest key index | Key index for SSE-C encrypting the output. Output file named `stdtime.<suffix>.key<N>` |
| `ENCRYPTION_KEYS_FILE` | `conf/encryption_keys.json` | JSON file mapping key index → base64 AES-256 key |

### Debug / Inspection

| Variable | Default | Description |
|---|---|---|
| `FD_LOCAL_DEBUG_DIR` | (unset) | If set, copies downloaded inputs and C++ output to `<dir>/<suffix>/input/` and `<dir>/<suffix>/output/` before temp cleanup |

### Encryption Keys File

`conf/encryption_keys.json` maps each key index (the `.keyN` suffix on S3 files)
to its base64-encoded 32-byte AES-256 SSE-C key:

```json
{
  "1": "<base64-32-byte-key>",
  "2": "<base64-32-byte-key>"
}
```

Input files are decrypted with the key matching their `.keyN` suffix. Output files
are encrypted with `S3_OUTPUT_KEY_INDEX`.

---

## How env vars map to C++ parameters

The Spark job reads the `MIX_MAPRULE_*` env vars and translates them into
explicit JNI call parameters — the C++ JNI bridge (`FD_JNI.cpp`) does **not**
read env vars itself:

| Env var | C++ call |
|---|---|
| `MIX_MAPRULE_IDIR` | Files downloaded here → `fcc.setInputDirectory(localInputDir)` |
| `MIX_MAPRULE_ODIR` | C++ writes locally → Scala uploads to this S3 prefix |
| `MIX_MAPRULE_CONFIG` | Downloaded → passed as string → `fcc.readCommands(tmpFile)` |
| `MIX_MAPRULE_SUFFIX` | `fcc.setAnswerSuffix(suffix)` → output file = `stdtime.<suffix>` |
| `MIX_MAPRULE_THINNING_PCT_JUMP` | `fcc.setThinningPctJump(pct)` |

---

## Output

After a successful run, the output file appears at:

```
s3://<S3_BUCKET>/<MIX_MAPRULE_ODIR>/stdtime.<suffix>.key<N>
```

Example:
```
s3://fds-e3testing-bucket-husingh/Anirudh/mix_output/stdtime.config.key2
```

Inspect locally if `FD_LOCAL_DEBUG_DIR` was set:

```bash
ls /tmp/fd_debug_out/config/
#   input/    ← downloaded stdtime.* inputs
#   output/   ← C++ mix result

head /tmp/fd_debug_out/config/output/stdtime.config
```

---

## Standalone Binaries (in cpp_src/)

Built by `compile_native.sh` for local testing — identical logic to what the JNI
bridge calls:

| Binary | Description |
|---|---|
| `mix_maprule` | Blend `stdtime.*` files per a config (same logic as the Spark job) |
| `t2s` | Convert `stdtime.*` → `stdspace.*` sizing curve |
| `t2c` | Convert `stdtime.*` → `stdcount.*` sizing curve |
| `concat` | Concatenate / combine `stdtime.*` files |
| `unshard` | Merge sharded `stdtime.*` files from multiple reducers |

These binaries also read `MIX_MAPRULE_*` env vars directly (unlike the JNI path).

Example — run `mix_maprule` locally to verify the same result as the Spark job:

```bash
cd fd-tools-spark/cpp_src

MIX_MAPRULE_IDIR=/tmp/fd_debug_out/config/input \
MIX_MAPRULE_ODIR=/tmp/fd_debug_out/config/standalone_output \
MIX_MAPRULE_CONFIG=/path/to/config \
MIX_MAPRULE_SUFFIX=config \
./mix_maprule
```

---

## Known Limitations / Future Work

| Item | Notes |
|---|---|
| Single Spark task | One mix per job invocation. Run the job multiple times (with different `MIX_MAPRULE_*` vars) for multiple mixes in parallel. |
| `t2s` / `t2c` not yet in Spark | JNI wrappers for these tools follow the same pattern as `FD_JNI.cpp` and can be added to `FDNative.scala` |
| `concat` / `unshard` not yet in Spark | Same — the C++ sources are included for reference |
