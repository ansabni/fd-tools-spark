# fd-tools-spark

Spark implementation of the **FD tools** (`mix_maprule`, `unshard`, `t2s`, `t2c`).

Reads `stdtime.*` footprint descriptor files from Linode Object Storage (S3),
runs the desired C++ tool via JNI, and uploads the resulting `stdtime.*` /
`stdspace.*` / `stdcount.*` output file back to S3 — encrypted with SSE-C.

Four Spark jobs are provided:

| Job class | Launcher | Replaces |
|---|---|---|
| `com.fd.FDPipelineJob` | `run_pipeline_job.sh` | `mix_maprule` |
| `com.fd.UnshardJob`    | `run_unshard_job.sh`  | `unshard` |
| `com.fd.T2sJob`        | `run_t2s_job.sh`      | `t2s` (stdtime → stdspace) |
| `com.fd.T2cJob`        | `run_t2c_job.sh`      | `t2c` (stdtime → stdcount) |

This replaces the standalone binaries for cloud/distributed execution while
keeping all computation in the existing C++ codebase.

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
│                              (sbt package bundles these into the jar
│                               under  native/  for runtime extraction)
└── src/main/scala/com/fd/
    ├── FDPipelineJob.scala    ← Spark job — mix_maprule
    ├── UnshardJob.scala       ← Spark job — unshard
    ├── T2xJob.scala           ← Shared pipeline + T2sJob / T2cJob wrappers
    └── FDNative.scala         ← JNI interface + jar-resource loader
```

---

## Deliverables

| File | Portable? | Notes |
|---|---|---|
| `target/scala-2.13/fd-spark_2.13-0.1.jar` | ⚠️ Bundles native lib for **the platform it was packaged on** | Pure Scala bytecode + the `.so`/`.dylib` produced by `compile_native.sh`, embedded as resources under `native/` |
| `run_pipeline_job.sh` / `run_unshard_job.sh` / `run_t2s_job.sh` / `run_t2c_job.sh` | ✅ | Shell scripts, no changes needed |
| `conf/encryption_keys.json` | ✅ | Keep secret — contains AES keys |

> **The native library is bundled inside the jar.**
> At runtime, `FDNative` extracts `native/libFD.{so,dylib}` from the jar to a
> temp file and calls `System.load()`. No `LD_LIBRARY_PATH` / `java.library.path`
> setup needed on the receiving cluster.
>
> ⚠️ The jar is platform-specific because the bundled `.so`/`.dylib` is.
> Re-run `./compile_native.sh && sbt package` on the target OS/arch (typically
> Linux x86_64) before distributing to the cluster. A macOS-built jar will
> fail with `dlopen` errors on Linux.

---

## Setup

### Step 1 — Compile the Native Library

Run on the **target platform** (the machine where Spark executors will run):

```bash
cd fd-tools-spark
./compile_native.sh
```

This produces:
- `lib/libFD.dylib` (macOS) or `lib/libFD.so` (Linux) — embedded into the jar by step 2
- Standalone binaries in `cpp_src/`: `mix_maprule`, `t2s`, `t2c`, `concat`, `unshard` (for local CLI testing)

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

`sbt package` automatically copies whichever of `lib/libFD.{so,dylib}` exist
into the jar under `native/`. You can verify with:

```bash
unzip -l target/scala-2.13/fd-spark_2.13-0.1.jar | grep native/
#   native/libFD.so      ...
#   native/libFD.dylib   ...   (only on macOS dev boxes)
```

> Always run `compile_native.sh` first. If `lib/` is empty when `sbt package`
> runs, the resulting jar will not contain a native lib and the runtime loader
> will fall back to `System.loadLibrary("FD")` (which most deploys won't have
> set up via `LD_LIBRARY_PATH`).

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

### Step 3b — Run the Unshard Job

The unshard job divides every value in a `stdtime.*` file by an integer
`UNSHARD_FACTOR` (typically the number of map shards that produced it). It
operates on a single input file rather than a directory.

```bash
cd fd-tools-spark

UNSHARD_IDIR=Anirudh/mix_output/stdtime.config.key2 \
UNSHARD_ODIR=Anirudh/unshard_output/stdtime.unshard \
UNSHARD_FACTOR=16 \
./run_unshard_job.sh
```

| Variable | Required | Description |
|---|---|---|
| `UNSHARD_IDIR` | ✅ | S3 path to the **single** input `stdtime.*.keyN` file |
| `UNSHARD_ODIR` | ✅ | S3 output path (without `.keyN` — added automatically) |
| `UNSHARD_FACTOR` | ✅ | Float divisor applied to every value in the input |
| `S3_OUTPUT_KEY_INDEX` | ✗ | SSE-C key index for the output file. Defaults to smallest key index |
| `FD_LOCAL_DEBUG_DIR` | ✗ | If set, also writes a local copy of the unsharded output |

Output is uploaded to `s3://<S3_BUCKET>/<UNSHARD_ODIR>.key<N>`.

---

### Step 3c — Run the t2s Job (stdtime → stdspace)

The t2s job converts a `stdtime.*` file into a `stdspace.*` sizing curve.
It operates on a single input file rather than a directory.

```bash
cd fd-tools-spark

T2S_IDIR=Anirudh/unshard_output/stdtime.unshard.key2 \
T2S_ODIR=Anirudh/t2s_output/stdspace.unshard \
./run_t2s_job.sh
```

| Variable | Required | Description |
|---|---|---|
| `T2S_IDIR` | ✅ | S3 path to the **single** input `stdtime.*.keyN` file |
| `T2S_ODIR` | ✅ | S3 output path (without `.keyN` — added automatically) |
| `S3_OUTPUT_KEY_INDEX` | ✗ | SSE-C key index for the output file. Defaults to smallest key index |
| `FD_LOCAL_DEBUG_DIR` | ✗ | If set, also writes a local copy of the stdspace output |

Output is uploaded to `s3://<S3_BUCKET>/<T2S_ODIR>.key<N>`.

---

### Step 3d — Run the t2c Job (stdtime → stdcount)

The t2c job converts a `stdtime.*` file into a `stdcount.*` sizing curve.
Same shape as t2s — only the output type differs.

```bash
cd fd-tools-spark

T2C_IDIR=Anirudh/unshard_output/stdtime.unshard.key2 \
T2C_ODIR=Anirudh/t2c_output/stdcount.unshard \
./run_t2c_job.sh
```

| Variable | Required | Description |
|---|---|---|
| `T2C_IDIR` | ✅ | S3 path to the **single** input `stdtime.*.keyN` file |
| `T2C_ODIR` | ✅ | S3 output path (without `.keyN` — added automatically) |
| `S3_OUTPUT_KEY_INDEX` | ✗ | SSE-C key index for the output file. Defaults to smallest key index |
| `FD_LOCAL_DEBUG_DIR` | ✗ | If set, also writes a local copy of the stdcount output |

Output is uploaded to `s3://<S3_BUCKET>/<T2C_ODIR>.key<N>`.

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
| Single Spark task | One operation per job invocation. Run the job multiple times (with different env vars) for multiple operations in parallel. |
| `concat` not yet in Spark | The C++ source is included for reference; a JNI wrapper would follow the same pattern as `runT2s` / `runT2c` in `FD_JNI.cpp`. |
