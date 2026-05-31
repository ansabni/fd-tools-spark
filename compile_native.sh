#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# compile_native.sh — Build libFD.so (Linux) or libFD.dylib (macOS)
# from the C++ sources in ./cpp_src/ and copy the result into ./lib/
#
# Also builds standalone tool binaries (mix_maprule, t2s, t2c, concat, unshard)
# into ./cpp_src/ for local testing.
#
# Run this on every target platform before running the Spark job.
#
# Requirements (Linux):
#   sudo apt-get install -y g++ openjdk-11-jdk
#
# Requirements (macOS):
#   brew install llvm openjdk@11
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$SCRIPT_DIR/cpp_src"
LIB_DIR="$SCRIPT_DIR/lib"

mkdir -p "$LIB_DIR"

# ---------------------------------------------------------------------------
# Detect platform
# ---------------------------------------------------------------------------
OS="$(uname -s)"
ARCH="$(uname -m)"
echo "Platform: $OS / $ARCH"

# ---------------------------------------------------------------------------
# Locate JDK for JNI headers
# ---------------------------------------------------------------------------
if [ -n "${JAVA_HOME:-}" ] && [ -d "$JAVA_HOME" ]; then
  JDK_HOME="$JAVA_HOME"
elif [ -d "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" ]; then
  JDK_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
elif [ -d "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home" ]; then
  JDK_HOME="/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"
elif command -v java &>/dev/null; then
  JDK_HOME="$(java -XshowSettings:all -version 2>&1 | grep 'java.home' | awk '{print $3}')"
  JDK_HOME="${JDK_HOME%/jre}"
else
  echo "ERROR: JDK not found. Set JAVA_HOME or install openjdk-11."
  exit 1
fi
echo "JDK_HOME: $JDK_HOME"

if [ "$OS" = "Darwin" ]; then
  JNI_PLATFORM_INC="$JDK_HOME/include/darwin"
else
  JNI_PLATFORM_INC="$JDK_HOME/include/linux"
fi
JNI_CFLAGS="-I$JDK_HOME/include -I$JNI_PLATFORM_INC"

# ---------------------------------------------------------------------------
# Compiler and flags
# ---------------------------------------------------------------------------
if [ "$OS" = "Darwin" ]; then
  CXX="${CXX:-clang++}"
else
  CXX="${CXX:-g++}"
fi

CXXFLAGS_COMMON="-std=c++11 -O2 -Wall"
CXXFLAGS_SHARED="$CXXFLAGS_COMMON -fPIC"
LIBS="-lpthread -lm"

echo "Compiler : $CXX"
echo "Sources  : $SRC_DIR"
echo "Lib dir  : $LIB_DIR"
echo ""

cd "$SRC_DIR"

# ---------------------------------------------------------------------------
# Core library objects (shared between the JNI .so and the standalone binaries)
# ---------------------------------------------------------------------------
LIB_SRCS=(
  FD.cpp
  FD_Calculus.cpp
  FD_QuickAdd.cpp
  FD_stdcount.cpp
  FD_stdspace.cpp
  FD_stdtime.cpp
  now.cpp
)

# ---------------------------------------------------------------------------
# Step 1: Compile core lib objects as PIC (for shared library)
# ---------------------------------------------------------------------------
echo "=== Step 1: Compiling core library objects (PIC) ==="
LIB_OBJS=()
for src in "${LIB_SRCS[@]}"; do
  obj="${src%.cpp}_pic.o"
  echo "  $src → $obj"
  $CXX $CXXFLAGS_SHARED -c -o "$obj" "$src"
  LIB_OBJS+=("$obj")
done

# ---------------------------------------------------------------------------
# Step 2: Compile JNI bridge object (PIC)
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 2: Compiling JNI bridge (FD_JNI.cpp) ==="
JNI_OBJ="FD_JNI_pic.o"
echo "  FD_JNI.cpp → $JNI_OBJ"
$CXX $CXXFLAGS_SHARED $JNI_CFLAGS -c -o "$JNI_OBJ" FD_JNI.cpp

# ---------------------------------------------------------------------------
# Step 3: Link shared library (libFD)
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 3: Linking shared library ==="
if [ "$OS" = "Darwin" ]; then
  TARGET="$LIB_DIR/libFD.dylib"
  echo "  → $TARGET"
  $CXX -dynamiclib -o "$TARGET" "${LIB_OBJS[@]}" "$JNI_OBJ" $LIBS
  # Also produce a .so symlink so Spark's System.loadLibrary("FD") finds it on Linux paths
  ln -sf libFD.dylib "$LIB_DIR/libFD.so"
  echo "  → $LIB_DIR/libFD.so  (symlink)"
else
  TARGET="$LIB_DIR/libFD.so"
  echo "  → $TARGET"
  $CXX -shared -fPIC -o "$TARGET" "${LIB_OBJS[@]}" "$JNI_OBJ" $LIBS
fi

# ---------------------------------------------------------------------------
# Step 4: Build standalone binaries (for local testing / reference)
#         These use non-PIC objects compiled fresh from the same sources.
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 4: Compiling core library objects (non-PIC for static linking) ==="
STATIC_OBJS=()
for src in "${LIB_SRCS[@]}"; do
  obj="${src%.cpp}.o"
  echo "  $src → $obj"
  $CXX $CXXFLAGS_COMMON -c -o "$obj" "$src"
  STATIC_OBJS+=("$obj")
done

echo ""
echo "=== Step 5: Building standalone binaries ==="

BINARIES=(
  "mix_maprule:mix_maprule.cpp"
  "t2s:t2s.cpp"
  "t2c:t2c.cpp"
  "concat:concat_stdtime.cpp"
  "unshard:unshard.cpp"
)

for entry in "${BINARIES[@]}"; do
  bin="${entry%%:*}"
  src="${entry##*:}"
  echo "  $src → $bin"
  $CXX $CXXFLAGS_COMMON -c -o "${src%.cpp}.o" "$src"
  $CXX $CXXFLAGS_COMMON -o "$bin" "${src%.cpp}.o" "${STATIC_OBJS[@]}" $LIBS
done

# ---------------------------------------------------------------------------
# Clean up intermediate objects
# ---------------------------------------------------------------------------
echo ""
echo "=== Cleaning up object files ==="
rm -f "${LIB_OBJS[@]}" "$JNI_OBJ" "${STATIC_OBJS[@]}"
for entry in "${BINARIES[@]}"; do
  src="${entry##*:}"
  rm -f "${src%.cpp}.o"
done
echo "  Done."

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "✓ Shared library : $TARGET"
if [ "$OS" = "Darwin" ]; then
  echo "✓ Symlink        : $LIB_DIR/libFD.so"
fi
echo ""
echo "✓ Standalone binaries in $SRC_DIR:"
for entry in "${BINARIES[@]}"; do
  bin="${entry%%:*}"
  echo "    $bin"
done
echo ""
echo "Next steps:"
echo "  1. sbt package                    # build the Scala jar"
echo "  2. ./run_pipeline_job.sh          # run the Spark job"
